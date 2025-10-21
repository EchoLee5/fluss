/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.SortKeyAwareKeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;

/**
 * An implementation of {@link Lookuper} that performs range queries by calling the server-side
 * range lookup functionality.
 *
 * <p>This lookuper first validates the prefix lookup configuration, then delegates range queries to
 * the server where the actual range filtering is performed using RocksDB capabilities.
 */
class RangeKeyLookuper implements Lookuper {

    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;

    /** Extract bucket key from prefix lookup key row. */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /**
     * a getter to extract partition from prefix lookup key row, null when it's not a partitioned.
     */
    private @Nullable final PartitionGetter partitionGetter;

    /** Decode the lookup bytes to result row. */
    private final ValueDecoder kvValueDecoder;

    private final String rangeColumnName;

    private final KeyEncoder primaryKeyEncoder;

    public RangeKeyLookuper(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            String rangeColumnName) {
        List<String> lookupColumnNames =
                tableInfo.getPrimaryKeys().stream()
                        .filter(k -> !rangeColumnName.equals(k))
                        .collect(Collectors.toList());
        // sanity check - reuse the same validation as PrefixKeyLookuper
        validatePrefixLookup(tableInfo, lookupColumnNames);

        // initialization
        this.tableInfo = tableInfo;
        this.numBuckets = tableInfo.getNumBuckets();
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;

        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(lookupColumnNames);
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        this.bucketKeyEncoder = KeyEncoder.of(lookupRowType, tableInfo.getBucketKeys(), lakeFormat);
        this.bucketingFunction = BucketingFunction.of(lakeFormat);
        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                tableInfo.getRowType().getChildren().toArray(new DataType[0])));
        this.rangeColumnName = rangeColumnName;
        String sortKeyField = tableInfo.getSchema().getSortKeyField().orElse(null);
        RowType rowType = tableInfo.getRowType().project(tableInfo.getPrimaryKeys());
        this.primaryKeyEncoder =
                createSortKeyAwarePrimaryKeyEncoder(
                        rowType, tableInfo.getPhysicalPrimaryKeys(), sortKeyField, lakeFormat);
    }

    // Same validation logic as PrefixKeyLookuper
    private void validatePrefixLookup(TableInfo tableInfo, List<String> lookupColumns) {
        if (!tableInfo.hasPrimaryKey()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Log table %s doesn't support prefix lookup",
                            tableInfo.getTablePath()));
        }

        List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        for (int i = 0; i < bucketKeys.size(); i++) {
            if (!bucketKeys.get(i).equals(physicalPrimaryKeys.get(i))) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not perform prefix lookup on table '%s', "
                                        + "because the bucket keys %s is not a prefix subset of the "
                                        + "physical primary keys %s (excluded partition fields if present).",
                                tableInfo.getTablePath(), bucketKeys, physicalPrimaryKeys));
            }
        }

        if (tableInfo.isPartitioned()) {
            List<String> partitionKeys = tableInfo.getPartitionKeys();
            Set<String> lookupColumnsSet = new HashSet<>(lookupColumns);
            if (!lookupColumnsSet.containsAll(partitionKeys)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not perform prefix lookup on table '%s', "
                                        + "because the lookup columns %s must contain all partition fields %s.",
                                tableInfo.getTablePath(), lookupColumns, partitionKeys));
            }
        }

        List<String> physicalLookupColumns = new ArrayList<>(lookupColumns);
        physicalLookupColumns.removeAll(tableInfo.getPartitionKeys());
        if (!physicalLookupColumns.equals(bucketKeys)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not perform prefix lookup on table '%s', "
                                    + "because the lookup columns %s must contain all bucket keys %s in order.",
                            tableInfo.getTablePath(), lookupColumns, bucketKeys));
        }
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        // encoding the key row using a compacted way consisted with how the key is encoded when put
        // a row
        byte[] pkBytes = primaryKeyEncoder.encodeKey(lookupKey);
        byte[] bkBytes =
                bucketKeyEncoder == primaryKeyEncoder
                        ? pkBytes
                        : bucketKeyEncoder.encodeKey(lookupKey);
        Long partitionId = null;
        if (partitionGetter != null) {
            try {
                partitionId =
                        getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }

        int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupClient
                .lookup(tableBucket, pkBytes)
                .thenApply(
                        valueBytes -> {
                            InternalRow row =
                                    valueBytes == null
                                            ? null
                                            : kvValueDecoder.decodeValue(valueBytes).row;
                            return new LookupResult(row);
                        });
    }

    public CompletableFuture<LookupResult> rangeLookup(
            InternalRow prefixKey, RangeCondition rangeCondition) {
        return rangeLookup(prefixKey, rangeCondition, null);
    }

    public CompletableFuture<LookupResult> rangeLookup(
            InternalRow prefixKey, RangeCondition rangeCondition, @Nullable Integer limit) {

        byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(prefixKey);
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);

        Long partitionId = null;
        if (partitionGetter != null) {
            try {
                partitionId =
                        getPartitionId(
                                prefixKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }

        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);

        // Call server-side range lookup instead of prefix lookup
        return lookupClient
                .rangeLookup(tableBucket, bucketKeyBytes, rangeCondition, limit)
                .thenApply(
                        result -> {
                            List<InternalRow> rowList = new ArrayList<>(result.size());
                            for (byte[] valueBytes : result) {
                                if (valueBytes == null) {
                                    continue;
                                }
                                rowList.add(kvValueDecoder.decodeValue(valueBytes).row);
                            }
                            return new LookupResult(rowList);
                        })
                .exceptionally(
                        throwable -> {
                            // If range lookup fails, fallback to prefix lookup
                            // This provides backward compatibility during transition
                            return performPrefixLookupSync(prefixKey);
                        });
    }

    private LookupResult performPrefixLookupSync(InternalRow prefixKey) {
        try {
            return performPrefixLookup(prefixKey).get();
        } catch (Exception e) {
            return new LookupResult(Collections.emptyList());
        }
    }

    private CompletableFuture<LookupResult> performPrefixLookup(InternalRow prefixKey) {
        byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(prefixKey);
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);

        Long partitionId = null;
        if (partitionGetter != null) {
            try {
                partitionId =
                        getPartitionId(
                                prefixKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }

        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupClient
                .prefixLookup(tableBucket, bucketKeyBytes)
                .thenApply(
                        result -> {
                            List<InternalRow> rowList = new ArrayList<>(result.size());
                            for (byte[] valueBytes : result) {
                                if (valueBytes == null) {
                                    continue;
                                }
                                rowList.add(kvValueDecoder.decodeValue(valueBytes).row);
                            }
                            return new LookupResult(rowList);
                        });
    }

    /**
     * Performs a range lookup using the default range column with the specified bounds.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param lowerBound the lower bound (inclusive), null for no lower bound
     * @param upperBound the upper bound (inclusive), null for no upper bound
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeLookup(
            InternalRow prefixKey, @Nullable Object lowerBound, @Nullable Object upperBound) {
        RangeCondition condition = RangeCondition.between(rangeColumnName, lowerBound, upperBound);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Performs a range lookup using the default range column with the specified bounds and limit.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param lowerBound the lower bound (inclusive), null for no lower bound
     * @param upperBound the upper bound (inclusive), null for no upper bound
     * @param limit maximum number of results to return, null for no limit
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeLookup(
            InternalRow prefixKey,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            @Nullable Integer limit) {
        RangeCondition condition = RangeCondition.between(rangeColumnName, lowerBound, upperBound);
        return rangeLookup(prefixKey, condition, limit);
    }

    /**
     * Performs a "greater than or equal to" range lookup using the default range column.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param lowerBound the lower bound (inclusive)
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeGreaterThanOrEqual(
            InternalRow prefixKey, Object lowerBound) {
        RangeCondition condition = RangeCondition.greaterThanOrEqual(rangeColumnName, lowerBound);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Performs a "greater than" range lookup using the default range column.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param lowerBound the lower bound (exclusive)
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeGreaterThan(
            InternalRow prefixKey, Object lowerBound) {
        RangeCondition condition = RangeCondition.greaterThan(rangeColumnName, lowerBound);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Performs a "less than or equal to" range lookup using the default range column.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param upperBound the upper bound (inclusive)
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeLessThanOrEqual(
            InternalRow prefixKey, Object upperBound) {
        RangeCondition condition = RangeCondition.lessThanOrEqual(rangeColumnName, upperBound);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Performs a "less than" range lookup using the default range column.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param upperBound the upper bound (exclusive)
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeLessThan(InternalRow prefixKey, Object upperBound) {
        RangeCondition condition = RangeCondition.lessThan(rangeColumnName, upperBound);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Performs a range lookup for the last N minutes using the default range column. Assumes the
     * range column contains timestamp values.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param minutes number of minutes to look back from current time
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeLastNMinutes(InternalRow prefixKey, int minutes) {
        long currentTime = System.currentTimeMillis();
        long startTime = currentTime - (minutes * 60L * 1000L);
        RangeCondition condition = RangeCondition.between(rangeColumnName, startTime, currentTime);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Performs a range lookup for the last N hours using the default range column. Assumes the
     * range column contains timestamp values.
     *
     * @param prefixKey the prefix key to filter the initial dataset
     * @param hours number of hours to look back from current time
     * @return the result of the range lookup
     */
    public CompletableFuture<LookupResult> rangeLastNHours(InternalRow prefixKey, int hours) {
        long currentTime = System.currentTimeMillis();
        long startTime = currentTime - (hours * 60L * 60L * 1000L);
        RangeCondition condition = RangeCondition.between(rangeColumnName, startTime, currentTime);
        return rangeLookup(prefixKey, condition);
    }

    /**
     * Returns the configured range column name.
     *
     * @return the range column name
     */
    public String getRangeColumn() {
        return rangeColumnName;
    }

    /**
     * Creates a sortKey-aware primary key encoder that handles sortKey serialization.
     *
     * @param rowType the row type
     * @param primaryKeys the primary key field names
     * @param sortKeyField the sortKey field name (must be part of primary keys)
     * @param lakeFormat the data lake format
     * @return a KeyEncoder that properly handles sortKey serialization
     */
    private KeyEncoder createSortKeyAwarePrimaryKeyEncoder(
            RowType rowType,
            List<String> primaryKeys,
            String sortKeyField,
            DataLakeFormat lakeFormat) {

        // Validate that sortKey is part of primary key
        if (!primaryKeys.contains(sortKeyField)) {
            throw new IllegalStateException(
                    "SortKey field '" + sortKeyField + "' must be part of the primary key");
        }

        return new SortKeyAwareKeyEncoder(rowType, primaryKeys, sortKeyField, lakeFormat);
    }
}
