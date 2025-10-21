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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.metadata.SortKeyUtils;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A wrapper for the operation of {@link org.rocksdb.RocksDB}. */
public class RocksDBKv implements AutoCloseable {

    /** The container of RocksDB option factory and predefined options. */
    private final RocksDBResourceContainer optionsContainer;

    /**
     * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call
     * that disposes the RocksDB object.
     */
    private final ResourceGuard rocksDBResourceGuard;

    /** The write options to use in the states. We disable write ahead logging. */
    private final WriteOptions writeOptions;

    /**
     * We are not using the default column family for KV ops, but we still need to remember this
     * handle so that we can close it properly when the kv is closed. Note that the one returned by
     * {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamilyHandle;

    /** Our RocksDB database. Currently, one kv tablet, one RocksDB instance. */
    protected final RocksDB db;

    // mark whether this kv is already closed and prevent duplicate closing
    private volatile boolean closed = false;

    public RocksDBKv(
            RocksDBResourceContainer optionsContainer,
            RocksDB db,
            ResourceGuard rocksDBResourceGuard,
            ColumnFamilyHandle defaultColumnFamilyHandle) {
        this.optionsContainer = optionsContainer;
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.writeOptions = optionsContainer.getWriteOptions();
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
    }

    public ResourceGuard getResourceGuard() {
        return rocksDBResourceGuard;
    }

    /**
     * Get the underlying RocksDB instance. This method provides access to the raw RocksDB for
     * advanced operations.
     *
     * @return the RocksDB instance
     */
    public RocksDB getDb() {
        return db;
    }

    public RocksDBWriteBatchWrapper newWriteBatch(long writeBatchSize) {
        return new RocksDBWriteBatchWrapper(db, writeBatchSize);
    }

    public @Nullable byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Fail to get key.", e);
        }
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        try {
            return db.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new IOException("Fail to get keys.", e);
        }
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) {
        List<byte[]> pkList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);
        try {
            iterator.seek(prefixKey);
            while (iterator.isValid() && BytesUtils.prefixEquals(prefixKey, iterator.key())) {
                pkList.add(iterator.value());
                iterator.next();
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return pkList;
    }

    public List<byte[]> rangeLookup(
            byte[] prefixKey, List<RangeCondition> rangeConditions, @Nullable Integer limit) {
        return rangeLookup(prefixKey, rangeConditions, limit, null);
    }

    /**
     * High-performance sortKey range lookup method for direct Long value bounds.
     *
     * <p>This method is optimized for sortKey-based range queries where the table schema has a
     * designated sortKey field and the range bounds are Long values.
     *
     * @param prefixKey the prefix key (primary key part)
     * @param lowerBound the lower bound Long value for the sortKey range
     * @param upperBound the upper bound Long value for the sortKey range
     * @param limit optional limit on the number of results
     * @return list of matching value bytes
     */
    public List<byte[]> sortKeyRangeLookup(
            byte[] prefixKey, Long lowerBound, Long upperBound, @Nullable Integer limit) {
        // Use the optimized key-range lookup with Long-specific serialization
        return sortKeyOptimizedLookup(prefixKey, lowerBound, upperBound, limit);
    }

    public List<byte[]> rangeLookup(
            byte[] prefixKey,
            List<RangeCondition> rangeConditions,
            @Nullable Integer limit,
            @Nullable ValueFieldExtractor fieldExtractor) {

        // 直接使用 key 范围优化：seek 到下边界，遍历到上边界后退出
        return keyRangeOptimizedLookup(prefixKey, rangeConditions, limit, fieldExtractor);
    }

    /**
     * Key 边界优化的范围查询：使用 sortKey 序列化方式直接 seek 到边界
     *
     * <p>核心策略： 1. 将 Integer 范围值转换为 Long 并使用 sortKey 无符号序列化格式 2. 构造边界 key：prefix +
     * serialized_long(bound) 3. 直接 seek 到下边界，遍历到上边界后退出 4. 确保字典序 = 数值序，实现高效的范围定位.
     */
    private List<byte[]> keyRangeOptimizedLookup(
            byte[] prefixKey,
            List<RangeCondition> rangeConditions,
            @Nullable Integer limit,
            @Nullable ValueFieldExtractor fieldExtractor) {

        if (rangeConditions == null || rangeConditions.isEmpty()) {
            return prefixLookup(prefixKey);
        }

        List<byte[]> resultList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);

        try {
            // 使用第一个范围条件构造边界
            RangeCondition condition = rangeConditions.get(0);

            // 构造范围边界 key（使用 sortKey 序列化格式）
            byte[] startKey = buildSortKeyBasedStartKey(prefixKey, condition);
            byte[] endKey = buildSortKeyBasedEndKey(prefixKey, condition);

            // 直接 seek 到下边界
            iterator.seek(startKey);

            int count = 0;
            long startTime = System.currentTimeMillis();

            while (iterator.isValid()) {
                byte[] currentKey = iterator.key();

                // 检查是否超出上边界 - 如果是则直接退出
                if (compareKeysLexicographically(currentKey, endKey) > 0) {
                    break;
                }

                // 检查是否仍在 prefix 范围内（安全检查）
                if (!BytesUtils.prefixEquals(prefixKey, currentKey)) {
                    break;
                }

                // 检查数量限制
                if (limit != null && count >= limit) {
                    break;
                }

                byte[] valueBytes = iterator.value();
                if (valueBytes != null) {
                    // 可选的额外 value 条件检查（用于多条件或复杂场景）
                    if (fieldExtractor == null
                            || matchesRangeConditions(
                                    valueBytes, rangeConditions, fieldExtractor)) {
                        resultList.add(valueBytes);
                        count++;
                    }
                }

                iterator.next();
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return resultList;
    }

    /** 构造基于 sortKey 格式的起始边界 key . */
    private byte[] buildSortKeyBasedStartKey(byte[] prefixKey, RangeCondition condition) {
        Integer lowerBound = condition.getLowerBound();
        if (lowerBound == null) {
            return prefixKey;
        }

        // 将 Integer 转为 Long，使用 sortKey 的特殊序列化格式
        long longValue = lowerBound.longValue();
        byte[] sortKeyBytes = SortKeyUtils.serializeSortKeyLong(longValue);

        byte[] startKey = new byte[prefixKey.length + sortKeyBytes.length];
        System.arraycopy(prefixKey, 0, startKey, 0, prefixKey.length);
        System.arraycopy(sortKeyBytes, 0, startKey, prefixKey.length, sortKeyBytes.length);

        return startKey;
    }

    /** 构造基于 sortKey 格式的结束边界 key . */
    private byte[] buildSortKeyBasedEndKey(byte[] prefixKey, RangeCondition condition) {
        Integer upperBound = condition.getUpperBound();
        if (upperBound == null) {
            // 没有上边界，构造最大可能的 key
            byte[] maxKey = new byte[prefixKey.length + 8];
            System.arraycopy(prefixKey, 0, maxKey, 0, prefixKey.length);
            // 填充最大的 8字节值（sortKey 格式的最大值）
            for (int i = 0; i < 8; i++) {
                maxKey[prefixKey.length + i] = (byte) 0xFF;
            }
            return maxKey;
        }

        // 处理排他边界
        long longValue = upperBound.longValue();
        if (!condition.isUpperInclusive()) {
            // 安全的减1操作，避免溢出
            if (longValue > Long.MIN_VALUE) {
                longValue = longValue - 1;
            }
        }

        byte[] sortKeyBytes = SortKeyUtils.serializeSortKeyLong(longValue);
        byte[] endKey = new byte[prefixKey.length + sortKeyBytes.length];
        System.arraycopy(prefixKey, 0, endKey, 0, prefixKey.length);
        System.arraycopy(sortKeyBytes, 0, endKey, prefixKey.length, sortKeyBytes.length);

        return endKey;
    }

    /** SortKey 的特殊序列化方式：Long → 8字节无符号大端序列 参考用户提供的序列化逻辑，确保字典序 = 数值序 . */
    private byte[] serializeSortKeyLong(long value) {
        // 转换为无符号表示：Long.MIN_VALUE → 0, Long.MAX_VALUE → 0xFFFFFFFFFFFFFFFF
        long unsigned = value ^ Long.MIN_VALUE;

        // 按大端字节序写入 8 字节
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (unsigned >> 56);
        bytes[1] = (byte) (unsigned >> 48);
        bytes[2] = (byte) (unsigned >> 40);
        bytes[3] = (byte) (unsigned >> 32);
        bytes[4] = (byte) (unsigned >> 24);
        bytes[5] = (byte) (unsigned >> 16);
        bytes[6] = (byte) (unsigned >> 8);
        bytes[7] = (byte) (unsigned);

        return bytes;
    }

    /** 字典序比较两个 key，用于判断是否到达上边界 . */
    private int compareKeysLexicographically(byte[] key1, byte[] key2) {
        if (key1 == null && key2 == null) {
            return 0;
        }
        if (key1 == null) {
            return -1;
        }
        if (key2 == null) {
            return 1;
        }

        int minLength = Math.min(key1.length, key2.length);
        for (int i = 0; i < minLength; i++) {
            int diff = (key1[i] & 0xFF) - (key2[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(key1.length, key2.length);
    }

    /**
     * Optimized sortKey range lookup using direct Long bounds.
     *
     * <p>This method directly constructs boundary keys using the sortKey serialization format
     * without going through RangeCondition conversion, providing maximum performance.
     *
     * @param prefixKey the prefix key (primary key part)
     * @param lowerBound the lower bound Long value (inclusive)
     * @param upperBound the upper bound Long value (inclusive)
     * @param limit optional limit on the number of results
     * @return list of matching value bytes
     */
    private List<byte[]> sortKeyOptimizedLookup(
            byte[] prefixKey, Long lowerBound, Long upperBound, @Nullable Integer limit) {

        List<byte[]> resultList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);

        try {
            // Construct boundary keys directly using sortKey serialization
            byte[] startKey = buildSortKeyStartKey(prefixKey, lowerBound);
            byte[] endKey = buildSortKeyEndKey(prefixKey, upperBound);

            // Direct seek to the lower bound
            iterator.seek(startKey);

            int count = 0;
            long startTime = System.currentTimeMillis();

            while (iterator.isValid()) {
                byte[] currentKey = iterator.key();

                // Check if we've exceeded the upper bound
                if (compareKeysLexicographically(currentKey, endKey) > 0) {
                    break;
                }

                // Safety check: still within prefix range
                if (!BytesUtils.prefixEquals(prefixKey, currentKey)) {
                    break;
                }

                // Check count limit
                if (limit != null && count >= limit) {
                    break;
                }

                byte[] valueBytes = iterator.value();
                if (valueBytes != null) {
                    resultList.add(valueBytes);
                    count++;
                }

                iterator.next();
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return resultList;
    }

    /** Build start key for sortKey range using Long lower bound. */
    private byte[] buildSortKeyStartKey(byte[] prefixKey, Long lowerBound) {
        if (lowerBound == null) {
            return prefixKey;
        }

        byte[] sortKeyBytes = SortKeyUtils.serializeSortKeyLong(lowerBound);
        byte[] startKey = new byte[prefixKey.length + sortKeyBytes.length];
        System.arraycopy(prefixKey, 0, startKey, 0, prefixKey.length);
        System.arraycopy(sortKeyBytes, 0, startKey, prefixKey.length, sortKeyBytes.length);

        return startKey;
    }

    /** Build end key for sortKey range using Long upper bound. */
    private byte[] buildSortKeyEndKey(byte[] prefixKey, Long upperBound) {
        if (upperBound == null) {
            // No upper bound, construct maximum possible key
            byte[] maxKey = new byte[prefixKey.length + 8];
            System.arraycopy(prefixKey, 0, maxKey, 0, prefixKey.length);
            // Fill with maximum 8-byte value (sortKey format maximum)
            for (int i = 0; i < 8; i++) {
                maxKey[prefixKey.length + i] = (byte) 0xFF;
            }
            return maxKey;
        }

        byte[] sortKeyBytes = SortKeyUtils.serializeSortKeyLong(upperBound);
        byte[] endKey = new byte[prefixKey.length + sortKeyBytes.length];
        System.arraycopy(prefixKey, 0, endKey, 0, prefixKey.length);
        System.arraycopy(sortKeyBytes, 0, endKey, prefixKey.length, sortKeyBytes.length);

        return endKey;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Fail to put kv.", e);
        }
    }

    public void delete(byte[] key) throws IOException {
        try {
            db.delete(writeOptions, key);
        } catch (RocksDBException e) {
            throw new IOException("Fail to delete key.", e);
        }
    }

    /**
     * Check whether the range conditions are matched with the row bytes.
     *
     * @param valueBytes the row bytes from RocksDB
     * @param rangeConditions the range conditions
     * @param fieldExtractor the field extractor to extract field value from row bytes
     * @return whether the range conditions are matched
     */
    private boolean matchesRangeConditions(
            byte[] valueBytes,
            List<RangeCondition> rangeConditions,
            ValueFieldExtractor fieldExtractor) {
        if (rangeConditions == null || rangeConditions.isEmpty()) {
            return true;
        }

        for (RangeCondition condition : rangeConditions) {
            if (!matchesSingleRangeCondition(valueBytes, condition, fieldExtractor)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether a single range condition is matched with the row bytes.
     *
     * @param valueBytes the row bytes from RocksDB
     * @param condition the range condition
     * @param fieldExtractor the field extractor to extract field value from row bytes
     * @return whether the single range condition is matched
     */
    private boolean matchesSingleRangeCondition(
            byte[] valueBytes, RangeCondition condition, ValueFieldExtractor fieldExtractor) {
        if (condition == null) {
            return true;
        }

        String fieldName = condition.getFieldName();
        Integer lowerBound = condition.getLowerBound();
        Integer upperBound = condition.getUpperBound();

        if (lowerBound == null && upperBound == null) {
            return true;
        }

        // Extract field value using fieldExtractor
        try {
            // Use value-based filtering instead of direct comparison
            byte[] fieldBytes = fieldExtractor.extractFieldValue(valueBytes, fieldName);
            if (fieldBytes == null) {
                return false;
            }

            // Convert field bytes to int for comparison
            int fieldValue;
            if (fieldBytes.length == 4) {
                fieldValue = java.nio.ByteBuffer.wrap(fieldBytes).getInt();
            } else {
                return false; // unsupported field type for range query
            }

            // Check lower bound
            if (lowerBound != null) {
                if (condition.isLowerInclusive()) {
                    if (fieldValue < lowerBound) {
                        return false;
                    }
                } else {
                    if (fieldValue <= lowerBound) {
                        return false;
                    }
                }
            }

            // Check upper bound
            if (upperBound != null) {
                if (condition.isUpperInclusive()) {
                    if (fieldValue > upperBound) {
                        return false;
                    }
                } else {
                    if (fieldValue >= upperBound) {
                        return false;
                    }
                }
            }

            return true;
        } catch (Exception e) {
            // If field extraction fails, return false (doesn't match)
            return false;
        }
    }

    public void sync() throws IOException {
        try {
            db.syncWal();
        } catch (RocksDBException e) {
            throw new IOException("Fail to sync RocksDB.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        try {
            // we have to call close on the default CF handle manually
            IOUtils.closeQuietly(defaultColumnFamilyHandle);

            IOUtils.closeQuietly(writeOptions);

            IOUtils.closeQuietly(db);

            IOUtils.closeQuietly(optionsContainer);
        } catch (Exception ex) {
            throw new IOException("Fail to close RocksDB.", ex);
        }
    }

    public boolean closed() {
        return closed;
    }

    /**
     * Check if the RocksDB instance is closed and throw an exception if it is. This method is used
     * to ensure that operations are not performed on a closed database.
     */
    public void checkIfRocksDBClosed() {
        if (closed) {
            throw new IllegalStateException("RocksDB instance has been closed and cannot be used.");
        }
    }

    /**
     * Perform a limited scan of the RocksDB instance, returning up to 'limit' values. This method
     * scans the database sequentially and returns raw value bytes.
     *
     * @param limit the maximum number of records to return
     * @return list of value bytes, up to the specified limit
     * @throws IOException if there's an error accessing RocksDB
     */
    public List<byte[]> limitScan(int limit) throws IOException {
        checkIfRocksDBClosed();

        List<byte[]> resultList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);

        try {
            iterator.seekToFirst();
            int count = 0;

            while (iterator.isValid() && count < limit) {
                byte[] valueBytes = iterator.value();
                if (valueBytes != null) {
                    resultList.add(valueBytes);
                    count++;
                }
                iterator.next();
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return resultList;
    }
}
