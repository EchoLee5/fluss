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

package org.apache.fluss.row.encode;

import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.SortKeyUtils;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.List;

/**
 * Custom KeyEncoder implementation that handles sortKey serialization.
 *
 * <p>For primary keys containing a sortKey field: - Non-sortKey fields: use standard Fluss encoding
 * - SortKey field: use special unsigned serialization for optimal range queries
 */
public class SortKeyAwareKeyEncoder implements KeyEncoder {
    private final List<String> primaryKeys;
    private final String sortKeyField;
    private final int sortKeyIndex;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final KeyEncoder[] fieldEncoders;

    public SortKeyAwareKeyEncoder(
            RowType rowType,
            List<String> primaryKeys,
            String sortKeyField,
            DataLakeFormat lakeFormat) {
        this.primaryKeys = primaryKeys;
        this.sortKeyField = sortKeyField;
        this.sortKeyIndex = primaryKeys.indexOf(sortKeyField);

        // Create field getters for all primary key fields
        this.fieldGetters = new InternalRow.FieldGetter[primaryKeys.size()];
        this.fieldEncoders = new KeyEncoder[primaryKeys.size()];

        for (int i = 0; i < primaryKeys.size(); i++) {
            String pkField = primaryKeys.get(i);
            int fieldIndex = rowType.getFieldIndex(pkField);
            DataType fieldType = rowType.getTypeAt(fieldIndex);

            this.fieldGetters[i] = InternalRow.createFieldGetter(fieldType, fieldIndex);

            if (pkField.equals(sortKeyField)) {
                // For sortKey field, we'll handle encoding specially
                this.fieldEncoders[i] = null;
            } else {
                // For non-sortKey fields, create standard single-field encoders
                RowType singleFieldRowType = RowType.of(fieldType);
                this.fieldEncoders[i] =
                        CompactedKeyEncoder.createKeyEncoder(
                                singleFieldRowType,
                                java.util.Arrays.asList(singleFieldRowType.getFieldNames().get(0)));
            }
        }
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        java.io.ByteArrayOutputStream keyStream = new java.io.ByteArrayOutputStream();

        try {
            for (int i = 0; i < primaryKeys.size(); i++) {
                String pkField = primaryKeys.get(i);
                Object fieldValue = fieldGetters[i].getFieldOrNull(row);

                if (fieldValue == null) {
                    throw new IllegalArgumentException(
                            "Primary key field '" + pkField + "' cannot be null");
                }

                byte[] fieldBytes;

                if (i == sortKeyIndex) {
                    // Special sortKey serialization
                    if (!(fieldValue instanceof Long)) {
                        throw new IllegalArgumentException(
                                "SortKey field '"
                                        + sortKeyField
                                        + "' must be of type BIGINT/Long, got: "
                                        + fieldValue.getClass());
                    }
                    fieldBytes = SortKeyUtils.serializeSortKeyLong((Long) fieldValue);
                } else {
                    // Standard Fluss encoding for non-sortKey fields
                    GenericRow singleFieldRow = new GenericRow(1);
                    singleFieldRow.setField(0, fieldValue);
                    fieldBytes = fieldEncoders[i].encodeKey(singleFieldRow);
                }

                keyStream.write(fieldBytes);
            }

            return keyStream.toByteArray();

        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to encode sortKey-aware primary key", e);
        }
    }
}
