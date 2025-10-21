/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtai

a copy of the License at
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

import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

/**
 * Extracts field values from encoded value bytes for range comparison.
 *
 * <p>This class is responsible for decoding value bytes and extracting specific field values that
 * can be used for range filtering in RocksDB scans.
 */
public class ValueFieldExtractor {

    private final ValueDecoder valueDecoder;
    private final RowType rowType;

    public ValueFieldExtractor(ValueDecoder valueDecoder, RowType rowType) {
        this.valueDecoder = valueDecoder;
        this.rowType = rowType;
    }

    /**
     * Extracts a field value from encoded value bytes.
     *
     * @param valueBytes the encoded value bytes
     * @param fieldName the name of the field to extract
     * @return the extracted field value as bytes, or null if extraction fails
     */
    public @Nullable byte[] extractFieldValue(byte[] valueBytes, String fieldName) {
        try {
            // Decode the value bytes to get the row
            ValueDecoder.Value decodedValue = valueDecoder.decodeValue(valueBytes);

            // Check if decoding was successful
            if (decodedValue == null || decodedValue.row == null) {
                return null;
            }

            // Find the field index
            int fieldIndex = rowType.getFieldIndex(fieldName);
            if (fieldIndex < 0) {
                return null;
            }

            // Extract field value based on its type
            DataType fieldType = rowType.getTypeAt(fieldIndex);
            return extractFieldValueByType(decodedValue.row, fieldIndex, fieldType);

        } catch (Exception e) {
            // If decoding fails, return null
            return null;
        }
    }

    /**
     * Gets the data type of a field by name.
     *
     * @param fieldName the name of the field
     * @return the data type of the field, or null if field not found
     */
    public @Nullable DataType getFieldType(String fieldName) {
        int fieldIndex = rowType.getFieldIndex(fieldName);
        if (fieldIndex < 0) {
            return null;
        }
        return rowType.getTypeAt(fieldIndex);
    }

    private byte[] extractFieldValueByType(Object row, int fieldIndex, DataType fieldType) {
        try {
            if (row instanceof org.apache.fluss.row.BinaryRow) {
                org.apache.fluss.row.BinaryRow binaryRow = (org.apache.fluss.row.BinaryRow) row;

                // Check if field is null
                if (binaryRow.isNullAt(fieldIndex)) {
                    return null;
                }

                // Extract field value based on type
                switch (fieldType.getTypeRoot()) {
                    case INTEGER:
                    case TIME_WITHOUT_TIME_ZONE:
                        int intValue = binaryRow.getInt(fieldIndex);
                        return java.nio.ByteBuffer.allocate(4).putInt(intValue).array();
                    case BIGINT:
                        long longValue = binaryRow.getLong(fieldIndex);
                        return java.nio.ByteBuffer.allocate(8).putLong(longValue).array();
                    case FLOAT:
                        float floatValue = binaryRow.getFloat(fieldIndex);
                        return java.nio.ByteBuffer.allocate(4).putFloat(floatValue).array();
                    case DOUBLE:
                        double doubleValue = binaryRow.getDouble(fieldIndex);
                        return java.nio.ByteBuffer.allocate(8).putDouble(doubleValue).array();
                    case CHAR:
                        org.apache.fluss.row.BinaryString stringValue =
                                binaryRow.getString(fieldIndex);
                        return stringValue.toBytes();
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        org.apache.fluss.row.TimestampNtz timestampValue =
                                binaryRow.getTimestampNtz(fieldIndex, 6);
                        return java.nio.ByteBuffer.allocate(8)
                                .putLong(timestampValue.getMillisecond())
                                .array();
                    case DATE:
                        int dateValue = binaryRow.getInt(fieldIndex);
                        return java.nio.ByteBuffer.allocate(4).putInt(dateValue).array();
                    case BOOLEAN:
                        boolean boolValue = binaryRow.getBoolean(fieldIndex);
                        return new byte[] {(byte) (boolValue ? 1 : 0)};
                    case BINARY:
                        byte[] binaryValue = binaryRow.getBinary(fieldIndex, 128);
                        return binaryValue;
                    default:
                        // For unsupported types, return null to skip comparison
                        return null;
                }
            } else if (row instanceof org.apache.fluss.row.InternalRow) {
                // Handle other row types if needed
                // For now, try to convert to BinaryRow format or use generic approach
                return null;
            }

            return null;
        } catch (Exception e) {
            // If extraction fails for any reason, return null to skip comparison
            return null;
        }
    }

    /**
     * Compares two byte arrays representing field values.
     *
     * @param value field value to compare
     * @param bound range bound value
     * @param fieldType the data type of the field
     * @return negative if value < bound, 0 if value == bound, positive if value > bound
     */
    /**
     * This method is no longer needed since we now use int bounds directly. Kept for backward
     * compatibility but not used.
     */
    @Deprecated
    public int compareFieldValues(byte[] value, byte[] bound, DataType fieldType) {
        // This method is deprecated and should not be used with int bounds
        throw new UnsupportedOperationException("Use compareFieldValueWithInt instead");
    }

    /**
     * Compares a field value (as bytes) with an integer bound value. This is optimized for
     * integer-based range conditions.
     *
     * @param fieldValue field value extracted from BinaryRow as bytes
     * @param intBound integer bound value
     * @param fieldType the data type of the field
     * @return negative if fieldValue < intBound, 0 if equal, positive if fieldValue > intBound
     */
    public int compareFieldValueWithInt(byte[] fieldValue, Integer intBound, DataType fieldType) {
        if (fieldValue == null) {
            return intBound == null ? 0 : -1;
        }
        if (intBound == null) {
            return 1;
        }

        try {
            switch (fieldType.getTypeRoot()) {
                case INTEGER:
                case TIME_WITHOUT_TIME_ZONE:
                case DATE:
                    if (fieldValue.length >= 4) {
                        int fieldIntValue = java.nio.ByteBuffer.wrap(fieldValue).getInt();
                        return Integer.compare(fieldIntValue, intBound);
                    }
                    break;
                case BIGINT:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    if (fieldValue.length >= 8) {
                        long fieldLongValue = java.nio.ByteBuffer.wrap(fieldValue).getLong();
                        // Compare long value with int bound (promote int to long)
                        return Long.compare(fieldLongValue, intBound.longValue());
                    }
                    break;
                case FLOAT:
                    if (fieldValue.length >= 4) {
                        float fieldFloatValue = java.nio.ByteBuffer.wrap(fieldValue).getFloat();
                        return Float.compare(fieldFloatValue, intBound.floatValue());
                    }
                    break;
                case DOUBLE:
                    if (fieldValue.length >= 8) {
                        double fieldDoubleValue = java.nio.ByteBuffer.wrap(fieldValue).getDouble();
                        return Double.compare(fieldDoubleValue, intBound.doubleValue());
                    }
                    break;
                case BOOLEAN:
                    if (fieldValue.length >= 1) {
                        boolean fieldBoolValue = fieldValue[0] != 0;
                        // Convert int to boolean: 0 = false, non-zero = true
                        boolean intBoolBound = intBound != 0;
                        return Boolean.compare(fieldBoolValue, intBoolBound);
                    }
                    break;
                case CHAR:
                case BINARY:
                    // For string types, convert int bound to string and compare
                    String fieldStringValue =
                            new String(fieldValue, java.nio.charset.StandardCharsets.UTF_8);
                    String intBoundStr = intBound.toString();
                    return fieldStringValue.compareTo(intBoundStr);
                default:
                    // For unknown types, convert both to strings and compare
                    String fieldStr =
                            new String(fieldValue, java.nio.charset.StandardCharsets.UTF_8);
                    String boundStr = intBound.toString();
                    return fieldStr.compareTo(boundStr);
            }
        } catch (Exception e) {
            // If comparison fails, exclude this record by returning a non-matching result
            return -1;
        }

        // Fallback: treat as strings and compare
        try {
            String fieldStr = new String(fieldValue, java.nio.charset.StandardCharsets.UTF_8);
            String boundStr = intBound.toString();
            return fieldStr.compareTo(boundStr);
        } catch (Exception e) {
            return -1;
        }
    }

    private int compareBytesLexicographically(byte[] value, byte[] bound) {
        int n = Math.min(value.length, bound.length);
        for (int i = 0; i < n; i++) {
            int cmp = (value[i] & 0xff) - (bound[i] & 0xff);
            if (cmp != 0) {
                return cmp;
            }
        }
        return value.length - bound.length;
    }
}
