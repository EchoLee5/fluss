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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

/**
 * Utilities for handling sort key serialization and composite key construction.
 *
 * <p>This class provides methods to serialize BIGINT sort keys using a special unsigned format that
 * ensures dictionary order equals numeric order, which is crucial for efficient range queries on
 * RocksDB.
 */
@Internal
public class SortKeyUtils {

    /**
     * Serializes a Long value using the special unsigned format for sort keys.
     *
     * <p>This format ensures that: - Negative values are sorted before positive values - Dictionary
     * order (byte-wise comparison) equals numeric order - Long.MIN_VALUE maps to 0x0000000000000000
     * - Long.MAX_VALUE maps to 0xFFFFFFFFFFFFFFFF
     *
     * @param value the Long value to serialize
     * @return 8-byte array representing the serialized value
     */
    public static byte[] serializeSortKeyLong(long value) {
        // Convert to unsigned representation
        long unsigned = value ^ Long.MIN_VALUE;

        // Write 8 bytes in big-endian order
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

    /**
     * Deserializes a Long value from the special unsigned format.
     *
     * @param bytes 8-byte array representing the serialized value
     * @return the deserialized Long value
     * @throws IllegalArgumentException if bytes length is not 8
     */
    public static long deserializeSortKeyLong(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException("SortKey must be 8 bytes, got " + bytes.length);
        }

        // Read 8 bytes in big-endian order
        long unsigned =
                ((((long) bytes[0] & 0xFF) << 56)
                        | (((long) bytes[1] & 0xFF) << 48)
                        | (((long) bytes[2] & 0xFF) << 40)
                        | (((long) bytes[3] & 0xFF) << 32)
                        | (((long) bytes[4] & 0xFF) << 24)
                        | (((long) bytes[5] & 0xFF) << 16)
                        | (((long) bytes[6] & 0xFF) << 8)
                        | (((long) bytes[7] & 0xFF)));

        // Convert back to signed representation
        return unsigned + Long.MIN_VALUE;
    }

    /**
     * Constructs a composite key by appending the serialized sort key to the prefix key.
     *
     * @param prefixKey the prefix key (primary key bytes)
     * @param sortKeyValue the sort key value to append
     * @return composite key = prefixKey + serialized(sortKeyValue)
     */
    public static byte[] buildCompositeKey(byte[] prefixKey, long sortKeyValue) {
        byte[] sortKeyBytes = serializeSortKeyLong(sortKeyValue);
        byte[] compositeKey = new byte[prefixKey.length + sortKeyBytes.length];

        System.arraycopy(prefixKey, 0, compositeKey, 0, prefixKey.length);
        System.arraycopy(sortKeyBytes, 0, compositeKey, prefixKey.length, sortKeyBytes.length);

        return compositeKey;
    }

    /**
     * Extracts the sort key value from a composite key.
     *
     * @param compositeKey the composite key
     * @param prefixKeyLength the length of the prefix key part
     * @return the sort key value
     * @throws IllegalArgumentException if the key structure is invalid
     */
    public static long extractSortKeyValue(byte[] compositeKey, int prefixKeyLength) {
        if (compositeKey.length < prefixKeyLength + 8) {
            throw new IllegalArgumentException("Composite key too short to contain sort key");
        }

        byte[] sortKeyBytes = new byte[8];
        System.arraycopy(compositeKey, prefixKeyLength, sortKeyBytes, 0, 8);

        return deserializeSortKeyLong(sortKeyBytes);
    }

    /**
     * Checks if a composite key has the expected structure for sort key.
     *
     * @param compositeKey the key to check
     * @param expectedPrefixLength the expected prefix length
     * @return true if the key has prefix + 8-byte sort key structure
     */
    public static boolean hasSortKeyStructure(byte[] compositeKey, int expectedPrefixLength) {
        return compositeKey.length == expectedPrefixLength + 8;
    }

    /** Tests the serialization format correctness with various values. */
    public static void testSerializationFormat() {
        System.out.println("=== SortKey Serialization Format Test ===");

        long[] testValues = {
            Long.MIN_VALUE,
            -1000000000L,
            -1L,
            0L,
            1L,
            1000000000L,
            44941312L, // Example query bound
            44957952L, // Example query bound
            Long.MAX_VALUE
        };

        for (long value : testValues) {
            byte[] serialized = serializeSortKeyLong(value);
            long deserialized = deserializeSortKeyLong(serialized);

            System.out.println(
                    "Value: "
                            + value
                            + " → Serialized: "
                            + java.util.Arrays.toString(serialized)
                            + " → Deserialized: "
                            + deserialized
                            + " (correct: "
                            + (value == deserialized)
                            + ")");
        }

        // Test ordering
        System.out.println("\n=== Dictionary Order Test ===");
        for (int i = 0; i < testValues.length - 1; i++) {
            long val1 = testValues[i];
            long val2 = testValues[i + 1];

            byte[] bytes1 = serializeSortKeyLong(val1);
            byte[] bytes2 = serializeSortKeyLong(val2);

            int numericOrder = Long.compare(val1, val2);
            int dictionaryOrder = compareBytes(bytes1, bytes2);

            boolean orderCorrect =
                    (numericOrder < 0 && dictionaryOrder < 0)
                            || (numericOrder > 0 && dictionaryOrder > 0)
                            || (numericOrder == 0 && dictionaryOrder == 0);

            System.out.println(
                    "Values: "
                            + val1
                            + " vs "
                            + val2
                            + " → NumericOrder: "
                            + numericOrder
                            + ", DictionaryOrder: "
                            + dictionaryOrder
                            + " (correct: "
                            + orderCorrect
                            + ")");
        }

        System.out.println("=== End Test ===");
    }

    private static int compareBytes(byte[] bytes1, byte[] bytes2) {
        int minLength = Math.min(bytes1.length, bytes2.length);
        for (int i = 0; i < minLength; i++) {
            int diff = (bytes1[i] & 0xFF) - (bytes2[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(bytes1.length, bytes2.length);
    }
}
