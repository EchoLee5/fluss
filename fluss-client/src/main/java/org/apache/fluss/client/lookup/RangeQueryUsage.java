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

/**
 * Usage example for the new range query functionality.
 *
 * <p>The range query feature allows you to: 1. First use a prefix key to filter data (using
 * RocksDB's prefix scan) 2. Then apply additional range conditions on other fields (server-side
 * filtering) 3. Optionally limit the number of results returned
 *
 * <p>Basic usage pattern:
 *
 * <pre>{@code
 * // 1. Create a range lookuper with prefix lookup columns
 * RangeLookuper rangeLookuper = table.newLookup()
 *     .lookupBy("userId", "categoryId")  // Must be bucket keys
 *     .createRangeLookuper();
 *
 * // 2. Define prefix key and range condition
 * InternalRow prefixKey = GenericRow.of(12345L, "electronics");
 * RangeCondition rangeCondition = RangeCondition.between("amount", 100.0, 1000.0);
 *
 * // 3. Execute range query
 * CompletableFuture<LookupResult> future = rangeLookuper.rangeLookup(
 *     prefixKey, rangeCondition, 10); // Optional limit
 *
 * // 4. Process results
 * LookupResult result = future.get();
 * for (InternalRow row : result.getRowList()) {
 *     // Process each matching row
 * }
 * }</pre>
 *
 * <p>Available range condition types: - {@code RangeCondition.between(field, lower, upper)} - Range
 * with inclusive bounds - {@code RangeCondition.greaterThan(field, value)} - Lower bound only
 * (exclusive) - {@code RangeCondition.greaterThanOrEqual(field, value)} - Lower bound only
 * (inclusive) - {@code RangeCondition.lessThan(field, value)} - Upper bound only (exclusive) -
 * {@code RangeCondition.lessThanOrEqual(field, value)} - Upper bound only (inclusive)
 *
 * <p>Requirements: - Table must have primary key and bucket key defined - Lookup columns must equal
 * bucket keys and be a prefix of primary key - For partitioned tables, lookup columns must include
 * all partition fields
 *
 * <p>Architecture: - Range filtering is performed on the server side using RocksDB's iterator
 * capabilities - Only filtered results are returned to the client, reducing network overhead -
 * Supports efficient prefix + range queries on large datasets
 *
 * @since 0.8
 */
public class RangeQueryUsage {
    // This is a documentation class - no implementation needed
}
