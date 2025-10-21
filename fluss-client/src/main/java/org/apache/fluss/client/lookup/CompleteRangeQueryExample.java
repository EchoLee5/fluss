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
 * Complete example demonstrating the enhanced range query functionality.
 *
 * <p>This example shows the complete architecture and usage of the new range query feature that
 * properly leverages RocksDB's capabilities for efficient range filtering.
 *
 * <h3>Architecture Overview</h3>
 *
 * <pre>
 * Client Request
 *      ↓
 * RangeKeyLookuper (validates prefix + range conditions)
 *      ↓
 * LookupClient (queues range lookup request)
 *      ↓
 * RPC Layer (RangeLookupRequest → Server)
 *      ↓
 * TabletService (processes range lookup)
 *      ↓
 * ReplicaManager (coordinates range lookups)
 *      ↓
 * Replica (delegates to KvTablet)
 *      ↓
 * KvTablet (thread-safe access)
 *      ↓
 * RocksDBKv (performs actual range scan + filtering)
 *      ↓
 * RocksDB Iterator (prefix scan + range filter)
 *      ↓
 * ValueFieldExtractor (decodes values and compares fields)
 *      ↓
 * Filtered Results (returned to client)
 * </pre>
 *
 * <h3>Key Features</h3>
 *
 * <ul>
 *   <li><b>Server-side Filtering</b>: Range conditions are applied in RocksDB layer
 *   <li><b>Efficient Scanning</b>: Uses RocksDB iterator for prefix + range queries
 *   <li><b>Type-safe Comparison</b>: Supports multiple data types with correct comparison logic
 *   <li><b>Graceful Fallback</b>: Falls back to prefix-only queries if range filtering fails
 *   <li><b>Resource Management</b>: Proper cleanup of RocksDB resources
 * </ul>
 *
 * <h3>Usage Example</h3>
 *
 * <pre>{@code
 * // Given a table with:
 * // - Primary key: [userId, categoryId, timestamp]
 * // - Bucket key: [userId, categoryId]
 * // - Additional fields: [amount, description, status]
 *
 * // Step 1: Create range lookuper
 * RangeLookuper rangeLookuper = table.newLookup()
 *     .lookupBy("userId", "categoryId")  // Prefix keys (must be bucket keys)
 *     .createRangeLookuper();
 *
 * // Step 2: Define prefix key
 * InternalRow prefixKey = GenericRow.of(12345L, "electronics");
 *
 * // Step 3: Define range conditions
 * RangeCondition amountRange = RangeCondition.between("amount", 100.0, 1000.0);
 *
 * // Step 4: Execute range query (server-side filtering)
 * CompletableFuture<LookupResult> future = rangeLookuper.rangeLookup(
 *     prefixKey, amountRange, 50);  // Optional limit
 *
 * // Step 5: Process results
 * LookupResult result = future.get();
 * System.out.println("Found " + result.getRowList().size() + " matching records");
 *
 * for (InternalRow row : result.getRowList()) {
 *     // Process each record that matches both:
 *     // 1. Prefix condition: userId=12345 AND categoryId="electronics"
 *     // 2. Range condition: amount BETWEEN 100.0 AND 1000.0
 *     System.out.println("Matching record: " + row);
 * }
 * }</pre>
 *
 * <h3>Range Condition Types</h3>
 *
 * <pre>{@code
 * // Numeric ranges
 * RangeCondition.between("price", 10.0, 100.0);           // 10.0 <= price <= 100.0
 * RangeCondition.greaterThan("age", 18);                  // age > 18
 * RangeCondition.greaterThanOrEqual("score", 80);         // score >= 80
 * RangeCondition.lessThan("attempts", 5);                 // attempts < 5
 * RangeCondition.lessThanOrEqual("rating", 4.5);          // rating <= 4.5
 *
 * // String ranges (lexicographic)
 * RangeCondition.between("name", "Alice", "Charlie");     // "Alice" <= name <= "Charlie"
 *
 * // Time ranges
 * RangeCondition.greaterThan("timestamp", startTime);     // timestamp > startTime
 *
 * // Custom bounds
 * RangeCondition.between("value", 0, 100, false, true);   // 0 < value <= 100
 * }</pre>
 *
 * <h3>Performance Benefits</h3>
 *
 * <ul>
 *   <li><b>Reduced Network Traffic</b>: Only matching records are returned
 *   <li><b>Efficient Storage Access</b>: RocksDB iterator optimizes disk I/O
 *   <li><b>Memory Efficiency</b>: Server-side filtering reduces memory usage
 *   <li><b>Scalable Architecture</b>: Leverages RocksDB's built-in optimization
 * </ul>
 *
 * <h3>Requirements and Limitations</h3>
 *
 * <ul>
 *   <li>Table must have primary key and bucket key defined
 *   <li>Lookup columns must equal bucket keys and be prefix of primary key
 *   <li>For partitioned tables, lookup columns must include partition fields
 *   <li>Range conditions are applied after prefix filtering
 *   <li>Field extraction requires proper value decoder configuration
 * </ul>
 *
 * @since 0.8
 */
public class CompleteRangeQueryExample {

    /**
     * This is a comprehensive documentation and example class.
     *
     * <p>The actual implementation spans multiple modules: - fluss-client: RangeKeyLookuper,
     * RangeCondition, LookupClient - fluss-server: RocksDBKv, ValueFieldExtractor, TabletService -
     * fluss-rpc: RangeLookupRequest/Response protocol messages
     *
     * <p>This architecture ensures that range filtering is performed efficiently at the storage
     * layer (RocksDB) rather than in the client, providing optimal performance for large-scale data
     * queries.
     */
    private CompleteRangeQueryExample() {
        // Documentation class - no instantiation needed
    }
}
