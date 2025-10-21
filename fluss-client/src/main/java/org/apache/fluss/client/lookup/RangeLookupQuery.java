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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a range lookup operation. It contains the table bucket, prefix key, range
 * condition, optional limit, and related CompletableFuture.
 *
 * <p>This extends AbstractLookupQuery to integrate with the existing lookup queue system.
 */
@Internal
public class RangeLookupQuery extends AbstractLookupQuery<List<byte[]>> {

    private final RangeCondition rangeCondition;
    private final @Nullable Integer limit;
    private final CompletableFuture<List<byte[]>> future;

    RangeLookupQuery(
            TableBucket tableBucket,
            byte[] prefixKey,
            RangeCondition rangeCondition,
            @Nullable Integer limit) {
        super(tableBucket, prefixKey);
        this.rangeCondition = rangeCondition;
        this.limit = limit;
        this.future = new CompletableFuture<>();
    }

    public RangeCondition getRangeCondition() {
        return rangeCondition;
    }

    public @Nullable Integer getLimit() {
        return limit;
    }

    @Override
    public CompletableFuture<List<byte[]>> future() {
        return future;
    }

    @Override
    public LookupType lookupType() {
        return LookupType.RANGE_LOOKUP;
    }
}
