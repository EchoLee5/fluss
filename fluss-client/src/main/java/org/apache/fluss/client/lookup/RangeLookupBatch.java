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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch that contains the range lookup operations that send to same destination and some table
 * together.
 */
@Internal
public class RangeLookupBatch {

    /** The table bucket that the lookup operations should fall into. */
    private final TableBucket tableBucket;

    private final List<RangeLookupQuery> rangeLookups;

    public RangeLookupBatch(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
        this.rangeLookups = new ArrayList<>();
    }

    public void addLookup(RangeLookupQuery lookup) {
        rangeLookups.add(lookup);
    }

    public List<RangeLookupQuery> lookups() {
        return rangeLookups;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public void complete(List<List<byte[]>> values) {
        if (values.size() != rangeLookups.size()) {
            completeExceptionally(
                    new FlussRuntimeException(
                            String.format(
                                    "The number of values return by range lookup request is not equal to the number of "
                                            + "range lookups send. Got %d values, but expected %d.",
                                    values.size(), rangeLookups.size())));
        } else {
            for (int i = 0; i < values.size(); i++) {
                AbstractLookupQuery<List<byte[]>> lookup = rangeLookups.get(i);
                lookup.future().complete(values.get(i));
            }
        }
    }

    /** Complete the get operations with given exception. */
    public void completeExceptionally(Exception exception) {
        for (RangeLookupQuery rangeLookup : rangeLookups) {
            rangeLookup.future().completeExceptionally(exception);
        }
    }
}
