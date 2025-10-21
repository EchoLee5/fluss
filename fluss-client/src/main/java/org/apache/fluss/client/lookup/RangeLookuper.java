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

import org.apache.fluss.annotation.PublicEvolving;

/**
 * An extended lookuper that supports range queries on top of prefix key lookup.
 *
 * <p>This interface extends the basic {@link Lookuper} to provide range query capabilities. It
 * allows users to first filter data by a prefix key, then apply additional range conditions on
 * other fields within the filtered dataset.
 *
 * @since 0.8
 */
@PublicEvolving
public interface RangeLookuper extends Lookuper {}
