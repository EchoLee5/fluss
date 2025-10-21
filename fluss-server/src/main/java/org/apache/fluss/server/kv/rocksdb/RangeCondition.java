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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Server-side representation of a range condition for range lookup queries.
 *
 * <p>This class represents the range conditions to be applied during RocksDB scanning. The bounds
 * are stored as integer values for efficient comparison.
 */
public class RangeCondition {

    private final String fieldName;
    private final @Nullable Integer lowerBound;
    private final @Nullable Integer upperBound;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    public RangeCondition(
            String fieldName,
            @Nullable Integer lowerBound,
            @Nullable Integer upperBound,
            boolean lowerInclusive,
            boolean upperInclusive) {
        this.fieldName = Objects.requireNonNull(fieldName, "Field name cannot be null");
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    public String getFieldName() {
        return fieldName;
    }

    public @Nullable Integer getLowerBound() {
        return lowerBound;
    }

    public @Nullable Integer getUpperBound() {
        return upperBound;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RangeCondition{fieldName='").append(fieldName).append("', ");

        if (lowerBound != null) {
            sb.append(lowerInclusive ? "[" : "(");
            sb.append(lowerBound);
        } else {
            sb.append("(-∞");
        }

        sb.append(", ");

        if (upperBound != null) {
            sb.append(upperBound);
            sb.append(upperInclusive ? "]" : ")");
        } else {
            sb.append("+∞)");
        }

        sb.append("}");
        return sb.toString();
    }
}
