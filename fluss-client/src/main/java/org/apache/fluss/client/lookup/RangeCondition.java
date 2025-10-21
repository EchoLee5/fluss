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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a range condition for range lookup queries.
 *
 * <p>A range condition specifies a field name and the lower/upper bounds for filtering. Both bounds
 * are optional - if null, they represent unbounded conditions in that direction.
 *
 * @since 0.8
 */
@PublicEvolving
public class RangeCondition implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The field name to apply the range condition on. */
    private final String fieldName;

    /** The lower bound (inclusive), null means no lower bound. */
    private final @Nullable Object lowerBound;

    /** The upper bound (inclusive), null means no upper bound. */
    private final @Nullable Object upperBound;

    /** Whether the lower bound is inclusive. */
    private final boolean lowerInclusive;

    /** Whether the upper bound is inclusive. */
    private final boolean upperInclusive;

    private RangeCondition(
            String fieldName,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean lowerInclusive,
            boolean upperInclusive) {
        this.fieldName = Objects.requireNonNull(fieldName, "Field name cannot be null");
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    /**
     * Creates a range condition with inclusive bounds.
     *
     * @param fieldName the field name to apply the condition on
     * @param lowerBound the lower bound (inclusive), null for no lower bound
     * @param upperBound the upper bound (inclusive), null for no upper bound
     * @return a new RangeCondition instance
     */
    public static RangeCondition between(
            String fieldName, @Nullable Object lowerBound, @Nullable Object upperBound) {
        return new RangeCondition(fieldName, lowerBound, upperBound, true, true);
    }

    /**
     * Creates a range condition with customizable bound inclusivity.
     *
     * @param fieldName the field name to apply the condition on
     * @param lowerBound the lower bound, null for no lower bound
     * @param upperBound the upper bound, null for no upper bound
     * @param lowerInclusive whether the lower bound is inclusive
     * @param upperInclusive whether the upper bound is inclusive
     * @return a new RangeCondition instance
     */
    public static RangeCondition between(
            String fieldName,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean lowerInclusive,
            boolean upperInclusive) {
        return new RangeCondition(
                fieldName, lowerBound, upperBound, lowerInclusive, upperInclusive);
    }

    /**
     * Creates a range condition with only a lower bound.
     *
     * @param fieldName the field name to apply the condition on
     * @param lowerBound the lower bound (inclusive)
     * @return a new RangeCondition instance
     */
    public static RangeCondition greaterThanOrEqual(String fieldName, Object lowerBound) {
        return new RangeCondition(fieldName, lowerBound, null, true, true);
    }

    /**
     * Creates a range condition with only a lower bound (exclusive).
     *
     * @param fieldName the field name to apply the condition on
     * @param lowerBound the lower bound (exclusive)
     * @return a new RangeCondition instance
     */
    public static RangeCondition greaterThan(String fieldName, Object lowerBound) {
        return new RangeCondition(fieldName, lowerBound, null, false, true);
    }

    /**
     * Creates a range condition with only an upper bound.
     *
     * @param fieldName the field name to apply the condition on
     * @param upperBound the upper bound (inclusive)
     * @return a new RangeCondition instance
     */
    public static RangeCondition lessThanOrEqual(String fieldName, Object upperBound) {
        return new RangeCondition(fieldName, null, upperBound, true, true);
    }

    /**
     * Creates a range condition with only an upper bound (exclusive).
     *
     * @param fieldName the field name to apply the condition on
     * @param upperBound the upper bound (exclusive)
     * @return a new RangeCondition instance
     */
    public static RangeCondition lessThan(String fieldName, Object upperBound) {
        return new RangeCondition(fieldName, null, upperBound, true, false);
    }

    // Getters
    public String getFieldName() {
        return fieldName;
    }

    public @Nullable Object getLowerBound() {
        return lowerBound;
    }

    public @Nullable Object getUpperBound() {
        return upperBound;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RangeCondition that = (RangeCondition) o;
        return lowerInclusive == that.lowerInclusive
                && upperInclusive == that.upperInclusive
                && Objects.equals(fieldName, that.fieldName)
                && Objects.equals(lowerBound, that.lowerBound)
                && Objects.equals(upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, lowerBound, upperBound, lowerInclusive, upperInclusive);
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
