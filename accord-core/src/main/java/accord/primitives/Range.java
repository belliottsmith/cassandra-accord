/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.primitives;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.Search;

import java.util.Objects;

import javax.annotation.Nullable;

import static accord.api.ProtocolModifiers.RangeSpec.END_INCLUSIVE;
import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * A range of keys
 */
public class Range implements Comparable<RoutableKey>, Unseekable, Seekable, RangeFactory
{
    // used to construct an unsafe Ranges used only for representing an absence of information. Imposes weaker invariants.
    public enum UnsafeMarker { NULLS, ANTI_RANGE }

    private final RoutingKey start;
    private final RoutingKey end;

    protected Range(RoutingKey start, RoutingKey end)
    {
        // TODO (expected): should we at least relax to permit an empty Range?
        Invariants.requireArgument(start.compareTo(end) < 0, "%s >= %s", start, end);
        Invariants.requireArgument(Objects.equals(start.prefix(), end.prefix()), "Range bounds must share their prefix: %s vs %s", start, end);
        this.start = start;
        this.end = end;
    }

    protected Range(RoutingKey start, RoutingKey end, UnsafeMarker antiRange)
    {
        Invariants.requireArgument(antiRange == UnsafeMarker.NULLS || start.compareTo(end) < 0, "%s >= %s", start, end);
        this.start = start;
        this.end = end;
    }

    public RoutingKey start()
    {
        return start;
    }

    public RoutingKey end()
    {
        return end;
    }

    public Object prefix()
    {
        return start.prefix();
    }

    @Override
    public final Domain domain() { return Domain.Range; }

    @Override
    public final Kind kind() { return Kind.Range; }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, contained by,
     * or less than this range.
     */
    @Override
    public final int compareTo(RoutableKey key)
    {
        if (key.compareTo(start) < (END_INCLUSIVE ? 1 : 0))
            return 1;
        if (key.compareTo(end) > (END_INCLUSIVE ? 0 : -1))
            return -1;
        return 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, equal to,
     * or less than the start of this range. This comparison is informed by the inclusivity of the start, i.e.
     * if the raw keys are equal but the start is exclusive, the start is considered to sort after the provided key,
     * so that it falls outside of the range.
     */
    public final int compareStartTo(RoutableKey key)
    {
        int c = start().compareTo(key);
        if (END_INCLUSIVE && c == 0) c = 1;
        return c;
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, contained by,
     * or less than the end of this range. This comparison is informed by the inclusivity of the end, i.e.
     * if the raw keys are equal but the end is exclusive, the end is considered to sort before the provided key,
     * so that it falls outside of the range.
     */
    public final int compareEndTo(RoutableKey key)
    {
        int c = end().compareTo(key);
        if (!END_INCLUSIVE && c == 0) c = -1;
        return c;
    }

    public final boolean startInclusive()
    {
        return !END_INCLUSIVE;
    }

    public final boolean endInclusive()
    {
        return END_INCLUSIVE;
    }

    @Override
    public Range newRange(RoutingKey start, RoutingKey end)
    {
        return new Range(start, end);
    }

    @Override
    public Key asKey()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RoutingKey asRoutingKey()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range asRange()
    {
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range that = (Range) o;
        return this.start.equals(that.start) && this.end.equals(that.end);
    }

    @Override
    public int hashCode()
    {
        return start.hashCode() * 31 + end.hashCode();
    }

    public boolean contains(RoutableKey key)
    {
        return compareTo(key) == 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than, the
     * range intersects this range, or both points are greater than this range
     */
    public int compareIntersecting(Range that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix Range of different types");
        if (this.start.compareTo(that.end) >= 0)
            return 1;
        if (this.end.compareTo(that.start) <= 0)
            return -1;
        return 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than,
     * the range touches or intersects this range, or both points are greater than this range
     */
    public int compareTouching(Range that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix Range of different types");
        if (this.start.compareTo(that.end) > 0)
            return 1;
        if (this.end.compareTo(that.start) < 0)
            return -1;
        return 0;
    }

    /**
     * Sorts by start then end
     */
    public int compare(Range that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix Range of different types");
        int c = this.start.compareTo(that.start);
        if (c == 0) c = this.end.compareTo(that.end);
        return c;
    }

    public boolean contains(Range that)
    {
        return that.start.compareTo(this.start) >= 0 && that.end.compareTo(this.end) <= 0;
    }

    @Override
    public Range slice(Range truncateTo)
    {
        int cs = start.compareTo(truncateTo.start);
        int ce = end.compareTo(truncateTo.end);
        if (cs >= 0 && ce <= 0) return this;
        if (cs <= 0 && ce >= 0) return truncateTo;
        return newRange(cs >= 0 ? start : truncateTo.start, ce <= 0 ? end : truncateTo.end);
    }

    public int compareTo(Range range)
    {
        return compare(range);
    }

    public boolean intersects(AbstractKeys<?> keys)
    {
        return SortedArrays.binarySearch(keys.keys, 0, keys.size(), this, Range::compareTo, FAST) >= 0;
    }

    /**
     * Returns a range covering the overlapping parts of this and the provided range, returns
     * null if the ranges do not overlap
     */
    public Range intersection(Range that)
    {
        if (this.compareIntersecting(that) != 0)
            return null;

        RoutingKey start = this.start.compareTo(that.start) > 0 ? this.start : that.start;
        RoutingKey end = this.end.compareTo(that.end) < 0 ? this.end : that.end;
        return newRange(start, end);
    }

    /**
     * returns the index of the first key larger than what's covered by this range
     */
    public int nextHigherKeyIndex(AbstractKeys<?> keys, int from)
    {
        int i = SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, Range::compareTo, Search.FLOOR);
        if (i < 0) i = -1 - i;
        else i += 1;
        return i;
    }

    /**
     * returns the index of the lowest key contained in this range. If the keys object contains no intersecting
     * keys, <code>(-(<i>insertion point</i>) - 1)</code> is returned. Where <i>insertion point</i> is where an
     * intersecting key would be inserted into the keys array
     * @param keys
     */
    public int nextCeilKeyIndex(Keys keys, int from)
    {
        return SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, Range::compareTo, CEIL);
    }

    @Override
    public RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges)
    {
        if (ranges == null)
            return startInclusive() ? start.toUnseekable() : end.toUnseekable();

        // Use "CEIL" so we return the first match in the list and not any random match seen first.  This can become imporant
        // when different views of Ranges may have more or less non-intersecting ranges in the backing array, causing
        // different nodes to return different matches
        int i = ranges.ceilIndexOf(this);
        Range that = ranges.get(i);
        if (this.start().compareTo(that.start()) <= 0)
        {
            if (startInclusive())
                return that.start();

            if (this.end().compareTo(that.end()) <= 0)
                return this.end();

            return that.end();
        }
        else
        {
            if (startInclusive())
                return this.start();

            if (that.end().compareTo(this.end()) <= 0)
                return that.end();

            return this.end();
        }
    }

    public static Range slice(Range bound, Range toSlice)
    {
        Invariants.requireArgument(bound.compareIntersecting(toSlice) == 0);
        if (bound.contains(toSlice))
            return toSlice;

        return toSlice.newRange(
                toSlice.start().compareTo(bound.start()) >= 0 ? toSlice.start() : bound.start(),
                toSlice.end().compareTo(bound.end()) <= 0 ? toSlice.end() : bound.end()
        );
    }

    @Override
    public Range toUnseekable()
    {
        return this;
    }

    @Override
    public String toString()
    {
        Object prefix = start().prefix();
        return (prefix == null ? "" : prefix + ":") + toSuffixString();
    }

    public String toSuffixString()
    {
        return (startInclusive() ? "[" : "(") + start().printableSuffix() + "," + end().printableSuffix() + (endInclusive() ? ']' : ')');
    }

    public static Range of(RoutingKey start, RoutingKey end)
    {
        return start.rangeFactory().newRange(start, end);
    }
}
