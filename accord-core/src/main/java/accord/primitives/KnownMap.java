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

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.utils.ReducingRangeMap;

import static accord.utils.SortedArrays.Search.FAST;

public class KnownMap extends ReducingRangeMap<KnownMap.MinMax>
{
    public static final class MinMax extends Known
    {
        public static final MinMax Nothing = new MinMax(Known.Nothing);

        public final Known min;

        public MinMax(Known minmax)
        {
            this(minmax, minmax);
        }

        public MinMax(Known min, Known max)
        {
            super(max);
            this.min = min.equals(max) ? this : min.getClass() == MinMax.class ? new Known(min) : min;
        }

        public MinMax atLeast(Known that)
        {
            Known max = super.atLeast(that);
            if (max.equals(this))
                return this;
            return new MinMax(min, max);
        }

        MinMax merge(MinMax that)
        {
            Known max = super.atLeast(that);
            Known min = this.min.min(that.min);
            if (max.equals(this) && min.equals(this.min))
                return this;
            if (max.equals(that) && min.equals(that.min))
                return that;
            return new MinMax(min, max);
        }
    }

    public static class SerializerSupport
    {
        public static KnownMap create(boolean inclusiveEnds, RoutingKey[] ends, MinMax[] values)
        {
            return new KnownMap(inclusiveEnds, ends, values);
        }
    }

    public static final KnownMap EMPTY = new KnownMap();

    private transient final Known validForAll;

    private KnownMap()
    {
        this.validForAll = MinMax.Nothing;
    }

    public KnownMap(boolean inclusiveEnds, RoutingKey[] starts, MinMax[] values)
    {
        this(inclusiveEnds, starts, values, MinMax.Nothing);
    }

    private KnownMap(boolean inclusiveEnds, RoutingKey[] starts, MinMax[] values, Known validForAll)
    {
        super(inclusiveEnds, starts, values);
        this.validForAll = validForAll;
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, SaveStatus saveStatus)
    {
        return create(keysOrRanges, saveStatus.known);
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, Known known)
    {
        return create(keysOrRanges, new MinMax(known));
    }

    public static KnownMap create(Unseekables<?> keysOrRanges, MinMax known)
    {
        if (keysOrRanges.isEmpty())
            return new KnownMap();

        return create(keysOrRanges, known, Builder::new);
    }

    public static KnownMap merge(KnownMap a, KnownMap b)
    {
        return ReducingRangeMap.merge(a, b, MinMax::merge, Builder::new);
    }

    public Known computeValidForAll(Unseekables<?> routeOrParticipants)
    {
        Known validForAll = foldlWithDefault(routeOrParticipants, KnownMap::reduceKnownFor, MinMax.Nothing, null, i -> false);
        return this.validForAll.atLeast(validForAll).validForAll();
    }

    public KnownMap with(Known validForAll)
    {
        if (validForAll.equals(this.validForAll))
            return this;

        int i = 0;
        for (; i < size(); ++i)
        {
            Known pre = values[i];
            if (pre == null)
                continue;

            Known post = pre.atLeast(validForAll);
            if (!pre.equals(post))
                break;
        }

        if (i == size())
            return new KnownMap(inclusiveEnds(), starts, values, validForAll);

        RoutingKey[] newStarts = new RoutingKey[size() + 1];
        MinMax[] newValues = new MinMax[size()];
        System.arraycopy(starts, 0, newStarts, 0, i);
        System.arraycopy(values, 0, newValues, 0, i);
        int count = i;
        while (i < size())
        {
            MinMax pre = values[i++];
            MinMax post = pre == null ? null : pre.atLeast(validForAll);
            if (count == 0 || !Objects.equals(post, newValues[count - 1]))
            {
                newStarts[count] = starts[i-1];
                newValues[count++] = post;
            }
        }
        newStarts[count] = starts[size()];
        if (count != newValues.length)
        {
            newValues = Arrays.copyOf(newValues, count);
            newStarts = Arrays.copyOf(newStarts, count + 1);
        }
        return new KnownMap(inclusiveEnds(), newStarts, newValues, validForAll);
    }

    public boolean hasAnyFullyTruncated(Routables<?> routables)
    {
        return foldlWithDefault(routables, (known, prev) -> known.isTruncated() && known.min.isTruncated(), MinMax.Nothing, false, i -> i);
    }

    public boolean hasFullyTruncated(Routables<?> routables)
    {
        return foldlWithDefault(routables, (known, prev) -> known.isTruncated() && known.min.isTruncated(), MinMax.Nothing, true, i -> !i);
    }

    public boolean hasTruncated()
    {
        return foldl((known, prev) -> known.isTruncated(), false, i -> i);
    }

    public Known knownFor(Routables<?> owns, Routables<?> touches)
    {
        Known known = owns.isEmpty() ? knownForAny() : validForAll.atLeast(foldlWithDefault(owns, KnownMap::reduceKnownFor, MinMax.Nothing, null, i -> false));
        if (owns != touches && !touches.isEmpty())
        {
            Known knownDeps = validForAll.atLeast(foldlWithDefault(touches, KnownMap::reduceKnownFor, MinMax.Nothing, null, i -> false));
            known = known.with(knownDeps.deps());
        }
        return known;
    }

    public Known knownForAny()
    {
        return validForAll.atLeast(foldl(Known::atLeast, Known.Nothing, i -> false));
    }

    public Ranges matchingRanges(Predicate<MinMax> match)
    {
        return foldlWithBounds((known, ranges, start, end) -> match.test(known) ? ranges.with(Ranges.of(start.rangeFactory().newRange(start, end))) : ranges, Ranges.EMPTY, i -> false);
    }

    private static Known reduceKnownFor(Known foundKnown, @Nullable Known prev)
    {
        if (prev == null)
            return foundKnown;

        return prev.reduce(foundKnown);
    }

    public Participants<?> knownFor(Known required, Participants<?> expect)
    {
        return foldlWithDefaultAndBounds(expect, (known, prev, start, end) -> {
            if (known == null || !required.isSatisfiedBy(known))
            {
                if (end != null)
                    return prev.without(Ranges.of(start.rangeFactory().newAntiRange(start, end)));

                int i = prev.find(start, FAST);
                if (i < 0)
                    return prev.slice(0, -1 - i);

                if (prev.domain() == Routable.Domain.Key)
                    return prev.slice(0, i + 1);

                Range r = prev.get(i).asRange();
                Range newR = r.start().equals(start) ? start.asRange() : r.newRange(r.start(), start);
                return prev.slice(0, i).with((Participants)Ranges.of(newR));
            }
            return prev;
        }, null, expect, i -> false);
    }

    public static class Builder extends AbstractBoundariesBuilder<RoutingKey, MinMax, KnownMap>
    {
        public Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected KnownMap buildInternal()
        {
            return new KnownMap(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new MinMax[0]));
        }
    }
}

