package accord.utils;

import accord.api.RoutingKey;
import accord.primitives.*;
import com.google.common.annotations.VisibleForTesting;

import java.util.function.BiFunction;
import java.util.function.Predicate;

import static accord.utils.SortedArrays.Search.FAST;

public class ReducingRangeMap<V> extends ReducingIntervalMap<RoutingKey, V>
{
    final RoutingKeys endKeys;

    public ReducingRangeMap(V value)
    {
        super(value);
        this.endKeys = RoutingKeys.EMPTY;
    }

    ReducingRangeMap(boolean inclusiveEnds, RoutingKey[] ends, V[] values)
    {
        super(inclusiveEnds, ends, values);
        this.endKeys = RoutingKeys.ofSortedUnique(ends);
    }

    public <V2> V2 foldl(Routables<?, ?> routables, BiFunction<V, V2, V2> fold, V2 initialValue, Predicate<V2> terminate)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError();
            case Key: return foldl((AbstractKeys<?, ?>) routables, fold, initialValue, terminate);
            case Range: return Routables.foldl(endKeys, (AbstractRanges<?>) routables, (f, vs, routingKey, v, index) -> f.apply(vs[index], v), fold, values, initialValue, terminate);
        }
    }

    public <V2> V2 foldl(AbstractKeys<?, ?> keys, BiFunction<V, V2, V2> reduce, V2 accumulator, Predicate<V2> terminate)
    {
        int i = 0, j = 0;
        while (j < keys.size())
        {
            i = endKeys.findNext(i, keys.get(j), FAST);
            if (i < 0) i = -1 - i;
            else if (inclusiveEnds) ++i;

            accumulator = reduce.apply(values[i], accumulator);
            if (i == endKeys.size() || terminate.test(accumulator))
                return accumulator;

            j = keys.findNext(j + 1, endKeys.get(i), FAST);
            if (j < 0) j = -1 - j;
            else if (inclusiveEnds) ++j;
        }
        return accumulator;
    }

    /**
     * returns a copy of this LinearRangeMap limited to the ranges supplied, with all other ranges reporting Ballot.none()
     */
    @VisibleForTesting
    static <V> ReducingRangeMap<V> trim(ReducingRangeMap<V> existing, Ranges ranges, BiFunction<V, V, V> reduce)
    {
        boolean inclusiveEnds = inclusiveEnds(existing.inclusiveEnds, existing.size() > 0, ranges.size() > 0 && ranges.get(0).endInclusive(), ranges.size() > 0);
        ReducingRangeMap.Builder<V> builder = new ReducingRangeMap.Builder<>(inclusiveEnds, existing.size());

        V zero = existing.values[0];
        for (Range select : ranges)
        {
            ReducingIntervalMap<RoutingKey, V>.RangeIterator intersects = existing.intersecting(select.start(), select.end());
            while (intersects.hasNext())
            {
                if (zero.equals(intersects.value()))
                {
                    intersects.next();
                    continue;
                }

                RoutingKey start = intersects.hasStart() && intersects.start().compareTo(select.start()) >= 0 ? intersects.start() : select.start();
                RoutingKey end = intersects.hasEnd() && intersects.end().compareTo(select.end()) <= 0 ? intersects.end() : select.end();

                builder.append(start, zero, reduce);
                builder.append(end, intersects.value(), reduce);
                intersects.next();
            }
        }

        builder.appendLast(zero);
        return builder.build();
    }

    public static <V> ReducingRangeMap<V> merge(ReducingRangeMap<V> historyLeft, ReducingRangeMap<V> historyRight, BiFunction<V, V, V> reduce)
    {
        return ReducingIntervalMap.merge(historyLeft, historyRight, reduce, ReducingRangeMap.Builder::new);
    }

    public static <V> ReducingRangeMap<V> add(ReducingRangeMap<V> existing, Ranges ranges, V value, BiFunction<V, V, V> reduce)
    {
        V zero = existing.values[0];
        boolean inclusiveEnds = inclusiveEnds(existing.inclusiveEnds, existing.size() > 0, ranges.size() > 0 && ranges.get(0).endInclusive(), ranges.size() > 0);
        ReducingRangeMap.Builder<V> builder = new ReducingRangeMap.Builder<>(inclusiveEnds, ranges.size() * 2);
        for (Range range : ranges)
        {
            // don't add a point for an opening min token, since it
            // effectively leaves the bottom of the range unbounded
            builder.append(range.start(), zero, reduce);
            builder.append(range.end(), value, reduce);
        }
        builder.appendLast(zero);

        return merge(existing, builder.build(), reduce, ReducingRangeMap.Builder::new);
    }

    public static ReducingRangeMap<Timestamp> add(ReducingRangeMap<Timestamp> existing, Ranges ranges, Timestamp value)
    {
        return add(existing, ranges, value, Timestamp::max);
    }

    static class Builder<V> extends ReducingIntervalMap.Builder<RoutingKey, V, ReducingRangeMap<V>>
    {
        Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        ReducingRangeMap<V> buildInternal()
        {
            return new ReducingRangeMap<>(inclusiveEnds, ends.toArray(new RoutingKey[0]), (V[])values.toArray(new Object[0]));
        }
    }
}