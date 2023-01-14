package accord.utils;

import accord.api.RoutingKey;
import accord.primitives.*;
import com.google.common.annotations.VisibleForTesting;

import java.util.function.BiFunction;

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

    public <P> V reduce(Routables<?, ?> routables, BiFunction<V, V, V> reduce, V initialValue)
    {
        return Routables.foldl(endKeys, routables, (routingKey, v, index) -> reduce.apply(v, values[index]), initialValue);
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
