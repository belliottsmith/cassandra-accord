package accord.primitives;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.SortedArrays;

public class PartialRoute extends AbstractRoute
{
    public final RoutingKey homeKey;
    public final KeyRanges covering;

    public PartialRoute(KeyRanges covering, RoutingKey homeKey, RoutingKey[] keys)
    {
        super(keys);
        this.covering = covering;
        this.homeKey = homeKey;
    }

    public PartialRoute slice(KeyRanges ranges)
    {
        return slice(ranges, factory());
    }

    public PartialRoute union(PartialRoute that)
    {
        Preconditions.checkState(homeKey.equals(that.homeKey));
        RoutingKey[] keys = SortedArrays.linearUnion(this.keys, that.keys, factory());
        KeyRanges covering = this.covering.union(that.covering);
        if (covering == this.covering && keys == this.keys)
            return this;
        if (covering == that.covering && keys == that.keys)
            return that;
        return new PartialRoute(covering, homeKey, keys);
    }

    private static IntFunction<RoutingKey[]> factory()
    {
        return RoutingKey[]::new;
    }
}
