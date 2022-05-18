package accord.primitives;

import java.util.function.IntFunction;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

@SuppressWarnings("rawtypes")
public class RoutingKeys extends AbstractKeys<RoutingKey, RoutingKeys>
{
    public static final RoutingKeys EMPTY = new RoutingKeys(new RoutingKey[0]);

    public RoutingKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    public RoutingKeys union(RoutingKeys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, factory()), that);
    }

    public RoutingKeys intersect(RoutingKeys that)
    {
        return wrap(SortedArrays.linearIntersection(keys, that.keys, factory()), that);
    }

    public RoutingKeys slice(KeyRanges ranges)
    {
        return wrap(slice(ranges, factory()));
    }

    private RoutingKeys wrap(RoutingKey[] wrap, RoutingKeys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new RoutingKeys(wrap);
    }

    private RoutingKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }

    public Route toRoute(RoutingKey homeKey)
    {
        return new Route(homeKey, keys);
    }

    private static IntFunction<RoutingKey[]> factory()
    {
        return RoutingKey[]::new;
    }
}
