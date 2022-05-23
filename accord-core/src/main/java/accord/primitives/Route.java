package accord.primitives;

import accord.api.RoutingKey;

public class Route extends AbstractRoute
{
    public final RoutingKey homeKey;

    public Route(RoutingKey homeKey, RoutingKey[] keys)
    {
        super(keys);
        this.homeKey = homeKey;
    }

    @Override
    public RoutingKeys union(RoutingKeys that)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartialRoute slice(KeyRanges ranges)
    {
        return new PartialRoute(ranges, homeKey, slice(ranges, RoutingKey[]::new));
    }

    @Override
    public PartialRoute sliceStrict(KeyRanges ranges)
    {
        return slice(ranges);
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }
}
