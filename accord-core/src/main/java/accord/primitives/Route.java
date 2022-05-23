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
    public PartialRoute slice(KeyRanges newRange)
    {
        newRange = newRange.maximalSlices(this);
        RoutingKey[] keys = slice(newRange, RoutingKey[]::new);
        return new PartialRoute(newRange, homeKey, keys);
    }

    @Override
    public String toString()
    {
        return "{homeKey:" + homeKey + ',' + super.toString() + '}';
    }
}
