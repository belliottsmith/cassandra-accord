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
    public RoutingKeys intersect(RoutingKeys that)
    {
        throw new UnsupportedOperationException();
    }

    public PartialRoute slice(KeyRanges covering)
    {

    }
}
