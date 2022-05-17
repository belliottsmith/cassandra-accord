package accord.primitives;

import accord.api.RoutingKey;

public class Route extends RoutingKeys
{
    public static final Slicer<Route, PartialRoute> SLICER = new Slicer<>()
    {
        @Override
        public PartialRoute slice(Route in, KeyRanges ranges)
        {
            return in.slice(ranges);
        }

        @Override
        public PartialRoute merge(PartialRoute a, PartialRoute b)
        {
            return a.union(b);
        }
    };

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
