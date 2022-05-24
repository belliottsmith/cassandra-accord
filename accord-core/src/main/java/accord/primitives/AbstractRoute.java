package accord.primitives;

import accord.api.RoutingKey;

public abstract class AbstractRoute extends RoutingKeys
{
    public final RoutingKey homeKey;

    public AbstractRoute(RoutingKey[] keys, RoutingKey homeKey)
    {
        super(keys);
        this.homeKey = homeKey;
    }

    public abstract boolean covers(KeyRanges ranges);

    public abstract PartialRoute slice(KeyRanges ranges);

    /**
     * Requires that the ranges are fully covered by this collection
     */
    public abstract PartialRoute sliceStrict(KeyRanges ranges);

    public static AbstractRoute merge(AbstractRoute prefer, AbstractRoute defer)
    {
        if (prefer == null)
            return defer;
        else if (prefer instanceof PartialRoute && defer instanceof Route)
            return defer;
        else if (prefer instanceof PartialRoute && defer instanceof PartialRoute)
            return ((PartialRoute)prefer).union((PartialRoute) defer);
        else
            return prefer;
    }
}
