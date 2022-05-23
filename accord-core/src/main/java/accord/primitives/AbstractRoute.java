package accord.primitives;

import accord.api.RoutingKey;

public abstract class AbstractRoute extends RoutingKeys
{
    public AbstractRoute(RoutingKey[] keys)
    {
        super(keys);
    }

    public abstract PartialRoute slice(KeyRanges ranges);

    /**
     * Requires that the ranges are fully covered by this collection
     */
    public abstract PartialRoute sliceStrict(KeyRanges ranges);
}
