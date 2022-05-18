package accord.primitives;

import accord.api.RoutingKey;

public abstract class AbstractRoute extends RoutingKeys
{
    public AbstractRoute(RoutingKey[] keys)
    {
        super(keys);
    }

    public abstract PartialRoute slice(KeyRanges ranges);
}
