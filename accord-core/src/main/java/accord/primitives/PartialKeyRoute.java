package accord.primitives;

import accord.utils.Invariants;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;

/**
 * A slice of a Route that covers
 */
public class PartialKeyRoute extends KeyRoute implements PartialRoute<RoutingKey>
{
    public static class SerializationSupport
    {
        public static PartialKeyRoute create(Ranges covering, RoutingKey homeKey, RoutingKey[] keys)
        {
            return new PartialKeyRoute(covering, homeKey, keys);
        }
    }

    public final Ranges covering;

    public PartialKeyRoute(Ranges covering, RoutingKey homeKey, RoutingKey[] keys)
    {
        super(homeKey, keys);
        this.covering = covering;
    }

    public static PartialKeyRoute empty(RoutingKey homeKey)
    {
        return new PartialKeyRoute(Ranges.EMPTY, homeKey, RoutingKeys.EMPTY.keys);
    }

    @Override
    public PartialKeyRoute sliceStrict(Ranges newRanges)
    {
        if (!covering.containsAll(newRanges))
            throw new IllegalArgumentException("Not covered");

        return slice(newRanges);
    }

    @Override
    public PartialKeyRoute slice(Ranges newRanges)
    {
        RoutingKey[] keys = slice(newRanges, RoutingKey[]::new);
        return new PartialKeyRoute(newRanges, homeKey, keys);
    }

    @Override
    public Route<RoutingKey> union(Route<RoutingKey> that)
    {
        if (that.kind().isFullRoute()) return that;
        return union((PartialKeyRoute) that);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.PartialKeyRoute;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return covering.containsAll(ranges);
    }

    @Override
    public AbstractUnseekableKeys<?> with(RoutingKey withKey)
    {
        if (contains(withKey))
            return this;

        return new RoutingKeys(toRoutingKeysArray(withKey));
    }

    public PartialKeyRoute slice(Ranges newRanges, Slice slice)
    {
        if (newRanges.containsAll(covering))
            return this;

        RoutingKey[] keys = slice(newRanges, RoutingKey[]::new);
        return new PartialKeyRoute(newRanges, homeKey, keys);
    }

    @Override
    public Ranges covering()
    {
        return covering;
    }

    @Override
    public PartialKeyRoute union(PartialRoute<RoutingKey> with)
    {
        if (!(with instanceof PartialKeyRoute))
            throw new IllegalArgumentException();

        PartialKeyRoute that = (PartialKeyRoute) with;
        Invariants.checkState(homeKey.equals(that.homeKey));
        RoutingKey[] keys = SortedArrays.linearUnion(this.keys, that.keys, RoutingKey[]::new);
        Ranges covering = this.covering.with(that.covering);
        if (covering == this.covering && keys == this.keys)
            return this;
        if (covering == that.covering && keys == that.keys)
            return that;
        return new PartialKeyRoute(covering, homeKey, keys);
    }
}
