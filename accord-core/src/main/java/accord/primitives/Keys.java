package accord.primitives;

import java.util.*;
import java.util.function.IntFunction;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.SortedArrays;

public class Keys extends AbstractKeys<Key, Keys>
{
    public static final Keys EMPTY = new Keys(new Key[0]);

    public Keys(SortedSet<? extends Key> keys)
    {
        this(keys.toArray(Key[]::new));
    }

    public Keys(Collection<? extends Key> keys)
    {
        this(sort(keys.toArray(Key[]::new)));
    }

    public Keys(Key[] keys)
    {
        super(keys);
    }

    public Keys union(Keys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, factory()), that);
    }

    public Keys slice(KeyRanges ranges)
    {
        return wrap(slice(ranges, factory()));
    }

    public Route toRoute(RoutingKey homeKey)
    {
        if (isEmpty())
            return new Route(homeKey, new RoutingKey[] { homeKey });

        RoutingKey[] result = toRoutingKeysArray(homeKey);
        int pos = Arrays.binarySearch(result, homeKey);
        return new Route(result[pos], result);
    }

    public PartialRoute toPartialRoute(KeyRanges ranges, RoutingKey homeKey)
    {
        if (isEmpty())
            return new PartialRoute(ranges, homeKey, new RoutingKey[] { homeKey });

        RoutingKey[] result = toRoutingKeysArray(homeKey);
        int pos = Arrays.binarySearch(result, homeKey);
        return new PartialRoute(ranges, result[pos], result);
    }

    private RoutingKey[] toRoutingKeysArray(RoutingKey homeKey)
    {
        RoutingKey[] result;
        int resultCount;
        int insertPos = Arrays.binarySearch(keys, homeKey);
        if (insertPos > 0)
        {
            result = new RoutingKey[keys.length];
            resultCount = copyToRoutingKeys(keys, 0, result, 0, keys.length);
        }
        else
        {
            insertPos = -1 - insertPos;
            result = new RoutingKey[keys.length];
            resultCount = copyToRoutingKeys(keys, 0, result, 0, insertPos);
            if (resultCount == 0 || !homeKey.equals(result[resultCount - 1]))
                result[resultCount++] = homeKey;
            resultCount += copyToRoutingKeys(keys, insertPos, result, resultCount, keys.length - insertPos);
        }

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);

        return result;
    }

    public RoutingKeys toRoutingKeys()
    {
        if (isEmpty())
            return RoutingKeys.EMPTY;

        RoutingKey[] result = new RoutingKey[keys.length];
        int resultCount = copyToRoutingKeys(keys, 0, result, 0, keys.length);
        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);
        return new RoutingKeys(result);
    }

    private static int copyToRoutingKeys(Key[] keys, int srcPos, RoutingKey[] trg, int trgPos, int count)
    {
        if (count == 0)
            return 0;

        int trgStart = trgPos;
        int i = 0;
        if (trgPos == 0)
        {
            trg[trgPos++] = keys[srcPos++].toRoutingKey();
            ++i;
        }

        while (i < count)
        {
            RoutingKey next = keys[srcPos + i].toRoutingKey();
            if (!next.equals(trg[trgPos - 1]))
                trg[trgPos++] = next;
            ++i;
        }

        return trgPos - trgStart;
    }

    private Keys wrap(Key[] wrap, Keys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }

    private static IntFunction<Key[]> factory()
    {
        return Key[]::new;
    }

    public static Keys of(Key key)
    {
        return new Keys(new Key[] { key });
    }

    public static Keys of(Key ... keys)
    {
        return new Keys(sort(keys));
    }

    private static Key[] sort(Key[] array)
    {
        Arrays.sort(array);
        return array;
    }
}
