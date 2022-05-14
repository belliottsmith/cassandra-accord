package accord.primitives;

import java.util.*;

import accord.api.Key;
import accord.utils.*;

import static accord.utils.ArrayBuffers.cachedKeys;

@SuppressWarnings("rawtypes")
// TODO: this should probably be a BTree
// TODO: check that foldl call-sites are inlined and optimised by HotSpot
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

    /**
     * Creates Keys with the sorted array.  This requires the caller knows that the keys are in fact sorted and should
     * call {@link #of(Key[])} if it isn't known.
     * @param keys sorted
     */
    private Keys(Key[] keys)
    {
        super(keys);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Keys keys1 = (Keys) o;
        return Arrays.equals(keys, keys1.keys);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(keys);
    }

    public int indexOf(Key key)
    {
        return Arrays.binarySearch(keys, key);
    }

    public boolean contains(Key key)
    {
        return indexOf(key) >= 0;
    }

    public Key get(int indexOf)
    {
        return keys[indexOf];
    }

    public boolean isEmpty()
    {
        return keys.length == 0;
    }

    public int size()
    {
        return keys.length;
    }

    /**
     * return true if this keys collection contains all keys found in the given keys
     */
    public boolean containsAll(Keys that)
    {
        if (that.isEmpty())
            return true;

        return foldlIntersect(that, (li, ri, k, p, v) -> v + 1, 0, 0, 0) == that.size();
    }

    public Keys union(Keys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedKeys()), that);
    }

    public Keys slice(KeyRanges ranges)
    {
        return wrap(SortedArrays.sliceWithMultipleMatches(keys, ranges.ranges, Key[]::new, (k, r) -> -r.compareTo(k), KeyRange::compareTo));
    }

    public int findNext(Key key, int startIndex)
    {
        return SortedArrays.exponentialSearch(keys, startIndex, keys.length, key);
    }

    public Keys with(Key key)
    {
        int insertPos = Arrays.binarySearch(keys, key);
        if (insertPos >= 0)
            return this;
        insertPos = -1 - insertPos;

        Key[] src = keys;
        Key[] trg = new Key[src.length + 1];
        System.arraycopy(src, 0, trg, 0, insertPos);
        trg[insertPos] = key;
        System.arraycopy(src, insertPos, trg, insertPos + 1, src.length - insertPos);
        return new Keys(trg);
    }

    public static Keys of(Key key)
    {
        return new Keys(new Key[] { key });
    }

    public static Keys of(Key ... keys)
    {
        return new Keys(sort(keys));
    }

    public static Keys ofSorted(Key ... keys)
    {
        for (int i = 1 ; i < keys.length ; ++i)
        {
            if (keys[i - 1].compareTo(keys[i]) >= 0)
                throw new IllegalArgumentException(Arrays.toString(keys) + " is not sorted");
        }
        return new Keys(keys);
    }

    static Keys ofSortedUnchecked(Key ... keys)
    {
        return new Keys(keys);
    }

    private Keys wrap(Key[] wrap, Keys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }

    private static Key[] sort(Key[] array)
    {
        Arrays.sort(array);
        return array;
    }
}
