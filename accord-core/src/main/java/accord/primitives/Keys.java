package accord.primitives;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.Key;
import accord.utils.IndexedPredicate;
import accord.utils.SortedArrays;
import org.apache.cassandra.utils.concurrent.Inline;

@SuppressWarnings("rawtypes")
// TODO: this should probably be a BTree
public class Keys implements Iterable<Key>
{
    public interface Fold<V>
    {
        V apply(int index, Key key, V value);
    }

    public interface FoldToLong
    {
        long apply(int index, Key key, long param, long prev);
    }

    public interface IntersectFoldToLong
    {
        long apply(int leftIndex, int rightIndex, Key key, long param, long prev);
    }

    public static final Keys EMPTY = new Keys(new Key[0]);

    final Key[] keys;

    public Keys(SortedSet<? extends Key> keys)
    {
        this.keys = keys.toArray(Key[]::new);
    }

    public Keys(Collection<? extends Key> keys)
    {
        this.keys = keys.toArray(Key[]::new);
        Arrays.sort(this.keys);
    }

    public Keys(Key[] keys)
    {
        this.keys = keys;
        Arrays.sort(keys);
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

    public Keys select(int[] indexes)
    {
        Key[] selection = new Key[indexes.length];
        for (int i = 0 ; i < indexes.length ; ++i)
            selection[i] = keys[indexes[i]];
        return new Keys(selection);
    }

    /**
     * return true if this keys collection contains all keys found in the given keys
     */
    public boolean containsAll(Keys that)
    {
        if (that.isEmpty())
            return true;

        return foldl(that, (li, ri, k, p, v) -> v + 1, 0, 0, 0) == that.size();
    }

    public Keys union(Keys that)
    {
        Key[] result = SortedArrays.linearUnion(keys, that.keys, Key[]::new);
        return result == keys ? this : result == that.keys ? that : new Keys(result);
    }

    public Keys intersect(Keys that)
    {
        Key[] result = SortedArrays.linearIntersection(keys, that.keys, Key[]::new);
        return result == keys ? this : result == that.keys ? that : new Keys(result);
    }

    public boolean isEmpty()
    {
        return keys.length == 0;
    }

    public int size()
    {
        return keys.length;
    }

    public int search(int lowerBound, int upperBound, Object key, Comparator<Object> comparator)
    {
        return Arrays.binarySearch(keys, lowerBound, upperBound, key, comparator);
    }

    public int ceilIndex(int lowerBound, int upperBound, Key key)
    {
        int i = Arrays.binarySearch(keys, lowerBound, upperBound, key);
        if (i < 0) i = -1 - i;
        return i;
    }

    public int ceilIndex(Key key)
    {
        return ceilIndex(0, keys.length, key);
    }

    public int findFirst(Key key, int startIndex)
    {
        return SortedArrays.exponentialSearch(keys, startIndex, keys.length, key);
    }

    public long findNext(int li, Keys that, int ri)
    {
        return findNext(that, ((long)li << 32) | ri);
    }

    public long findNext(Keys that, long packedLiAndRi)
    {
        int li = (int) (packedLiAndRi >>> 32);
        if (li == size())
            return -1;

        int ri = (int) packedLiAndRi;
        while (true)
        {
            ri = SortedArrays.exponentialSearch(that.keys, ri, that.keys.length, get(li));
            if (ri >= 0)
                break;

            ri = -1 - ri;
            if (ri == that.keys.length)
                return -1;

            li = SortedArrays.exponentialSearch(this.keys, li, this.keys.length, that.get(ri));
            if (li >= 0)
                break;

            li = -1 -li;
            if (li == keys.length)
                return -1;
        }
        return ((long)li << 32) | ri;
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

    public Stream<Key> stream()
    {
        return Stream.of(keys);
    }

    @Override
    public Iterator<Key> iterator()
    {
        return new Iterator<>()
        {
            int i = 0;
            @Override
            public boolean hasNext()
            {
                return i < keys.length;
            }

            @Override
            public Key next()
            {
                return keys[i++];
            }
        };
    }

    @Override
    public String toString()
    {
        return stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }

    public static Keys of(Key k0, Key... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = k0;
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = kn[i];

        return new Keys(keys);
    }

    // TODO: optimise for case where nothing is modified
    public Keys intersect(KeyRanges ranges)
    {
        Key[] result = null;
        int resultSize = 0;

        int keyLB = 0;
        int keyHB = size();
        int rangeLB = 0;
        int rangeHB = ranges.rangeIndexForKey(keys[keyHB-1]);
        rangeHB = rangeHB < 0 ? -1 - rangeHB : rangeHB + 1;

        for (;rangeLB<rangeHB && keyLB<keyHB;)
        {
            Key key = keys[keyLB];
            rangeLB = ranges.rangeIndexForKey(rangeLB, ranges.size(), key);

            if (rangeLB < 0)
            {
                rangeLB = -1 -rangeLB;
                if (rangeLB >= rangeHB)
                    break;
                keyLB = ranges.get(rangeLB).lowKeyIndex(this, keyLB, keyHB);
            }
            else
            {
                if (result == null)
                    result = new Key[keyHB - keyLB];
                KeyRange<?> range = ranges.get(rangeLB);
                int highKey = range.higherKeyIndex(this, keyLB, keyHB);
                int size = highKey - keyLB;
                System.arraycopy(keys, keyLB, result, resultSize, size);
                keyLB = highKey;
                resultSize += size;
                rangeLB++;
            }

            if (keyLB < 0)
                keyLB = -1 - keyLB;
        }

        if (result != null && resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);

        return result != null ? new Keys(result) : EMPTY;
    }

    /**
     * Count the number of keys matching the predicate and intersecting with the given ranges.
     * If terminateAfter is greater than 0, the method will return once terminateAfter matches are encountered
     */
    @Inline
    public <V> V foldl(KeyRanges rs, Fold<V> fold, V accumulator)
    {
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = rs.findNext(ri, this, ai);
            if (ari < 0)
                break;

            ai = (int)(ari >>> 32);
            ri = (int)ari;
            KeyRange range = rs.get(ri);
            do
            {
                accumulator = fold.apply(ai, get(ai), accumulator);

                ++ai;
            } while (ai < size() && range.containsKey(get(ai)));
        }

        return accumulator;

    }

    public boolean any(KeyRanges ranges, Predicate<Key> predicate)
    {
        return 1 == foldl(ranges, (i1, key, i2, i3) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean any(IndexedPredicate<Key> predicate)
    {
        return 1 == foldl((i, key, p, v) -> predicate.test(i, key) ? 1 : 0, 0, 0, 1);
    }

    @Inline
    public long foldl(KeyRanges rs, FoldToLong fold, long param, long initialValue, long terminalValue)
    {
        int ai = 0, ri = 0;
        done: while (true)
        {
            long ari = rs.findNext(ri, this, ai);
            if (ari < 0)
                break;

            ai = (int)(ari >>> 32);
            ri = (int)ari;
            KeyRange range = rs.get(ri);
            do
            {
                initialValue = fold.apply(ai, get(ai), param, initialValue);
                if (initialValue == terminalValue)
                    break done;

                ++ai;
            } while (ai < size() && range.containsKey(get(ai)));
        }

        return initialValue;
    }

    public long foldl(FoldToLong fold, long param, long initialValue, long terminalValue)
    {
        for (int i = 0; i < keys.length; i++)
        {
            initialValue = fold.apply(i, keys[i], param, initialValue);
            if (terminalValue == initialValue)
                return initialValue;
        }
        return initialValue;
    }

    public boolean intersects(KeyRanges ranges, Keys keys)
    {
        return foldl(ranges, keys, (li, ri, k, p, v) -> 1, 0, 0, 1) == 1;
    }

    public long foldl(KeyRanges rs, Keys intersect, IntersectFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        Keys as = this, bs = intersect;
        int ai = 0, bi = 0, ri = 0;
        done: while (true)
        {
            long ari = rs.findNext(ri, as, ai);
            if (ari < 0)
                break;

            long bri = rs.findNext(ri, bs, bi);
            if (bri < 0)
                break;

            ai = (int) (ari >>> 32);
            bi = (int) (bri >>> 32);
            ri = (int)bri;
            if ((int)ari == ri)
            {
                KeyRange range = rs.get(ri);
                do
                {
                    initialValue = fold.apply(ai, bi, as.get(ai), param, initialValue);
                    if (initialValue == terminalValue)
                        break done;

                    ++ai;
                    ++bi;
                    long abi = as.findNext(ai, bs, bi);
                    if (abi < 0)
                        break done;

                    ai = (int)(abi >>> 32);
                    bi = (int)abi;
                } while (range.containsKey(as.get(ai)));
            }
        }

        return initialValue;
    }

    public long foldl(Keys intersect, IntersectFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        Keys as = this, bs = intersect;
        int ai = 0, bi = 0;
        while (true)
        {
            long abi = as.findNext(ai, bs, bi);
            if (abi < 0)
                break;

            ai = (int)(abi >>> 32);
            bi = (int)abi;

            initialValue = fold.apply(ai, bi, as.get(ai), param, initialValue);
            if (initialValue == terminalValue)
                break;

            ++ai;
            ++bi;
        }

        return initialValue;
    }

    public int[] remapper(Keys to)
    {
        return SortedArrays.remapper(keys, to.keys);
    }
}
