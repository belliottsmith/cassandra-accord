package accord.primitives;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.RoutingKey;
import accord.utils.IndexedFold;
import accord.utils.IndexedFoldIntersectToLong;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedPredicate;
import accord.utils.SortedArrays;
import org.apache.cassandra.utils.concurrent.Inline;

@SuppressWarnings("rawtypes")
// TODO: check that foldl call-sites are inlined and optimised by HotSpot
public abstract class AbstractKeys<K extends RoutingKey, KS extends AbstractKeys<K, KS>> implements Iterable<K>
{
    final K[] keys;

    protected AbstractKeys(K[] keys)
    {
        this.keys = keys;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeys that = (AbstractKeys) o;
        return Arrays.equals(keys, that.keys);
    }

//    public abstract KS union(KS that);
//    public abstract KS intersect(KS that);
//    public abstract KS slice(KeyRanges ranges);
//    abstract KS empty();

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(keys);
    }

    public int indexOf(K key)
    {
        return Arrays.binarySearch(keys, key);
    }

    public boolean contains(K key)
    {
        return indexOf(key) >= 0;
    }

    public K get(int indexOf)
    {
        return keys[indexOf];
    }

    /**
     * return true if this keys collection contains all keys found in the given keys
     */
    public boolean containsAll(AbstractKeys<K, ?> that)
    {
        if (that.isEmpty())
            return true;

        return foldlIntersect(that, (li, ri, k, p, v) -> v + 1, 0, 0, 0) == that.size();
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

    public int find(K key, int startIndex)
    {
        return SortedArrays.exponentialSearch(keys, startIndex, keys.length, key);
    }

    // returns thisIdx in top 32 bits, thatIdx in bottom
    public long findNextIntersection(int thisIdx, AbstractKeys<K, ?> that, int thatIdx)
    {
        return SortedArrays.findNextIntersection(this.keys, thisIdx, that.keys, thatIdx);
    }

    public Stream<K> stream()
    {
        return Stream.of(keys);
    }

    @Override
    public Iterator<K> iterator()
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
            public K next()
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

    // TODO: optimise for case where nothing is modified
    // TODO: generalise and move to SortedArrays
    protected K[] slice(KeyRanges ranges, IntFunction<K[]> factory)
    {
        K[] result = null;
        int resultSize = 0;

        int keyLB = 0;
        int keyHB = keys.length;
        int rangeLB = 0;
        int rangeHB = ranges.rangeIndexForKey(keys[keyHB-1]);
        rangeHB = rangeHB < 0 ? -1 - rangeHB : rangeHB + 1;

        for (;rangeLB<rangeHB && keyLB<keyHB;)
        {
            K key = keys[keyLB];
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
                    result = factory.apply(keyHB - keyLB);
                KeyRange range = ranges.get(rangeLB);
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

        return result;
    }

    /**
     * Count the number of keys matching the predicate and intersecting with the given ranges.
     * If terminateAfter is greater than 0, the method will return once terminateAfter matches are encountered
     */
    @Inline
    public <V> V foldl(KeyRanges rs, IndexedFold<K, V> fold, V accumulator)
    {
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = rs.findNextIntersection(ri, this, ai);
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

    public boolean any(KeyRanges ranges, Predicate<K> predicate)
    {
        return 1 == foldl(ranges, (i1, key, i2, i3) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean any(IndexedPredicate<K> predicate)
    {
        return 1 == foldl((i, key, p, v) -> predicate.test(i, key) ? 1 : 0, 0, 0, 1);
    }

    @Inline
    public long foldl(KeyRanges rs, IndexedFoldToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        int ai = 0, ri = 0;
        done: while (true)
        {
            long ari = rs.findNextIntersection(ri, this, ai);
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

    @Inline
    public long foldl(IndexedFoldToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        for (int i = 0; i < keys.length; i++)
        {
            initialValue = fold.apply(i, keys[i], param, initialValue);
            if (terminalValue == initialValue)
                return initialValue;
        }
        return initialValue;
    }

    public boolean intersects(KeyRanges ranges, KS keys)
    {
        return foldlIntersect(ranges, keys, (li, ri, k, p, v) -> 1, 0, 0, 1) == 1;
    }

    @Inline
    public long foldlIntersect(KeyRanges rs, AbstractKeys<K, ?> intersect, IndexedFoldIntersectToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        AbstractKeys<K, ?> as = this, bs = intersect;
        int ai = 0, bi = 0, ri = 0;
        done: while (true)
        {
            long ari = rs.findNextIntersection(ri, as, ai);
            if (ari < 0)
                break;

            long bri = rs.findNextIntersection(ri, bs, bi);
            if (bri < 0)
                break;

            ai = (int) (ari >>> 32);
            bi = (int) (bri >>> 32);
            if ((int)ari == (int)bri)
            {
                ri = (int)ari;
                KeyRange range = rs.get(ri);
                do
                {
                    initialValue = fold.apply(ai, bi, as.get(ai), param, initialValue);
                    if (initialValue == terminalValue)
                        break done;

                    ++ai;
                    ++bi;
                    long abi = as.findNextIntersection(ai, bs, bi);
                    if (abi < 0)
                        break done;

                    ai = (int)(abi >>> 32);
                    bi = (int)abi;
                } while (range.containsKey(as.get(ai)));
            }
            else
            {
                ri = Math.max((int)ari, (int)bri);
            }
        }

        return initialValue;
    }

    @Inline
    public long foldlIntersect(AbstractKeys<K, ?> intersect, IndexedFoldIntersectToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        AbstractKeys<K, ?> as = this, bs = intersect;
        int ai = 0, bi = 0;
        while (true)
        {
            long abi = as.findNextIntersection(ai, bs, bi);
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

    @Inline
    public long foldlDifference(AbstractKeys<K, ?> subtract, IndexedFoldToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        AbstractKeys<K, ?> as = this, bs = subtract;
        int ai = 0, bi = 0;
        while (ai < as.size())
        {
            long abi = as.findNextIntersection(ai, bs, bi);
            int next;
            if (abi < 0)
            {
                next = as.size();
            }
            else
            {
                next = (int)(abi >>> 32);
                bi = (int)abi;
            }

            while (ai < next)
            {
                initialValue = fold.apply(ai, as.get(ai), param, initialValue);
                if (initialValue == terminalValue)
                    break;
                ++ai;
            }
        }
        return initialValue;
    }

    public int[] remapper(KS target, boolean isTargetKnownSuperset)
    {
        return SortedArrays.remapper(keys, target.keys, isTargetKnownSuperset);
    }
}
