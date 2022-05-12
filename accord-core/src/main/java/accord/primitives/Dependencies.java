package accord.primitives;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandStore;
import accord.utils.InlineHeap;
import accord.utils.SortedArrays;

import static accord.utils.SortedArrays.remap;
import static accord.utils.SortedArrays.remapper;

// TODO (now): switch to RoutingKey
public class Dependencies implements Iterable<Map.Entry<Key, TxnId>>
{
    public static final Dependencies NONE = new Dependencies(Keys.EMPTY, new TxnId[0], new int[0]);

    public static class Entry implements Map.Entry<Key, TxnId>
    {
        final Key key;
        final TxnId txnId;

        public Entry(Key key, TxnId txnId)
        {
            this.key = key;
            this.txnId = txnId;
        }

        @Override
        public Key getKey()
        {
            return key;
        }

        @Override
        public TxnId getValue()
        {
            return txnId;
        }

        @Override
        public TxnId setValue(TxnId value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return key + "->" + txnId;
        }
    }

    public static class Builder
    {
        final Keys keys;
        final Map<TxnId, Integer> txnIdLookup = new HashMap<>(); // TODO: primitive map
        TxnId[] txnIds = new TxnId[4];
        final int[][] keysToTxnId;
        final int[] keysToTxnIdCounts;

        public Builder(Keys keys)
        {
            this.keys = keys;
            this.keysToTxnId = new int[keys.size()][4];
            this.keysToTxnIdCounts = new int[keys.size()];
        }

        public boolean isEmpty()
        {
            return Arrays.stream(keysToTxnIdCounts).allMatch(i -> i == 0);
        }

        public Builder add(Command command)
        {
            int idx = ensureTxnIdx(command.txnId());
            keys.foldl(command.txn().keys, (li, ri, k, p, v) -> {
                if (keysToTxnId[li].length == keysToTxnIdCounts[li])
                    keysToTxnId[li] = Arrays.copyOf(keysToTxnId[li], keysToTxnId[li].length * 2);
                keysToTxnId[li][keysToTxnIdCounts[li]++] = idx;
                return 0;
            }, 0, 0, 1);
            return this;
        }

        public Builder add(Key key, TxnId txnId)
        {
            int txnIdx = ensureTxnIdx(txnId);
            int keyIdx = keys.indexOf(key);
            if (keysToTxnIdCounts[keyIdx] == keysToTxnId[keyIdx].length)
                keysToTxnId[keyIdx] = Arrays.copyOf(keysToTxnId[keyIdx], Math.max(4, keysToTxnIdCounts[keyIdx] * 2));
            keysToTxnId[keyIdx][keysToTxnIdCounts[keyIdx]++] = txnIdx;
            return this;
        }

        public boolean contains(TxnId txnId)
        {
            return txnIdx(txnId) >= 0;
        }

        private int txnIdx(TxnId txnId)
        {
            return txnIdLookup.getOrDefault(txnId, -1);
        }

        private int ensureTxnIdx(TxnId txnId)
        {
            return txnIdLookup.computeIfAbsent(txnId, ignore -> {
                if (txnIds.length == txnIdLookup.size())
                    txnIds = Arrays.copyOf(txnIds, txnIds.length * 2);
                return txnIdLookup.size();
            });
        }

        public Dependencies build()
        {
            TxnId[] txnIds = txnIdLookup.keySet().toArray(TxnId[]::new);
            Arrays.sort(txnIds, TxnId::compareTo);
            int[] txnIdMap = new int[txnIds.length];
            for (int i = 0 ; i < txnIdMap.length ; i++)
                txnIdMap[txnIdLookup.get(txnIds[i])] = i;

            int keyCount = 0;
            int[] result; {
                int count = 0;
                for (int i = 0 ; i < keys.size() ; ++i)
                {
                    keyCount += keysToTxnIdCounts[i] > 0 ? 1 : 0;
                    count += keysToTxnIdCounts[i];
                }
                result = new int[count + keyCount];
            }

            int keyIndex = 0;
            for (int i = 0 ; i < keys.size() ; ++i)
            {
                if (keysToTxnIdCounts[i] > 0)
                {
                    int count = keysToTxnIdCounts[i];
                    int offset = keyIndex == 0 ? keyCount : result[keyIndex - 1];
                    result[keyIndex] = offset + count;
                    int[] src = keysToTxnId[i];
                    for (int j = 0 ; j < count ; ++j)
                        result[j + offset] = txnIdMap[src[j]];
                    Arrays.sort(result, offset, count + offset);
                    // TODO: filter duplicates
                    ++keyIndex;
                }
            }

            Keys keys = this.keys;
            if (keyCount < keys.size())
            {
                keyIndex = 0;
                Key[] newKeys = new Key[keyCount];
                for (int i = 0 ; i < keys.size() ; ++i)
                {
                    if (keysToTxnIdCounts[i] > 0)
                        newKeys[keyIndex++] = keys.get(i);
                }
                keys = new Keys(newKeys);
            }

            return new Dependencies(keys, txnIds, result);
        }
    }

    public static Builder builder(Keys keys)
    {
        return new Builder(keys);
    }

    static class MergeStream
    {
        // TODO: could share backing array for all of these if we want, with an additional offset
        final int[] input;
        final int[] remap; // TODO: use cached backing array
        final int[] keys; // TODO: use cached backing array
        int keyIndex;
        int index;
        int endIndex;

        MergeStream(Keys keys, TxnId[] txnIds, Dependencies dependencies)
        {
            this.input = dependencies.keyToTxnId;
            this.remap = remapper(dependencies.txnIds, txnIds);
            this.keys = dependencies.keys.remapper(keys);
            this.keyIndex = 0;
            this.index = dependencies.keys.size();
            this.endIndex = input[0];
        }
    }

    public static <T> Dependencies merge(Keys keys, List<T> merge, Function<T, Dependencies> getter)
    {
        if (merge.size() == 2)
        {
            Dependencies left = getter.apply(merge.get(0));
            Dependencies right = getter.apply(merge.get(1));
            if (left == null || right == null)
            {
                if (left == null && right == null) return NONE;
                else return left == null ? right : left;
            }
            return left.with(right);
        }

        // TODO: use Cassandra MergeIterator to perform more efficient merge of TxnId
        TxnId[] txnIds = NONE.txnIds;
        for (T t : merge)
        {
            Dependencies dependencies = getter.apply(t);
            if (dependencies != null)
                txnIds = SortedArrays.linearUnion(txnIds, dependencies.txnIds, TxnId[]::new);
        }

        if (txnIds.length == 0)
            return NONE;

        int mergeSize = merge.size();
        MergeStream[] streams = new MergeStream[mergeSize];
        int[] result; {
            int maxStreamSize = 0, totalStreamSize = 0;
            for (int stream = 0 ; stream < mergeSize ; ++stream)
            {
                Dependencies dependencies = getter.apply(merge.get(stream));
                if (dependencies != null)
                    streams[stream] = new MergeStream(keys, txnIds, dependencies);
            }
            result = new int[Math.min(maxStreamSize * 2, totalStreamSize)]; // TODO: use cached temporary array
        }

        int resultIndex = keys.size();
        int[] keyHeap = InlineHeap.create(mergeSize); // TODO: use cached temporary array
        int[] txnIdHeap = InlineHeap.create(mergeSize); // TODO: use cached temporary array

        // build a heap of keys and streams, so we can merge those streams with overlapping keys
        int keyHeapSize = 0;
        for (int stream = 0 ; stream < mergeSize ; ++stream)
        {
            if (streams[stream] != null)
                InlineHeap.set(keyHeap, keyHeapSize++, remap(0, streams[stream].keys), stream);
        }

        int keyIndex = 0;
        keyHeapSize = InlineHeap.heapify(keyHeap, keyHeapSize);
        while (keyHeapSize > 0)
        {
            // while the heap is non-empty, pop the streams matching the top key and insert them into their own heap
            int txnIdHeapSize = InlineHeap.consume(keyHeap, keyHeapSize, (key, streamIndex, size) -> {
                MergeStream stream = streams[streamIndex];
                InlineHeap.set(txnIdHeap, size, remap(stream.input[stream.index], stream.remap), streamIndex);
                return size + 1;
            }, 0);

            if (txnIdHeapSize > 1)
            {
                txnIdHeapSize = InlineHeap.heapify(txnIdHeap, txnIdHeapSize);
                do
                {
                    if (resultIndex + txnIdHeapSize >= result.length)
                        result = Arrays.copyOf(result, Math.max(resultIndex + txnIdHeapSize, resultIndex + (resultIndex/2)));

                    result[resultIndex++] = InlineHeap.key(txnIdHeap, 0);
                    InlineHeap.consume(txnIdHeap, txnIdHeapSize, (key, stream, v) -> 0, 0);

                    txnIdHeapSize = InlineHeap.advance(txnIdHeap, txnIdHeapSize, streamIndex -> {
                        MergeStream stream = streams[streamIndex];
                        int index = ++stream.index;
                        if (index == stream.endIndex)
                        {
                            stream.endIndex = stream.input[++stream.keyIndex];
                            return Integer.MIN_VALUE;
                        }
                        return remap(stream.input[index], stream.remap);
                    });
                }
                while (txnIdHeapSize > 1);
            }

            // fast path when one remaining source for this key
            if (txnIdHeapSize > 0)
            {

                int streamIndex = InlineHeap.stream(txnIdHeap, 0);
                MergeStream stream = streams[streamIndex];
                int index = stream.index;
                int endIndex = stream.endIndex;
                int count = endIndex - index;
                if (result.length < resultIndex + count)
                    result = Arrays.copyOf(result, Math.max(result.length + (result.length / 2), resultIndex + count));

                while (index < endIndex)
                    result[resultIndex++] = remap(stream.input[index++], stream.remap);

                stream.index = index;
                stream.endIndex = stream.input[++stream.keyIndex];
            }

            result[keyIndex++] = resultIndex;
            keyHeapSize = InlineHeap.advance(keyHeap, keyHeapSize, streamIndex -> {
                MergeStream stream = streams[streamIndex];
                // keyIndex should already have been advanced by the txnId copying
                // TODO (now): stream.keys may be null - cleanest way to handle?
                return stream.keyIndex == stream.keys.length
                       ? Integer.MIN_VALUE
                       : remap(stream.keyIndex, stream.keys);
            });
        }

        if (resultIndex < result.length)
            result = Arrays.copyOf(result, resultIndex);

        return new Dependencies(keys, txnIds, result);
    }

    private final Keys keys; // unique Keys
    private final TxnId[] txnIds; // unique TxnId TODO: this should perhaps be a BTree?

    // first N entries are offsets for each src item, remainder are pointers into value set (either keys or txnIds)
    private final int[] keyToTxnId; // Key -> [TxnId]
    private int[] txnIdToKey; // TxnId -> [Key]

    private Dependencies(Keys keys, TxnId[] txnIds, int[] keyToTxnId)
    {
        this.keys = keys;
        this.txnIds = txnIds;
        this.keyToTxnId = keyToTxnId;
    }

    public Dependencies select(KeyRanges ranges)
    {
        if (isEmpty())
            return this;

        Keys select = keys.intersect(ranges);
        if (select.isEmpty())
            return NONE;

        if (select.size() == keys.size())
            return this;

        int i = 0;
        int offset = select.size();
        for (int j = 0 ; j < select.size() ; ++j)
        {
            i = keys.find(select.get(j), i);
            offset += keyToTxnId[i] - (i == 0 ? keys.size() : keyToTxnId[i - 1]);
        }

        int[] src = keyToTxnId;
        int[] trg = new int[offset];

        i = 0;
        offset = select.size();
        for (int j = 0 ; j < select.size() ; ++j)
        {
            i = keys.find(select.get(j), i);
            int start = i == 0 ? keys.size() : src[i - 1];
            int count = src[i] - start;
            System.arraycopy(src, start, trg, offset, count);
            offset += count;
            trg[j] = offset;
        }

        TxnId[] txnIds = trimUnusedTxnId(select, this.txnIds, trg);
        return new Dependencies(select, txnIds, trg);
    }

    private static TxnId[] trimUnusedTxnId(Keys keys, TxnId[] txnIds, int[] keysToTxnId)
    {
        int[] remapTxnId = new int[txnIds.length];
        for (int i = keys.size() ; i < keysToTxnId.length ; ++i)
            remapTxnId[keysToTxnId[i]] = 1;

        int offset = 0;
        for (int i = 0 ; i < remapTxnId.length ; ++i)
        {
            if (remapTxnId[i] == 1) remapTxnId[i] = offset++;
            else remapTxnId[i] = -1;
        }

        TxnId[] result = txnIds;
        if (offset < remapTxnId.length)
        {
            result = new TxnId[offset];
            for (int i = 0 ; i < txnIds.length ; ++i)
            {
                if (remapTxnId[i]>= 0)
                    result[remapTxnId[i]] = txnIds[i];
            }
            for (int i = keys.size() ; i < keysToTxnId.length ; ++i)
                keysToTxnId[i] = remapTxnId[keysToTxnId[i]];
        }

        return result;
    }

    public Dependencies with(Dependencies that)
    {
        Keys keys = this.keys.union(that.keys);
        TxnId[] txnIds = SortedArrays.linearUnion(this.txnIds, that.txnIds, TxnId[]::new);
        int[] remapLeft = remapper(this.txnIds, txnIds);
        int[] remapRight = remapper(that.txnIds, txnIds);
        Keys leftKeys = this.keys, rightKeys = that.keys;
        int[] left = keyToTxnId, right = that.keyToTxnId;
        int[] out = null;
        int lk = 0, rk = 0, ok = 0, l = this.keys.size(), r = that.keys.size(), o = keys.size();

        if (remapLeft == null && keys == leftKeys)
        {
            noOp: while (lk < leftKeys.size() && rk < rightKeys.size())
            {
                int ck = leftKeys.get(lk).compareTo(rightKeys.get(rk));
                if (ck < 0)
                {
                    o += left[lk] - l;
                    l = left[lk];
                    assert o == l && ok == lk && left[ok] == o;
                    ok++;
                    lk++;
                }
                else if (ck > 0)
                {
                    throw new IllegalStateException();
                }
                else
                {
                    while (l < left[lk] && r < right[rk])
                    {
                        int nextLeft = left[l];
                        int nextRight = remap(right[r], remapRight);

                        if (nextLeft < nextRight)
                        {
                            o++;
                            l++;
                        }
                        else if (nextRight < nextLeft)
                        {
                            out = copy(left, o, left.length + right.length - r);
                            break noOp;
                        }
                        else
                        {
                            o++;
                            l++;
                            r++;
                        }
                    }

                    if (l < left[lk])
                    {
                        o += left[lk] - l;
                        l = left[lk];
                    }
                    else if (r < right[rk])
                    {
                        out = copy(left, o, left.length + right.length - r);
                        break;
                    }

                    assert o == l && ok == lk && left[ok] == o;
                    ok++;
                    rk++;
                    lk++;
                }
            }

            if (out == null)
                return this;
        }
        else if (remapRight == null && keys == rightKeys)
        {
            noOp: while (lk < leftKeys.size() && rk < rightKeys.size())
            {
                int ck = leftKeys.get(lk).compareTo(rightKeys.get(rk));
                if (ck < 0)
                {
                    throw new IllegalStateException();
                }
                else if (ck > 0)
                {
                    o += right[rk] - r;
                    r = right[rk];
                    assert o == r && ok == rk && right[ok] == o;
                    ok++;
                    rk++;
                }
                else
                {
                    while (l < left[lk] && r < right[rk])
                    {
                        int nextLeft = remap(left[l], remapLeft);
                        int nextRight = right[r];

                        if (nextLeft < nextRight)
                        {
                            out = copy(right, o, left.length + right.length - r);
                            break noOp;
                        }
                        else if (nextRight < nextLeft)
                        {
                            o++;
                            r++;
                        }
                        else
                        {
                            o++;
                            l++;
                            r++;
                        }
                    }

                    if (l < left[lk])
                    {
                        out = copy(right, o, left.length + right.length - r);
                    }
                    else if (r < right[rk])
                    {
                        o += right[rk] - r;
                        r = right[rk];
                        break;
                    }

                    assert o == r && ok == rk && right[ok] == o;
                    ok++;
                    rk++;
                    lk++;
                }
            }

            if (out == null)
                return that;
        }
        else
        {
            out = new int[left.length + right.length];
        }

        while (lk < leftKeys.size() && rk < rightKeys.size())
        {
            int ck = leftKeys.get(lk).compareTo(rightKeys.get(rk));
            if (ck < 0)
            {
                while (l < left[lk])
                    out[o++] = remap(left[l++], remapLeft);
                out[ok++] = o;
                lk++;
            }
            else if (ck > 0)
            {
                while (r < right[rk])
                    out[o++] = remap(right[r++], remapRight);
                out[ok++] = o;
                rk++;
            }
            else
            {
                while (l < left[lk] && r < right[rk])
                {
                    int nextLeft = remap(left[l], remapLeft);
                    int nextRight = remap(right[r], remapRight);

                    if (nextLeft <= nextRight)
                    {
                        out[o++] = nextLeft;
                        l += 1;
                        r += nextLeft == nextRight ? 1 : 0;
                    }
                    else
                    {
                        out[o++] = nextRight;
                        ++r;
                    }
                }

                while (l < left[lk])
                    out[o++] = remap(left[l++], remapLeft);

                while (r < right[rk])
                    out[o++] = remap(right[r++], remapRight);

                out[ok++] = o;
                rk++;
                lk++;
            }
        }

        while (lk < leftKeys.size())
        {
            while (l < left[lk])
                out[o++] = remap(left[l++], remapLeft);
            out[ok++] = o;
            lk++;
        }

        while (rk < rightKeys.size())
        {
            while (r < right[rk])
                out[o++] = remap(right[r++], remapRight);
            out[ok++] = o;
            rk++;
        }

        if (o < out.length)
            out = Arrays.copyOf(out, o);

        return new Dependencies(keys, txnIds, out);
    }

    private static int[] copy(int[] src, int to, int length)
    {
        int[] result = new int[length];
        System.arraycopy(src, 0, result, 0, to);
        return result;
    }

    // TODO: optimise for case where none removed
    public Dependencies without(Predicate<TxnId> remove)
    {
        int[] remapTxnIds = new int[txnIds.length];
        TxnId[] txnIds; {
            int count = 0;
            for (int i = 0 ; i < this.txnIds.length ; ++i)
            {
                if (remove.test(this.txnIds[i])) remapTxnIds[i] = -1;
                else remapTxnIds[i] = count++;
            }

            if (count == remapTxnIds.length)
                return this;

            txnIds = new TxnId[count];
            for (int i = 0 ; i < this.txnIds.length ; ++i)
            {
                if (remapTxnIds[i] >= 0)
                    txnIds[remapTxnIds[i]] = this.txnIds[i];
            }
        }

        int[] keysToTxnId = this.keyToTxnId.clone();
        int k = 0, i = keys.size(), o = i;
        while (i < keysToTxnId.length)
        {
            if (keysToTxnId[k] == i)
                keysToTxnId[k++] = o;

            int remapped = remapTxnIds[keysToTxnId[i]];
            if (remapped >= 0)
                keysToTxnId[o++] = remapped;
            ++i;
        }
        keysToTxnId = Arrays.copyOf(keysToTxnId, o);

        // TODO: trim keys?
        return new Dependencies(keys, txnIds, keysToTxnId);
    }

    public boolean contains(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId) >= 0;
    }

    public boolean isEmpty()
    {
        return keys.isEmpty();
    }

    public Keys someKeys(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            return Keys.EMPTY;

        ensureTxnIdToKey();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdToKey[txnIdIndex - 1];
        int end = txnIdToKey[txnIdIndex];
        Key[] result = new Key[end - start];
        for (int i = start ; i < end ; ++i)
            result[i - start] = keys.get(txnIdToKey[i]);
        return new Keys(result);
    }

    private void ensureTxnIdToKey()
    {
        if (txnIdToKey != null)
            return;

        int[] src = keyToTxnId;
        int[] trg = txnIdToKey = new int[txnIds.length - keys.size() + keyToTxnId.length];

        // first pass, count number of txnId per key
        for (int i = keys.size() ; i < src.length ; ++i)
            trg[src[i]]++;

        // turn into offsets (i.e. add txnIds.size() and then sum them)
        trg[0] += txnIds.length;
        for (int i = 1; i < txnIds.length ; ++i)
            trg[i] += trg[i - 1];

        // shuffle forwards one, so we have the start index rather than end
        System.arraycopy(trg, 0, trg, 1, txnIds.length - 1);
        trg[0] = txnIds.length;

        int k = 0;
        for (int i = keys.size() ; i < src.length ; ++i)
        {
            if (i == keyToTxnId[k])
                ++k;

            trg[trg[src[i]]++] = k;
        }
    }

    public void forEachOn(KeyRanges ranges, Predicate<Key> include, BiConsumer<Key, TxnId> forEach)
    {
        keys.foldl(ranges, (index, key, value) -> {
            if (!include.test(key))
                return null;

            for (int t = index == 0 ? keys.size() : keyToTxnId[index - 1], end = keyToTxnId[index]; t < end ; ++t)
            {
                TxnId txnId = txnIds[keyToTxnId[t]];
                forEach.accept(key, txnId);
            }
            return null;
        }, null);
    }

    public void forEachOn(CommandStore commandStore, Timestamp executeAt, Consumer<TxnId> forEach)
    {
        KeyRanges ranges = commandStore.ranges().since(executeAt.epoch);
        if (ranges == null)
            return;

        // TODO: check inlining
        forEachOn(ranges, commandStore::hashIntersects, forEach);
    }

    public void forEachOn(KeyRanges ranges, Predicate<Key> include, Consumer<TxnId> forEach)
    {
        for (int offset = 0 ; offset < txnIds.length ; offset += 64)
        {
            long bitset = keys.foldl(ranges, (keyIndex, key, off, value) -> {
                if (!include.test(key))
                    return value;

                int index = keyIndex == 0 ? keys.size() : keyToTxnId[keyIndex - 1];
                int end = keyToTxnId[keyIndex];
                if (off > 0)
                {
                    // TODO: interpolation search probably great here
                    index = Arrays.binarySearch(keyToTxnId, index, end, (int)off);
                    if (index < 0)
                        index = -1 - index;
                }

                while (index < end)
                {
                    long next = keyToTxnId[index++] - off;
                    if (next >= 64)
                        break;
                    value |= 1L << next;
                }

                return value;
            }, offset, 0, 0xffffffffffffffffL);

            while (bitset != 0)
            {
                int i = Long.numberOfTrailingZeros(bitset);
                TxnId txnId = txnIds[offset + i];
                forEach.accept(txnId);
                bitset ^= Long.lowestOneBit(bitset);
            }
        }
    }

    public void forEach(Key key, Consumer<TxnId> forEach)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return;

        int index = keyIndex == 0 ? keys.size() : keyToTxnId[keyIndex - 1];
        int end = keyToTxnId[keyIndex];
        while (index < end)
            forEach.accept(txnIds[keyToTxnId[index++]]);
    }

    public Keys keys()
    {
        return keys;
    }

    public int txnIdCount()
    {
        return txnIds.length;
    }

    public TxnId txnId(int i)
    {
        return txnIds[i];
    }

    @Override
    public Iterator<Map.Entry<Key, TxnId>> iterator()
    {
        return new Iterator<>()
        {
            int i = keys.size(), k = 0;

            @Override
            public boolean hasNext()
            {
                return i < keyToTxnId.length;
            }

            @Override
            public Map.Entry<Key, TxnId> next()
            {
                Entry result = new Entry(keys.get(k), txnIds[keyToTxnId[i++]]);
                if (i == keyToTxnId[k])
                    ++k;
                return result;
            }
        };
    }

    @Override
    public String toString()
    {
        if (keys.isEmpty())
            return "{}";

        StringBuilder builder = new StringBuilder("{");
        for (int k = 0, t = keys.size(); k < keys.size() ; ++k)
        {
            if (builder.length() > 1)
                builder.append(", ");

            builder.append(keys.get(k));
            builder.append(":[");
            boolean first = true;
            while (t < keyToTxnId[k])
            {
                if (first) first = false;
                else builder.append(", ");
                builder.append(txnIds[keyToTxnId[t++]]);
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dependencies that = (Dependencies) o;
        return txnIds.equals(that.txnIds)
               && keys.equals(that.keys)
               && Arrays.equals(keyToTxnId, that.keyToTxnId);
    }

}
