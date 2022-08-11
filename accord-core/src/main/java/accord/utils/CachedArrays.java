package accord.utils;

import accord.api.Key;
import accord.primitives.TxnId;

import java.util.Arrays;
import java.util.function.IntFunction;

public class CachedArrays
{
    private static final int MAX_CACHED_ARRAY_COUNT = 64; // means 2MiB total
    private static final int[] NO_INTS = new int[0];
    private static final TxnId[] NO_TXNIDS = new TxnId[0];
    private static final Key[] NO_KEYS = new Key[0];

    private static final IntArrayCache INTS = new IntArrayCache();
    private static final SharedObjectArrayCache OBJECTS = new SharedObjectArrayCache();

    public static final IntFunction<int[]> ALLOCATE_INTS = size -> size == 0 ? NO_INTS : new int[size];
    public static final IntFunction<TxnId[]> ALLOCATE_TXNIDS = size -> size == 0 ? NO_TXNIDS : new TxnId[size];
    public static final IntFunction<Key[]> ALLOCATE_KEYS = size -> size == 0 ? NO_KEYS : new Key[size];

    public static IntArrayCache ints()
    {
        return INTS;
    }
    public static SharedObjectArrayCache objects()
    {
        return OBJECTS;
    }

    // TODO: this is much too simple, need to make smarter before prod, with some thread local caching, local and global size limits
    public static class IntArrayCache implements SortedArrays.IntBufferManager
    {
        private final int[] sizes = new int[14];
        private final int[][][] caches = new int[14][MAX_CACHED_ARRAY_COUNT][];

        @Override
        public int[] complete(int[] buffer, int size)
        {
            if (size == buffer.length && Integer.bitCount(size) != 1)
                return buffer;

            return Arrays.copyOf(buffer, size);
        }

        @Override
        public synchronized void discard(int[] buffer)
        {
            if (Integer.bitCount(buffer.length) != 1)
                return;

            int log2 = 31 - Integer.numberOfLeadingZeros(buffer.length);
            for (int i = 0; i < caches[log2].length ; ++i)
            {
                if (caches[log2][i] == null)
                {
                    caches[log2][i] = buffer;
                    break;
                }
            }
        }

        @Override
        public synchronized int[] allocateInts(int minSize)
        {
            if (minSize == 0)
                return NO_INTS;

            int log2 = 32 - Integer.numberOfLeadingZeros(minSize - 1);
            if (log2 > sizes.length)
                return new int[minSize];

            if (sizes[log2] < MAX_CACHED_ARRAY_COUNT)
            {
                sizes[log2]++;
                return new int[1 << log2];
            }

            for (int i = 0; i < caches[log2].length ; ++i)
            {
                if (caches[log2][i] != null)
                {
                    int[] result = caches[log2][i];
                    caches[log2][i] = null;
                    return result;
                }
            }

            return new int[minSize];
        }
    }

    public static class SharedObjectArrayCache implements SortedArrays.BufferManager
    {
        private final int[] sizes = new int[14];
        private final Object[][][] caches;

        private SharedObjectArrayCache()
        {
            this.caches = new Object[14][MAX_CACHED_ARRAY_COUNT][];
        }

        public <T> T[] complete(IntFunction<T[]> allocator, T[] buffer, int size)
        {
            if (size == buffer.length && (Integer.bitCount(size) != 1 || buffer.getClass() != Object[].class))
                return buffer;

            T[] result = allocator.apply(size);
            System.arraycopy(buffer, 0, result, 0, size);
            return result;
        }

        public int lengthOfLast(Object[] left, int leftLength, Object[] right, int rightLength, Object[] buffer)
        {
            return buffer.length;
        }

        public synchronized void discard(Object[] buffer)
        {
            if (Integer.bitCount(buffer.length) != 1 || buffer.getClass() != Object[].class)
                return;

            int log2 = 31 - Integer.numberOfLeadingZeros(buffer.length);
            for (int i = 0; i < caches[log2].length ; ++i)
            {
                if (caches[log2][i] == null)
                {
                    caches[log2][i] = buffer;
                    break;
                }
            }
        }

        @Override
        public <T> T[] get(IntFunction<T[]> allocator, int minSize)
        {
            if (minSize == 0)
                return allocator.apply(0);

            int log2 = 32 - Integer.numberOfLeadingZeros(minSize - 1);
            if (log2 > sizes.length)
                return allocator.apply(minSize);

            if (sizes[log2] < MAX_CACHED_ARRAY_COUNT)
            {
                sizes[log2]++;
                return allocator.apply(1 << log2);
            }

            for (int i = 0; i < caches[log2].length ; ++i)
            {
                if (caches[log2][i] != null)
                {
                    T[] result = (T[]) caches[log2][i];
                    caches[log2][i] = null;
                    return result;
                }
            }

            return allocator.apply(minSize);
        }
    }

    public static class SavingManager implements SortedArrays.BufferManager, SortedArrays.IntBufferManager
    {
        final SortedArrays.BufferManager objs;
        final SortedArrays.IntBufferManager ints;
        int length;

        public SavingManager(SortedArrays.BufferManager objs, SortedArrays.IntBufferManager ints)
        {
            this.objs = objs;
            this.ints = ints;
        }

        @Override
        public <T> T[] get(IntFunction<T[]> allocator, int minSize)
        {
            length = -1;
            return objs.get(allocator, minSize);
        }

        @Override
        public <T> T[] complete(IntFunction<T[]> allocator, T[] buffer, int size)
        {
            length = size;
            return buffer;
        }

        @Override
        public void discard(Object[] buffer)
        {
        }

        public int lengthOfLast(Object[] left, int leftLength, Object[] right, int rightLength, Object[] buffer)
        {
            if (left == buffer) return leftLength;
            else if (right == buffer) return rightLength;
            else return length;
        }

        @Override
        public int[] allocateInts(int minSize)
        {
            return ints.allocateInts(minSize);
        }

        @Override
        public int[] complete(int[] buffer, int size)
        {
            return buffer;
        }

        @Override
        public void discard(int[] buffer)
        {
        }
    }

}
