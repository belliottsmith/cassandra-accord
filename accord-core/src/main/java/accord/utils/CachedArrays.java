package accord.utils;

import java.util.Arrays;

public class CachedArrays
{
    private static final int MAX_CACHED_ARRAY_COUNT = 64; // means 2MiB total
    private static final int[] NO_INTS = new int[0];

    private static final IntArrayCache INTS = new IntArrayCache();

    public static IntArrayCache ints()
    {
        return INTS;
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
        public synchronized int[] get(int minSize)
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

}
