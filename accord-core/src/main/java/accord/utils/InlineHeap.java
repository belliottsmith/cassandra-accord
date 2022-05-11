package accord.utils;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import org.apache.cassandra.utils.concurrent.Inline;

public class InlineHeap
{
    public interface IntHeapFold
    {
        int apply(int key, int stream, int v);
    }

    /**
     * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
     */
    static final int SORTED_SECTION_SIZE = 4;

    // returns the new size of the heap
    public static int heapify(int[] heap, int size)
    {
        for (int i = size - 1; i >= 0; --i)
            size = replaceAndSink(heap, key(heap, i), stream(heap, i), i, size);
        return size;
    }

    @Inline
    public static int advance(int[] heap, int size, IntUnaryOperator next)
    {
        for (int i = maxConsumed(heap, size) - 1; i >= 0; --i)
        {
            int key = key(heap, i);
            if (key == Integer.MIN_VALUE)
            {
                int stream = stream(heap, i);
                key = next.applyAsInt(stream);
                size = replaceAndSink(heap, key, stream, i, size);
            }
        }
        return size;
    }

    // yield the number of items to consume from the head of the heap (i.e. indexes heap[0..consume(heap)])
    @Inline
    public static int consume(int[] heap, int size, IntHeapFold consumer, int v)
    {
        int key = key(heap, 0);
        int i = 0;
        int sortedSize = Math.min(size, SORTED_SECTION_SIZE);
        do
        {
            v = consumer.apply(key, stream(heap, i), v);
            clearKey(heap, i);
            i++;
        } while (i < sortedSize && key(heap, i) == key);

        if (i == SORTED_SECTION_SIZE && i < size)
        {
            boolean descend;
            int depthSize = 2;
            do
            {
                descend = false;
                for (int j = 0 ; j < depthSize ; ++j)
                {
                    if (key(heap, i + j) == key)
                    {
                        descend = true;
                        v = consumer.apply(key, stream(heap, i + j), v);
                        clearKey(heap, i + j);
                    }
                }
                i += depthSize;
                depthSize = Math.min(depthSize * 2, size - i);
            } while (i < size && descend);
        }
        return v;
    }

    private static int maxConsumed(int[] heap, int size)
    {
        int i = 1;
        clearKey(heap, 0);
        int sortedSize = Math.min(size, SORTED_SECTION_SIZE);
        while (i < sortedSize && key(heap, i) == Integer.MIN_VALUE)
            i++;

        int result = i;
        if (i == SORTED_SECTION_SIZE && i < size)
        {
            int depthSize = 2;
            loop: while (i < size)
            {
                for (int j = depthSize - 1 ; j >= 0 ; --j)
                {
                    if (key(heap, i + j) == Integer.MIN_VALUE)
                    {
                        result = i + j;
                        i += depthSize;
                        depthSize = Math.min(depthSize * 2, size - i);
                        continue loop;
                    }
                }
                break;
            }
        }
        return result;
    }

    /**
     * Replace an iterator in the heap with the given position and move it down the heap until it finds its proper
     * position, pulling lighter elements up the heap.
     *
     * Whenever an equality is found between two elements that form a new parent-child relationship, the child's
     * equalParent flag is set to true if the elements are equal.
     */
    private static int replaceAndSink(int[] heap, int key, int stream, int currIdx, int size)
    {
        int headIndex = currIdx;
        if (key == Integer.MIN_VALUE)
        {
            // Drop iterator by replacing it with the last one in the heap.
            key = key(heap, --size);
            stream = stream(heap, size);
            clear(heap, size);
        }

        final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

        int nextIdx;

        // Advance within the sorted section, pulling up items lighter than candidate.
        while ((nextIdx = currIdx + 1) <= sortedSectionSize)
        {
            if (!equalParent(heap, headIndex, nextIdx)) // if we were greater then an (or were the) equal parent, we are >= the child
            {
                int cmp = Integer.compare(key, key(heap, nextIdx));
                if (cmp <= 0)
                {
                    set(heap, currIdx, key, stream);
                    return size;
                }
            }

            copy(heap, nextIdx, currIdx);
            currIdx = nextIdx;
        }
        // If size <= SORTED_SECTION_SIZE, nextIdx below will be no less than size,
        // because currIdx == sortedSectionSize == size - 1 and nextIdx becomes
        // (size - 1) * 2) - (size - 1 - 1) == size.

        // Advance in the binary heap, pulling up the lighter element from the two at each level.
        while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
        {
            if (!equalParent(heap, headIndex, nextIdx))
            {
                if (!equalParent(heap, headIndex, nextIdx + 1))
                {
                    // pick the smallest of the two children
                    int siblingCmp = Integer.compare(key(heap, nextIdx + 1), key(heap, nextIdx));
                    if (siblingCmp < 0)
                        ++nextIdx;

                    // if we're smaller than this, we are done, and must only restore the heap and equalParent properties
                    int cmp = Integer.compare(key, key(heap, nextIdx));
                    if (cmp <= 0)
                    {
                        set(heap, currIdx, key, stream);
                        return size;
                    }
                }
                else
                    ++nextIdx;  // descend down the path where we found the equal child
            }

            copy(heap, nextIdx, currIdx);
            currIdx = nextIdx;
        }

        // our loop guard ensures there are always two siblings to process; typically when we exit the loop we will
        // be well past the end of the heap and this next condition will match...
        if (nextIdx >= size)
        {
            set(heap, currIdx, key, stream);
            return size;
        }

        // ... but sometimes we will have one last child to compare against, that has no siblings
        if (!equalParent(heap, headIndex, nextIdx))
        {
            int cmp = Integer.compare(key, key(heap, nextIdx));
            if (cmp <= 0)
            {
                set(heap, currIdx, key, stream);
                return size;
            }
        }

        copy(heap, nextIdx, currIdx);
        set(heap, nextIdx, key, stream);
        return size;
    }

    static boolean equalParent(int[] heap, int headIndex, int index)
    {
        int parentIndex = parentIndex(index);
        if (parentIndex <= headIndex)
            return false;
        return key(heap, parentIndex) == key(heap, index);
    }

    public static void set(int[] heap, int index, int key, int stream)
    {
        heap[index * 2] = key;
        heap[index * 2 + 1] = stream;
    }

    public static void clearKey(int[] heap, int index)
    {
        heap[index * 2] = Integer.MIN_VALUE;
    }

    static void copy(int[] heap, int src, int trg)
    {
        heap[trg * 2] = heap[src * 2];
        heap[trg * 2 + 1] = heap[src * 2 + 1];
    }

    public static int key(int[] heap, int index)
    {
        return heap[index * 2];
    }

    public static int stream(int[] heap, int index)
    {
        return heap[index * 2 + 1];
    }

    static void clear(int[] heap, int index)
    {
        Arrays.fill(heap, index * 2, (index + 1) * 2, Integer.MIN_VALUE);
    }

    private static int parentIndex(int index)
    {
        if (index <= SORTED_SECTION_SIZE)
            return index - 1;
        return ((index - (SORTED_SECTION_SIZE + 1)) / 2) + SORTED_SECTION_SIZE;
    }

    public static int[] create(int size)
    {
        return new int[size * 2];
    }

    public static void validate(int[] heap, int size)
    {
        for (int i = 1 ; i < size ; i++)
        {
            if (key(heap, i) < key(heap, parentIndex(i)))
                throw new AssertionError();
        }
    }
}
