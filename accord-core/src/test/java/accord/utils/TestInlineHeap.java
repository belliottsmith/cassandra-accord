package accord.utils;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInlineHeap
{
    @Test
    public void testSmall()
    {
        int streams = 2;
        int range = 50;
        int minStreamLength = 2;
        int maxStreamLength = 10;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testMedium()
    {
        int streams = 16;
        int range = 200;
        int minStreamLength = 10;
        int maxStreamLength = 100;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testMany()
    {
        int streams = 32;
        int range = 400;
        int minStreamLength = 50;
        int maxStreamLength = 100;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    @Test
    public void testSparse()
    {
        int streams = 32;
        int range = 2000;
        int minStreamLength = 50;
        int maxStreamLength = 100;
        test(ThreadLocalRandom.current().nextLong(), streams, range, minStreamLength, maxStreamLength);
    }

    public static void test(long seed, int streams, int range, int minStreamLength, int maxStreamLength)
    {
        Random random = new Random();
        random.setSeed(seed);

        List<Map.Entry<Integer, Integer>> canonical = new ArrayList<>();

        int[] heap = InlineHeap.create(streams);
        int[][] keys = new int[streams][];
        int[] indexes = new int[streams];
        for (int i = 0 ; i < streams ; ++i)
        {
            BitSet used = new BitSet(range);
            int count = minStreamLength + random.nextInt(maxStreamLength - minStreamLength);
            keys[i] = new int[count];
            while (--count >= 0)
            {
                int key = random.nextInt(range);
                while (used.get(key))
                    key = random.nextInt(range);

                used.set(key);
                keys[i][count] = key;
                canonical.add(new SimpleEntry<>(key, i));
            }

            Arrays.sort(keys[i]);

            InlineHeap.set(heap, i, keys[i][0], i);
        }

        canonical.sort((a, b) -> {
            int c = Integer.compare(a.getKey(), b.getKey());
            if (c == 0) c = Integer.compare(a.getValue(), b.getValue());
            return c;
        });
        int count = 0;
        List<Map.Entry<Integer, Integer>> tmp = new ArrayList<>();
        int heapSize = InlineHeap.heapify(heap, streams);
        while (heapSize > 0)
        {
            tmp.clear();
            InlineHeap.consume(heap, heapSize, (key, stream, v) -> {
                tmp.add(new SimpleEntry<>(key, stream));
                return v;
            }, 0);
            tmp.sort(Entry.comparingByValue());
            for (int i = 0 ; i < tmp.size() ; i++)
                Assertions.assertEquals(canonical.get(count + i), tmp.get(i), "Seed: " + seed);
            count += tmp.size();
            heapSize = InlineHeap.advance(heap, heapSize, stream -> {
                return ++indexes[stream] == keys[stream].length ? Integer.MIN_VALUE : keys[stream][indexes[stream]];
            });
            InlineHeap.validate(heap, heapSize);
        }

        Assertions.assertEquals(canonical.size(), count, "Seed: " + seed);
    }
}
