package accord.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Objects;

import static accord.utils.Property.qt;

class SortedArraysTest
{
    @Test
    public void testRemapper()
    {
        qt().forAll(remapperLongs()).check(p -> {
            Long[] src = p.src;
            Long[] trg = p.trg;
            // use false so null is never returned
            int[] result = SortedArrays.remapper(src, trg, false);
            for (int i = 0; i < result.length; i++)
                assertRemapperProperty(i, src, trg, result);
        });
    }

    private static void assertRemapperProperty(int i, Long[] src, Long[] trg, int[] result)
    {
        if (!(result[i] == -1 || Objects.equals(src[i], trg[result[i]])))
        {
            StringBuilder sb = new StringBuilder();
            sb.append("i = ").append(i).append('\n');
            sb.append(src[i]).append(" != ").append(trg[result[i]]).append('\n');
            sb.append("src:\n").append(Arrays.toString(src)).append('\n');
            sb.append("trg:\n").append(Arrays.toString(trg)).append('\n');
            String str = Arrays.toString(result);
            sb.append("result:\n").append(str).append('\n');
            for (int j = 0, idx = indexOfNth(str, ",", i); j <= idx; j++)
                sb.append(' ');
            sb.append('^');
            throw new AssertionError(sb.toString());
        }
    }

    private static int indexOfNth(String original, String search, int n)
    {
        String str = original;
        for (int i = 0; i < n; i++)
        {
            int idx = str.indexOf(search);
            if (idx < 0)
                throw new AssertionError("Unable to find next " + search);
            str = str.substring(idx + 1);
        }
        return original.length() - str.length();
    }

    private static Gen<Pair<Long>> remapperLongs()
    {
        return remapper(longs());
    }

    private static <T extends Comparable<T>> Gen<Pair<T>> remapper(Gen<T[]> gen)
    {
        return random -> {
            T[] src = gen.next(random);
            Arrays.sort(src);
            int to = random.nextInt(0, src.length);
            int offset = random.nextInt(0, to);
            T[] trg = Arrays.copyOfRange(src, offset, to);
            return new Pair<>(src, trg);
        };
    }

    private static Gen<Long[]> longs()
    {
        return random -> {
            int size = random.nextInt(99) + 1;
            Long[] array = new Long[size];
            for (int i = 0; i < size; i++)
                array[i] = random.nextLong();
            return array;
        };
    }

    private static class Pair<T>
    {
        private final T[] src;
        private final T[] trg;

        private Pair(T[] src, T[] trg) {
            this.src = src;
            this.trg = trg;
        }
    }
}