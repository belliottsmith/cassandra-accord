package accord.utils;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    @Test
    public void testSearch()
    {
        qt().forAll(uniqueInts()).check(array -> {
            Arrays.sort(array);
            Integer[] jint = IntStream.of(array).mapToObj(Integer::valueOf).toArray(Integer[]::new);
            for (int i = 0; i < array.length; i++)
            {
                int find = array[i];
                Assertions.assertEquals(i, SortedArrays.exponentialSearch(array, 0, array.length, find));
                for (SortedArrays.Search search : SortedArrays.Search.values())
                {
                    Assertions.assertEquals(i, SortedArrays.exponentialSearch(jint, 0, array.length, find, Integer::compare, search));
                    Assertions.assertEquals(i, SortedArrays.binarySearch(jint, 0, array.length, find, Integer::compare, search));
                }

                // uses Search.FAST
                long lr = SortedArrays.findNextIntersection(jint, i, jint, i);
                int left = (int) (lr >>> 32);
                int right = (int) lr;
                Assertions.assertEquals(left, right);
                Assertions.assertEquals(left, i);

                // uses Search.CEIL
                lr = SortedArrays.findNextIntersectionWithOverlaps(jint, i, jint, i, Integer::compare, Integer::compare);
                left = (int) (lr >>> 32);
                right = (int) lr;
                Assertions.assertEquals(left, right);
                Assertions.assertEquals(left, i);
            }
        });
    }

    @Test
    public void testLinearUnion()
    {
        Gen<Integer[]> gen = Gens.arrays(Integer.class, Gens.ints().all())
                .unique()
                .ofSizeBetween(0, 100)
                .map(a -> {
                    Arrays.sort(a);
                    return a;
                });
        qt().forAll(gen, gen).check((a, b) -> {
            Set<Integer> seen = new HashSet<>();
            Stream.of(a).forEach(seen::add);
            Stream.of(b).forEach(seen::add);
            Integer[] actual = SortedArrays.linearUnion(a, b, Integer[]::new);
            Integer[] expected = seen.toArray(Integer[]::new);
            Arrays.sort(expected);
            Assertions.assertArrayEquals(expected, actual);
        });
    }

    @Test
    public void testLinearIntersection()
    {
        Gen<Integer[]> gen = Gens.arrays(Integer.class, Gens.ints().all())
                .unique()
                .ofSizeBetween(0, 100)
                .map(a -> {
                    Arrays.sort(a);
                    return a;
                });
        qt().withSeed(3576953691942488024L).forAll(gen, gen).check((a, b) -> {
            Set<Integer> left = new HashSet<>(Arrays.asList(a));
            Set<Integer> right = new HashSet<>(Arrays.asList(b));
            Set<Integer> intersection = Sets.intersection(left, right);
            Integer[] expected = intersection.toArray(Integer[]::new);
            Arrays.sort(expected);

            Integer[] actual = SortedArrays.linearIntersection(a, b, Integer[]::new);
            Assertions.assertArrayEquals(expected, actual);
        });
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
            int offset = to == 0 ? 0 : random.nextInt(0, to);
            T[] trg = Arrays.copyOfRange(src, offset, to);
            return new Pair<>(src, trg);
        };
    }

    private static Gen<Long[]> longs()
    {
        // need Long rather than long
        return Gens.arrays(Long.class, Gens.longs().all()).ofSizeBetween(1, 100);
    }

    private static Gen<int[]> uniqueInts()
    {
        return Gens.arrays(Gens.ints().all()).unique().ofSizeBetween(1, 100);
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