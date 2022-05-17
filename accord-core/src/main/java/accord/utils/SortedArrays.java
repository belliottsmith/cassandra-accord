package accord.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntFunction;

import static java.util.Arrays.*;

public class SortedArrays
{
    /**
     * Given two sorted arrays, return an array containing the elements present in either, preferentially returning one
     * of the inputs if it contains all elements of the other.
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T[] result = null;
        int resultSize = 0;

        // first, pick the superset candidate and merge the two until we find the first missing item
        // if none found, return the superset candidate
        if (left.length >= right.length)
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = leftIdx;
                    result = allocate.apply(resultSize + (left.length - leftIdx) + (right.length - (rightIdx - 1)));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    result[resultSize++] = right[rightIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (rightIdx == right.length)
                    return left;
                result = allocate.apply(resultSize + (left.length - leftIdx) + (right.length - rightIdx));
            }
        }
        else
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = rightIdx;
                    result = allocate.apply(resultSize + (left.length - (leftIdx - 1)) + (right.length - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    result[resultSize++] = left[leftIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (leftIdx == left.length)
                    return right;
                result = allocate.apply(resultSize + (left.length - leftIdx) + (right.length - rightIdx));
            }
        }

        while (leftIdx < left.length && rightIdx < right.length)
        {
            T leftKey = left[leftIdx];
            T rightKey = right[rightIdx];
            int cmp = leftKey.compareTo(rightKey);
            T minKey;
            if (cmp == 0)
            {
                leftIdx++;
                rightIdx++;
                minKey = leftKey;
            }
            else if (cmp < 0)
            {
                leftIdx++;
                minKey = leftKey;
            }
            else
            {
                rightIdx++;
                minKey = rightKey;
            }
            result[resultSize++] = minKey;
        }

        while (leftIdx < left.length)
            result[resultSize++] = left[leftIdx++];

        while (rightIdx < right.length)
            result[resultSize++] = right[rightIdx++];

        if (resultSize < result.length)
            result = copyOf(result, resultSize);

        return result;
    }

    /**
     * Given two sorted arrays, return the elements present only in both, preferentially returning one of the inputs if
     * it contains no elements not present in the other.
     */
    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T[] result = null;
        int resultSize = 0;

        // first pick a subset candidate, and merge both until we encounter an element not present in the other array
        if (left.length <= right.length)
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = leftIdx++;
                    result = allocate.apply(resultSize + Math.min(left.length - leftIdx, right.length - rightIdx));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return left;
        }
        else
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = rightIdx++;
                    result = allocate.apply(resultSize + Math.min(left.length - leftIdx, right.length - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return right;
        }

        while (leftIdx < left.length && rightIdx < right.length)
        {
            T leftKey = left[leftIdx];
            int cmp = leftKey.compareTo(right[rightIdx]);
            if (cmp == 0)
            {
                leftIdx++;
                rightIdx++;
                result[resultSize++] = leftKey;
            }
            else
            {
                if (cmp < 0) leftIdx++;
                else rightIdx++;
            }
        }

        if (resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);
        
        return result;
    }

    /**
     * Given two sorted arrays, return the elements present only in the first, preferentially returning the first array
     * itself if possible
     */
    public static <T extends Comparable<? super T>> T[] linearDifference(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int rightIdx = 0;
        int leftIdx = 0;

        T[] result = null;
        int resultSize = 0;

        while (leftIdx < left.length && rightIdx < right.length)
        {
            int cmp = left[leftIdx].compareTo(right[rightIdx]);
            if (cmp == 0)
            {
                resultSize = leftIdx++;
                ++rightIdx;
                result = allocate.apply(resultSize + left.length - leftIdx);
                System.arraycopy(left, 0, result, 0, resultSize);
                break;
            }
            else if (cmp < 0)
            {
                ++leftIdx;
            }
            else
            {
                ++rightIdx;
            }
        }

        if (result == null)
            return left;

        while (leftIdx < left.length && rightIdx < right.length)
        {
            int cmp = left[leftIdx].compareTo(right[rightIdx]);
            if (cmp > 0)
            {
                result[resultSize++] = left[leftIdx++];
            }
            else if (cmp < 0)
            {
                ++rightIdx;
            }
            else
            {
                ++leftIdx;
                ++rightIdx;
            }
        }

        if (resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);

        return result;
    }

    public static <T extends Comparable<? super T>> T[] insert(T[] src, T item, IntFunction<T[]> factory)
    {
        int insertPos = Arrays.binarySearch(src, item);
        if (insertPos >= 0)
            return src;
        insertPos = -1 - insertPos;

        T[] trg = factory.apply(src.length + 1);
        System.arraycopy(src, 0, trg, 0, insertPos);
        trg[insertPos] = item;
        System.arraycopy(src, insertPos, trg, insertPos + 1, src.length - insertPos);
        return trg;
    }

    /**
     * Equivalent to {@link Arrays#binarySearch}, only more efficient algorithmically for linear merges.
     * Binary search has worst case complexity {@code O(n.lg n)} for a linear merge, whereas exponential search
     * has a worst case of {@code O(n)}. However compared to a simple linear merge, the best case for exponential
     * search is {@code O(lg(n))} instead of {@code O(n)}.
     */
    public static <T1, T2 extends Comparable<? super T1>> int exponentialSearch(T1[] in, int from, int to, T2 find)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = find.compareTo(in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c == 0)
                return i;
            from = i + 1;
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return Arrays.binarySearch(in, from, to, find);
    }

    public static <T> int exponentialSearch(T[] in, int from, int to, T find, Comparator<T> comparator)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = comparator.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c == 0)
                return i;
            from = i + 1;
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return Arrays.binarySearch(in, from, to, find, comparator);
    }

    public static <T1, T2 extends Comparable<? super T1>> int exponentialSearchCeil(T2[] in, int from, int to, T1 find)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = in[i].compareTo(find);
            if (c >= 0)
            {
                if (c == 0 && step == 0)
                    return from;
                to = i;
                break;
            }
            from = i + 1;
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return binarySearchCeil(in, from, to, find);
    }

    public static <T1, T2 extends Comparable<? super T1>> int binarySearchCeil(T2[] in, int from, int to, T1 find)
    {
        boolean found = false;
        while (from < to)
        {
            int m = (from + to) >>> 1;
            int c = in[m].compareTo(find);
            if (c >= 0)
            {
                to = m;
                found = c == 0;
            }
            else from = m + 1;
        }
        return found ? to : -1 - to;
    }

    public static <T1, T2 extends Comparable<? super T1>> int exponentialSearchCeil2(T1[] in, int from, int to, T2 find)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = find.compareTo(in[i]);
            if (c <= 0)
            {
                if (c == 0 && step == 0)
                    return from;
                to = i;
                break;
            }
            from = i + 1;
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return binarySearchCeil2(in, from, to, find);
    }

    public static <T1, T2 extends Comparable<? super T1>> int binarySearchCeil2(T1[] in, int from, int to, T2 find)
    {
        boolean found = false;
        while (from < to)
        {
            int m = (from + to) >>> 1;
            int c = find.compareTo(in[m]);
            if (c <= 0)
            {
                to = m;
                found = c == 0;
            }
            else from = m + 1;
        }
        return found ? to : -1 - to;
    }

    public static <T1, T2 extends Comparable<? super T1>> long findNextIntersectionWithOverlaps(T1[] as, int ai, T2[] bs, int bi)
    {
        if (ai == as.length)
            return -1;

        while (true)
        {
            bi = SortedArrays.exponentialSearchCeil(bs, bi, bs.length, as[ai]);
            if (bi >= 0)
                break;

            bi = -1 - bi;
            if (bi == bs.length)
                return -1;

            ai = SortedArrays.exponentialSearchCeil2(as, ai, as.length, bs[bi]);
            if (ai >= 0)
                break;

            ai = -1 -ai;
            if (ai == as.length)
                return -1;
        }
        return ((long)ai << 32) | bi;
    }

    public static <T1 extends Comparable<? super T1>> long findNextIntersection(T1[] as, int ai, T1[] bs, int bi)
    {
        if (ai == as.length)
            return -1;

        while (true)
        {
            bi = SortedArrays.exponentialSearch(bs, bi, bs.length, as[ai]);
            if (bi >= 0)
                break;

            bi = -1 - bi;
            if (bi == bs.length)
                return -1;

            ai = SortedArrays.exponentialSearch(as, ai, as.length, bs[bi]);
            if (ai >= 0)
                break;

            ai = -1 -ai;
            if (ai == as.length)
                return -1;
        }
        return ((long)ai << 32) | bi;
    }

    public static <T> long findNextIntersection(T[] as, int ai, T[] bs, int bi, Comparator<T> comparator)
    {
        if (ai == as.length)
            return -1;

        while (true)
        {
            bi = SortedArrays.exponentialSearch(bs, bi, bs.length, as[ai], comparator);
            if (bi >= 0)
                break;

            bi = -1 - bi;
            if (bi == bs.length)
                return -1;

            ai = SortedArrays.exponentialSearch(as, ai, as.length, bs[bi], comparator);
            if (ai >= 0)
                break;

            ai = -1 -ai;
            if (ai == as.length)
                return -1;
        }
        return ((long)ai << 32) | bi;
    }

    public static <T extends Comparable<? super T>> int[] remapper(T[] src, T[] trg, boolean trgIsKnownSuperset)
    {
        return remapper(src, src.length, trg, trg.length, trgIsKnownSuperset);
    }

    /**
     * Given two sorted arrays, where one is a subset of the other, return an int[] of the same size as
     * the {@code src} parameter, with the index within {@code trg} of the corresponding element within {@code src},
     * or -1 otherwise.
     * That is, {@code src[i] == -1 || src[i].equals(trg[result[i]])}
     */
    public static <T extends Comparable<? super T>> int[] remapper(T[] src, int srcLength, T[] trg, int trgLength, boolean trgIsSuperset)
    {
        if (src == trg || (trgIsSuperset && trgLength == srcLength)) return null;
        int[] result = new int[srcLength];
        for (int i = 0, j = 0 ; i < srcLength && j < trgLength ;)
        {
            int c = src[i].compareTo(trg[j]);
            if (c < 0) result[i++] = -1;
            else if (c > 0) ++j;
            else result[i++] = j++;
        }
        return result;
    }

    public static int remap(int i, int[] remapper)
    {
        return remapper == null ? i : remapper[i];
    }
}
