package accord.utils;

import java.util.Arrays;
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
        T[] noOp = left.length >= right.length ? left : right;
        T[] result = noOp;
        int resultSize = 0;

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
                if (result == noOp)
                    continue;
                minKey = leftKey;
            }
            else if (cmp < 0)
            {
                leftIdx++;
                if (result == left)
                    continue;
                minKey = leftKey;
                if (result == noOp)
                {
                    resultSize = rightIdx;
                    result = allocate.apply(resultSize + (left.length - (leftIdx - 1)) + (right.length - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                }
            }
            else
            {
                rightIdx++;
                if (result == right)
                    continue;
                minKey = rightKey;
                if (result == noOp)
                {
                    resultSize = leftIdx;
                    result = allocate.apply(resultSize + (left.length - leftIdx) + (right.length - (rightIdx - 1)));
                    System.arraycopy(left, 0, result, 0, resultSize);
                }
            }
            result[resultSize++] = minKey;
        }

        if (result == noOp)
        {
            if (noOp == left && rightIdx == right.length)
                return left;
            if (noOp == right && leftIdx == left.length)
                return right;

            resultSize = noOp == left ? leftIdx : rightIdx;
            result = allocate.apply(resultSize + (left.length - leftIdx) + (right.length - rightIdx));
            System.arraycopy(noOp, 0, result, 0, resultSize);
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
    public static <T extends Comparable<T>> T[] linearIntersection(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int leftIdx = 0;
        int rightIdx = 0;
        T[] noOp = left.length <= right.length ? left : right;
        T[] result = noOp;
        int resultSize = 0;

        while (leftIdx < left.length && rightIdx < right.length)
        {
            T leftKey = left[leftIdx];
            T rightKey = right[rightIdx];
            int cmp = leftKey.compareTo(rightKey);
            if (cmp == 0)
            {
                leftIdx++;
                rightIdx++;
                if (result != noOp)
                    result[resultSize] = leftKey;
                resultSize++;
            }
            else
            {
                if (cmp < 0) leftIdx++;
                else rightIdx++;
                if (result == noOp)
                {
                    result = allocate.apply(resultSize + Math.min(left.length - leftIdx, right.length - rightIdx));
                    System.arraycopy(noOp, 0, result, 0, resultSize);
                }
            }
        }

        if (result == noOp && resultSize == noOp.length)
            return noOp;

        if (resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);
        
        return result;
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

    public static <T extends Comparable<? super T>> int[] remapper(T[] src, T[] trg)
    {
        return remapper(src, src.length, trg, trg.length);
    }

    /**
     * Given two sorted arrays, where one is a subset of the other, return an int[] of the same size as
     * the {@code src} parameter, with the index within {@code trg} of the corresponding element within {@code src},
     * or -1 otherwise.
     * That is, {@code src[i] == -1 || src[i].equals(trg[result[i]])}
     */
    public static <T extends Comparable<? super T>> int[] remapper(T[] src, int srcLength, T[] trg, int trgLength)
    {
        if (src == trg) return null;
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
