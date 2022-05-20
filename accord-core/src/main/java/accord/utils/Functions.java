package accord.utils;

import java.util.function.BiFunction;

public class Functions
{

    public static <T> T notNullOrMerge(T a, T b, BiFunction<T, T, T> merge)
    {
        return a == null ? b : b == null ? a : merge.apply(a, b);
    }

}
