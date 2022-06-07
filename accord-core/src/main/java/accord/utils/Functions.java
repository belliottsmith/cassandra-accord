package accord.utils;

import java.util.function.BiFunction;

public class Functions
{

    public static <T> T reduceNonNull(T a, T b, BiFunction<T, T, T> merge)
    {
        return a == null ? b : b == null ? a : merge.apply(a, b);
    }

    public static <T> T reduceNonNull(BiFunction<T, T, T> merge, T a, T ... bs)
    {
        for (T b : bs)
        {
            if (b != null)
            {
                if (a == null) a = b;
                else a = merge.apply(a, b);
            }
        }
        return a;
    }

}
