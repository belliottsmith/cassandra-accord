package accord.utils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Gens {
    private Gens() {}

    public static Gen<Gen.Random> random()
    {
        return r -> r;
    }

    public static <T> ListDSL<T> lists(Gen<T> fn)
    {
        return new ListDSL<>(fn);
    }

    public static class ListDSL<T>
    {
        private final Gen<T> fn;

        public ListDSL(Gen<T> fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        public Gen<List<T>> ofSize(int size)
        {
            return ofSizeBetween(size, size);
        }

        public Gen<List<T>> ofSizeBetween(int minSize, int maxSize)
        {
            Preconditions.checkArgument(minSize >= 0);
            Preconditions.checkArgument(maxSize >= minSize);
            return r -> {
                int size = r.nextInt(minSize, maxSize);
                List<T> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    list.add(fn.next(r));
                return list;
            };
        }
    }
}
