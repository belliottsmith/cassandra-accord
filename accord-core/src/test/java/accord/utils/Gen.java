package accord.utils;

import java.util.function.Function;
import java.util.function.Predicate;

public interface Gen<A> {
    /**
     * For cases where method handles isn't able to detect the proper type, this method acts as a cast
     * to inform the compiler of the desired type.
     */
    static <A> Gen<A> of(Gen<A> fn)
    {
        return fn;
    }

    A next(Random random);

    default <B> Gen<B> map(Function<A, B> fn)
    {
        return r -> fn.apply(this.next(r));
    }

    default Gen<A> filter(Predicate<A> fn)
    {
        Gen<A> self = this;
        return r -> {
            A value;
            do {
                value = self.next(r);
            }
            while (!fn.test(value));
            return value;
        };
    }

    class Random extends java.util.Random
    {
        public Random(long seed) {
            super(seed);
        }

        public int nextInt(int lower, int upper)
        {
            return nextInt(upper - lower + 1) + lower;
        }

        public int allPositive()
        {
            return nextInt(1, Integer.MAX_VALUE);
        }

        public int nextPositive(int upper)
        {
            return nextInt(1, upper);
        }
    }
}
