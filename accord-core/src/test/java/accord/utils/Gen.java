package accord.utils;

import java.util.function.Function;

public interface Gen<A> {
    A next(Random random);

    default <B> Gen<B> map(Function<A, B> fn)
    {
        return r -> fn.apply(this.next(r));
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
