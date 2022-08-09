package accord.utils;

public interface Gen<T> {
    T next(Random random);

    class Random extends java.util.Random
    {
        public Random(long seed) {
            super(seed);
        }

        public int nextInt(int lower, int upperExclusive)
        {
            return nextInt(upperExclusive - lower + 1) + lower;
        }
    }
}
