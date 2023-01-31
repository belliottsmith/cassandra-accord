package accord.burn.random;

import java.util.Random;

public interface Decision
{
    boolean get(Random randomSource);

    public static void main(String[] args) {
//        Random rs = new Random(0);
        Random rs = new Random();
        //TODO ranges < 16384 always returns 1...
        Decision desc = new RandomWalkPeriodChance(new IntRange(0, 100), 5, rs);
//        Decision desc = new FixedChance(new IntRange(1, 10).getInt(rs) / 100f);
        int trueSeq = 0;
        int trueCount = 0;
        int falseCount = 0;
        for (int i = 0; i < 100000; i++)
        {
            boolean result = desc.get(rs);
            if (result) trueCount++;
            else falseCount++;
            if (result) trueSeq++;
            else
            {
                if (trueSeq > 1)
                    System.out.println("True seen " + trueSeq + " times in a row");
                trueSeq = 0;
            }
        }
        System.out.println("Number of trues: " + trueCount + "(" + ((double) trueCount / (trueCount + falseCount) * 100.0 + "%)"));
        System.out.println("Number of false: " + falseCount + "(" + ((double) falseCount / (trueCount + falseCount) * 100.0 + "%)"));
    }

    public static class FixedChance implements Decision
    {
        private final float chance;

        public FixedChance(float chance)
        {
            this.chance = chance;
        }

        @Override
        public boolean get(Random randomSource)
        {
            return randomSource.nextFloat() < chance;
        }
    }

    public static class RandomWalkPeriodChance implements Decision
    {
        private final IntRange range;
        private final int targetBound;
        private final long maxStepSize;
        private long cur;

        public RandomWalkPeriodChance(IntRange range, int targetBound, Random random)
        {
            if (range.max - range.min < 1 << 14)
            {
                range = new IntRange(range.min * 10000, range.max * 1000);
                targetBound = targetBound * 10000;
            }
            this.range = range;
            this.targetBound = targetBound;
            this.maxStepSize = maxStepSize(range, random);
            this.cur = range.getInt(random);
        }

        @Override
        public boolean get(Random randomSource)
        {

            long step = uniform(randomSource, -maxStepSize, maxStepSize);
            long cur = this.cur;
            this.cur = step > 0 ? Math.min(range.max, cur + step)
                    : Math.max(range.min, cur + step);
            return targetBound <= cur;
        }

        private long uniform(Random random, long min, long max)
        {
            if (min >= max) throw new IllegalArgumentException();

            long delta = max - min;
            if (delta == 1) return min;
            if (delta == Long.MIN_VALUE && max == Long.MAX_VALUE) return random.nextLong();
            if (delta < 0) return random.longs(min, max).iterator().nextLong();
//            if (delta <= Integer.MAX_VALUE) return min + uniform(0, (int) delta);

            long result = min + 1 == max ? min : min + ((random.nextLong() & 0x7fffffff) % (max - min));
            assert result >= min && result < max;
            return result;
        }

        private static long maxStepSize(IntRange range, Random random)
        {
            switch (random.nextInt(3))
            {
                case 0:
                    return Math.max(1, (range.max/32) - (range.min/32));
                case 1:
                    return Math.max(1, (range.max/256) - (range.min/256));
                case 2:
                    return Math.max(1, (range.max/2048) - (range.min/2048));
                default:
                    return Math.max(1, (range.max/16384) - (range.min/16384));
            }
        }
    }
}
