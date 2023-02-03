/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.burn.random;

import java.util.Random;

public interface Decision
{
    boolean get(Random randomSource);

    public static void main(String[] args) {
//        Random rs = new Random(0);
        Random rs = new Random();
        //TODO ranges < 16384 always returns 1...
        int maxSmall = 100000;
        SegmentedRandomWalkPeriod period = new SegmentedRandomWalkPeriod(new IntRange(0, maxSmall), new IntRange(0, 100000000), 16, rs);
//        Decision desc = new FixedChance(new IntRange(1, 10).getInt(rs) / 100f);
        int largeSeq = 0;
        int largeCount = 0;
        int smallCount = 0;
        for (int i = 0; i < 100000000; i++)
        {
            long size = period.get(rs);
            if (size >= maxSmall)
            {
                largeCount++;
                largeSeq++;
            }
            else
            {
                if (largeSeq > 1)
                    System.out.println("Large seen " + largeSeq + " times in a row");
                smallCount++;
                largeSeq = 0;
            }
        }
        System.out.println("Number of large: " + largeCount + "(" + ((double) largeCount / (largeCount + smallCount) * 100.0 + "%)"));
        System.out.println("Number of small: " + smallCount + "(" + ((double) smallCount / (largeCount + smallCount) * 100.0 + "%)"));
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

    class SegmentedRandomWalkPeriod
    {
        private final RandomWalkPeriod small, large;
        private final int smallRatio;
        private RandomWalkPeriod cur;
        private long largeDebt;

        public SegmentedRandomWalkPeriod(IntRange smallRange, IntRange largeRange, int smallRatio, Random random)
        {
            this.small = new RandomWalkPeriod(smallRange, random);
            this.large = new RandomWalkPeriod(largeRange, random);
            this.smallRatio = smallRatio;
            this.cur = small;
        }

        public long get(Random randomSource)
        {
            long next;
            if (cur == small && (largeDebt <= 0 || (largeDebt < Integer.MAX_VALUE && randomSource.nextInt((int)largeDebt) == 0)))
            {
                large.cur = small.cur;
                next = large.get(randomSource);
                if (large.cur >= small.range.max) cur = large;
                else cur.cur = large.cur;
            }
            else
            {
                next = cur.get(randomSource);
            }

            if (cur == large && cur.cur < small.range.max)
            {
                small.cur = cur.cur;
                cur = small;
            }

            if (cur == large) largeDebt += smallRatio;
            else --largeDebt;

            return next;
        }
    }

    class RandomWalkPeriod
    {
        private final IntRange range;
        private final long maxStepSize;
        private long cur;

        public RandomWalkPeriod(IntRange range, Random random)
        {
            this.range = range;
            this.maxStepSize = maxStepSize(range, random);
            this.cur = range.getInt(random);
        }

        public long get(Random random)
        {
            long step = uniform(random, -maxStepSize, maxStepSize);
            long cur = this.cur;
            this.cur = step > 0 ? Math.min(range.max, cur + step)
                                : Math.max(range.min, cur + step);
            return cur;
        }

        private static int maxStepSize(IntRange range, Random random)
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

    private static long uniform(Random random, long min, long max)
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

}
