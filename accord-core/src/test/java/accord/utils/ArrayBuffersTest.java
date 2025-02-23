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

package accord.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.assertj.core.api.Assertions;

import static accord.utils.ArrayBuffers.*;
import static accord.utils.Property.qt;

public class ArrayBuffersTest
{
    public interface TestCase<T>
    {
        void initialize(int maxCount, int maxSize);
        T get(RandomSource rng);
        void check(T v);
        T complete(T v, RandomSource rng);
        T resize(T v, RandomSource rng);
        void discard(T v);
    }

    @Test
    public void simpleIntsTest()
    {
        simpleTest(new IntBufTestCase());
    }

    public static class IntBufTestCase implements TestCase<int[]>
    {
        private IntBufferCache cache;

        @Override
        public void initialize(int maxCount, int maxSize)
        {
            this.cache = new IntBufferCache(maxCount, maxSize);
        }

        @Override
        public int[] get(RandomSource rng)
        {
            int[] arr = cache.getInts(rng.nextInt(1, 100));
            Arrays.fill(arr, 255);
            return arr;
        }

        @Override
        public void check(int[] arr)
        {
            for (int v : arr)
                Assertions.assertThat(v).isEqualTo(255);
        }

        @Override
        public int[] complete(int[] v, RandomSource rng)
        {
            return new int[0];
        }

        @Override
        public int[] resize(int[] arr, RandomSource rng)
        {
            int usedSize = arr.length == 1 ? 1 : rng.nextInt(1, arr.length);
            arr = cache.resize(arr,
                               usedSize,
                               rng.nextInt(usedSize, 100));
            Arrays.fill(arr, 255);
            return arr;
        }

        @Override
        public void discard(int[] v)
        {
            Arrays.fill(v, 0);
            cache.discard(v, 0);
        }
    }

    @Test
    public void simpleLongsTest()
    {
        simpleTest(new LongBufTestCase());
    }

    public static class LongBufTestCase implements TestCase<long[]>
    {
        private LongBufferCache cache;

        @Override
        public void initialize(int maxCount, int maxSize)
        {
            this.cache = new LongBufferCache(maxCount, maxSize);
        }

        @Override
        public long[] get(RandomSource rng)
        {
            long[] arr = cache.getLongs(rng.nextInt(1, 100));
            Arrays.fill(arr, 255);
            return arr;
        }

        @Override
        public void check(long[] arr)
        {
            for (long v : arr)
                Assertions.assertThat(v).isEqualTo(255);
        }

        @Override
        public long[] complete(long[] v, RandomSource rng)
        {
            return new long[0];
        }

        @Override
        public long[] resize(long[] arr, RandomSource rng)
        {
            int usedSize = arr.length == 1 ? 1 : rng.nextInt(1, arr.length);
            arr = cache.resize(arr,
                               usedSize,
                               rng.nextInt(usedSize, 100));
            Arrays.fill(arr, 255);
            return arr;
        }

        @Override
        public void discard(long[] v)
        {
            Arrays.fill(v, 0);
            cache.discard(v, 0);
        }
    }

    private <T> void simpleTest(TestCase<T> testCase)
    {
        Gen<Operation> opGen = Gens.enums().all(Operation.class);
        qt().forAll(Gens.random())
            .check(rng -> {
                for (int maxCount = 1; maxCount < 10; maxCount += 2)
                {
                    for (int maxSize = 0; maxSize < 14; maxSize += 2)
                    {
                        testCase.initialize(maxCount, 1 << maxSize);

                        List<T> taken = new ArrayList<>();
                        for (int i = 0; i < 1000; i++)
                        {
                            switch (opGen.next(rng))
                            {
                                case GetInts:
                                {
                                    T arr = testCase.get(rng);
                                    taken.add(arr);
                                    break;
                                }
                                case Resize:
                                {
                                    if (taken.isEmpty())
                                        continue;
                                    T v = taken.remove(rng.nextInt(0, taken.size()));
                                    testCase.check(v);
                                    v = testCase.resize(v, rng);
                                    taken.add(v);
                                    break;
                                }
                                case Complete:
                                {
                                    if (taken.isEmpty())
                                        continue;
                                    T v = taken.remove(rng.nextInt(0, taken.size()));
                                    testCase.check(v);
                                    testCase.check(testCase.complete(v, rng));
                                    taken.add(v);
                                    break;
                                }
                                case Discard:
                                {
                                    if (taken.isEmpty())
                                        continue;
                                    T v = taken.remove(rng.nextInt(0, taken.size()));
                                    testCase.discard(v);
                                    break;
                                }
                            }
                        }
                    }
                }
            });
    }

    public enum Operation
    {
        GetInts, Complete, Resize, Discard
    }
}
