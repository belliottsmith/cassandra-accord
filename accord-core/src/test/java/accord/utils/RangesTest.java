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

import accord.primitives.Range;
import accord.impl.IntKey;
import accord.primitives.Ranges;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;

public class RangesTest
{
    private static Range r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static Ranges ranges(Range... ranges)
    {
        return Ranges.of(ranges);
    }

    @Test
    void rangeIndexForKeyTest()
    {
        Ranges ranges = ranges(r(100, 200), r(300, 400));
        Assertions.assertEquals(-1, ranges.indexOf(IntKey.key(50)));
        Assertions.assertEquals(0, ranges.indexOf(IntKey.key(150)));
        Assertions.assertEquals(-2, ranges.indexOf(IntKey.key(250)));
        Assertions.assertEquals(1, ranges.indexOf(IntKey.key(350)));
        Assertions.assertEquals(-3, ranges.indexOf(IntKey.key(450)));
    }

    @Test
    void differenceTest()
    {
        Assertions.assertEquals(ranges(r(100, 125), r(175, 200)),
                                ranges(r(100, 200)).without(
                                        ranges(r(125, 175))));
        Assertions.assertEquals(ranges(r(125, 175)),
                                ranges(r(100, 200)).without(
                                        ranges(r(100, 125), r(175, 200))));
        Assertions.assertEquals(ranges(r(100, 175)),
                                ranges(r(100, 200)).without(
                                        ranges(r(0, 75), r(175, 200))));
        Assertions.assertEquals(ranges(r(100, 200)),
                                ranges(r(100, 200)).without(
                                        ranges(r(0, 75), r(200, 205))));

        Assertions.assertEquals(ranges(r(125, 175), r(300, 350)),
                                ranges(r(100, 200), r(250, 350)).without(
                                        ranges(r(0, 125), r(175, 300))));
        Assertions.assertEquals(ranges(r(125, 200), r(300, 350)),
                                ranges(r(100, 200), r(250, 350)).without(
                                        ranges(r(0, 125), r(225, 300))));

        Assertions.assertEquals(ranges(r(125, 135), r(140, 160), r(175, 200)),
                                ranges(r(100, 200)).without(
                                        ranges(r(0, 125), r(135, 140), r(160, 170), r(170, 175))));
    }

    @Test
    void addTest()
    {
        Assertions.assertEquals(ranges(r(0, 50), r(50, 100), r(100, 150), r(150, 200)),
                                ranges(r(0, 50), r(100, 150)).with(ranges(r(50, 100), r(150, 200))));
    }

    private static void assertMergeTouchingResult(Ranges expectedTouching, Ranges input1, Ranges input2)
    {
        Assertions.assertEquals(expectedTouching, input1.union(MERGE_ADJACENT, input2));
        Assertions.assertEquals(expectedTouching, input2.union(MERGE_ADJACENT, input1));
    }

    private static void assertMergeOverlappingResult(Ranges expectedOverlapping, Ranges input1, Ranges input2)
    {
        Assertions.assertEquals(expectedOverlapping, input1.union(MERGE_OVERLAPPING, input2));
        Assertions.assertEquals(expectedOverlapping, input2.union(MERGE_OVERLAPPING, input1));
    }

    @Test
    void mergeTest()
    {
        assertMergeOverlappingResult(ranges(r(0, 50), r(100, 350)),
                                     ranges(r(100, 250), r(300, 350)),
                                     ranges(r(0, 50), r(200, 301), r(310, 315)));
        assertMergeTouchingResult(ranges(r(0, 50), r(100, 350)),
                                     ranges(r(100, 250), r(300, 350)),
                                     ranges(r(0, 50), r(200, 300), r(310, 315)));
        assertMergeOverlappingResult(ranges(r(0, 100)),
                                     Ranges.EMPTY,
                                     ranges(r(0, 100)));
        assertMergeTouchingResult(ranges(r(0, 100)),
                                  Ranges.EMPTY,
                                  ranges(r(0, 100)));
    }

    @Test
    void mergeTouchingTest()
    {
        Assertions.assertEquals(ranges(r(0, 400)), ranges(r(0, 100), r(100, 200), r(200, 300), r(300, 400)).mergeTouching());
        Assertions.assertEquals(ranges(r(0, 200), r(300, 400)), ranges(r(0, 100), r(100, 200), r(300, 400)).mergeTouching());
        Assertions.assertEquals(ranges(r(0, 100), r(200, 400)), ranges(r(0, 100), r(200, 300), r(300, 400)).mergeTouching());
    }

    @Test
    void mergeTestWithOverlapsTest()
    {
        for (int count = 0 ; count < 1000 ; ++count)
        {
            RandomTestRunner.test().check(rs -> {
                Range[] as = new Range[rs.nextInt(24)];
                int rangeSize = rs.nextInt(128, 1024);
                float gapRate = rs.nextFloat() * 0.5f;
                int start = 0;
                for (int i = 0 ; i < as.length ; i++)
                {
                    if (rs.decide(gapRate))
                        start = start + rs.nextInt(1, rangeSize);
                    int end = start + rs.nextInt(1, rangeSize);
                    as[i] = IntKey.range(start, end);
                    start = end;
                }
                Range[] bs = new Range[as.length == 0 ? 0 : rs.nextInt(as.length)];
                {
                    int bi = 0;
                    for (int ai = 0 ; ai < as.length ; ++ai)
                    {
                        int ar = as.length - ai;
                        int br = bs.length - bi;
                        if (rs.nextInt(ar) < br)
                            bs[bi++] = as[ai];
                    }
                }

                float modifyChance = rs.nextFloat() * 0.5f;
                for (int bi = 0 ; bi < bs.length ; bi++)
                {
                    IntKey.Routing s = (IntKey.Routing) bs[bi].start();
                    IntKey.Routing e = (IntKey.Routing) bs[bi].end();
                    if (s.key + 2 < e.key && rs.decide(modifyChance))
                    {
                        if (rs.nextBoolean()) bs[bi] = bs[bi].newRange(IntKey.routing(rs.nextInt(s.key, e.key - 1)), e);
                        else bs[bi] = bs[bi].newRange(s, IntKey.routing(rs.nextInt(s.key + 1, e.key)));
                    }
                }

                Range[] all = new Range[as.length + bs.length];
                System.arraycopy(as, 0, all, 0, as.length);
                System.arraycopy(bs, 0, all, as.length, bs.length);
                Ranges canonical = Ranges.of(all);
                Ranges testOverlap = Ranges.of(as).union(MERGE_OVERLAPPING, Ranges.of(bs));
                Ranges testAdjacent = Ranges.of(as).union(MERGE_ADJACENT, Ranges.of(bs));
                Assertions.assertEquals(canonical, testOverlap);
                canonical = canonical.mergeTouching();
                Assertions.assertEquals(canonical, testAdjacent);
            });

        }
    }

    @Test
    void selectTest()
    {
        Ranges testRanges = ranges(r(0, 100), r(100, 200), r(200, 300), r(300, 400), r(400, 500));
        Assertions.assertEquals(ranges(testRanges.get(1), testRanges.get(3)), testRanges.select(new int[]{1, 3}));
    }
}
