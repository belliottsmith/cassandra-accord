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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import accord.utils.SortedArrays.SortedArrayList;

import static org.assertj.core.api.Assertions.assertThat;

public class SortedListSetTest
{
    @Test
    public void setTest()
    {
        for (int iteration = 0; iteration < 1000; iteration++)
        {
            RandomTestRunner.test().withSeed(0).check(rs -> {
                List<Integer> input = new ArrayList<>();
                int size = rs.nextInt(1, 100);
                for (int i = 0; i < size; i++)
                {
                    int v = rs.nextInt();
                    input.add(v);
                }

                input.sort(Integer::compare);
                Integer[] sorted = input.toArray(new Integer[0]);
                SortedArrays.assertSorted(sorted);
                SortedListSet<Integer> sut = SortedListSet.noneOf(new SortedArrayList<>(sorted));
                Set<Integer> model = new TreeSet<>();
                for (int i = 0; i < 10_000; i++)
                {
                    int v = input.get(rs.nextInt(input.size()));
                    assertThat(model.size()).isEqualTo(sut.size());
                    assertThat(model.contains(v)).isEqualTo(sut.contains((Integer)v));
                    assertThat(model).isEqualTo(sut);
                    assertThat(ImmutableList.copyOf(model.iterator())).isEqualTo(ImmutableList.copyOf(sut.iterator()));
                    if (rs.nextBoolean())
                    {
                        model.add(v);
                        sut.add(v);
                    }
                    else
                    {
                        model.remove(v);
                        sut.remove(v);
                    }
                }
            });
        }
    }
}
