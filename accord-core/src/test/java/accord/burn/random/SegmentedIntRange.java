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

import java.util.Objects;
import java.util.Random;

public class SegmentedIntRange implements RandomInt
{
    private final RandomInt small, large;
    private final Decision chooseLargeChance;

    public SegmentedIntRange(RandomInt small, RandomInt large, Decision chooseLargeChance)
    {
        this.small = Objects.requireNonNull(small);
        this.large = Objects.requireNonNull(large);
        this.chooseLargeChance = Objects.requireNonNull(chooseLargeChance);
    }

    @Override
    public int getInt(Random randomSource)
    {
        if (chooseLargeChance.get(randomSource)) return large.getInt(randomSource);
        return small.getInt(randomSource);
    }
}
