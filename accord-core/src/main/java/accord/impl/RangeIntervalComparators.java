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

package accord.impl;

import java.util.Comparator;
import java.util.function.Function;

import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.utils.AsymmetricComparator;
import accord.utils.SymmetricComparator;
import accord.utils.btree.IntervalBTree;

import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.endWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.keyEndWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.keyStartWithEnd;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.keyStartWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.startWithEnd;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.startWithStart;

public class RangeIntervalComparators
{
    public static class InclusiveEndEntryComparators<R> implements IntervalBTree.IntervalComparators<R>
    {
        final Function<R, Range> get;
        final Comparator<R> compareId;

        public InclusiveEndEntryComparators(Function<R, Range> get, Comparator<R> compareId)
        {
            this.get = get;
            this.compareId = compareId;
        }

        @Override public Comparator<R> totalOrder()
        {
            return (a, b) -> {
                int c = get.apply(a).compare(get.apply(b));
                if (c == 0) c = compareId.compare(a, b);
                return c;
            };
        }
        @Override public Comparator<R> endWithEndSorter() { return (a, b) -> get.apply(a).end().compareTo(get.apply(b).end()); }
        @Override public SymmetricComparator<R> startWithStartSeeker() { return (a, b) -> startWithStart(get.apply(a).start().compareTo(get.apply(b).start())); }
        @Override public SymmetricComparator<R> startWithEndSeeker() { return (a, b) -> startWithEnd(get.apply(a).start().compareTo(get.apply(b).end())); }
        @Override public SymmetricComparator<R> endWithStartSeeker() { return (a, b) -> endWithStart(get.apply(a).end().compareTo(get.apply(b).start())); }
    }

    public static class InclusiveEndWithKeyComparators<R> implements IntervalBTree.WithIntervalComparators<RoutingKey, R>
    {
        final Function<R, Range> get;

        public InclusiveEndWithKeyComparators(Function<R, Range> get)
        {
            this.get = get;
        }

        @Override public AsymmetricComparator<RoutingKey, R> startWithStartSeeker() { return (a, b) -> keyStartWithStart(a.compareTo(get.apply(b).start())); }
        @Override public AsymmetricComparator<RoutingKey, R> startWithEndSeeker() { return (a, b) -> keyStartWithEnd(a.compareTo(get.apply(b).end())); }
        @Override public AsymmetricComparator<RoutingKey, R> endWithStartSeeker() { return (a, b) -> keyEndWithStart(a.compareTo(get.apply(b).start())); }
    }

    public static class InclusiveEndWithRangeComparators<R> implements IntervalBTree.WithIntervalComparators<Range, R>
    {
        final Function<R, Range> get;

        public InclusiveEndWithRangeComparators(Function<R, Range> get)
        {
            this.get = get;
        }

        @Override public AsymmetricComparator<Range, R> startWithStartSeeker() { return (a, b) -> startWithStart(a.start().compareTo(get.apply(b).start())); }
        @Override public AsymmetricComparator<Range, R> startWithEndSeeker() { return (a, b) -> startWithEnd(a.start().compareTo(get.apply(b).end())); }
        @Override public AsymmetricComparator<Range, R> endWithStartSeeker() { return (a, b) -> endWithStart(a.end().compareTo(get.apply(b).start())); }
    }

}
