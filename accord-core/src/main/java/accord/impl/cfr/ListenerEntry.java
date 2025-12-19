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

package accord.impl.cfr;

import java.util.Comparator;

import accord.api.RoutingKey;
import accord.impl.RangeIntervalComparators;
import accord.primitives.Range;
import accord.utils.btree.IntervalBTree;

class ListenerEntry
{
    static final IntervalBTree.IntervalComparators<ListenerEntry> LISTENER_ENTRIES = new RangeIntervalComparators.InclusiveEndEntryComparators<>(e -> e.range, Comparator.comparingLong(a -> a.listener.id));
    static final IntervalBTree.WithIntervalComparators<RoutingKey, ListenerEntry> LISTENER_WITH_KEYS = new RangeIntervalComparators.InclusiveEndWithKeyComparators<>(e -> e.range);
    static final IntervalBTree.WithIntervalComparators<Range, ListenerEntry> LISTENER_WITH_RANGES = new RangeIntervalComparators.InclusiveEndWithRangeComparators<>(e -> e.range);

    final Range range;
    final Listener listener;

    ListenerEntry(Range range, Listener listener)
    {
        this.range = range;
        this.listener = listener;
    }
}
