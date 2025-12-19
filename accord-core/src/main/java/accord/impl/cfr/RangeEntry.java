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

import accord.api.RoutingKey;
import accord.impl.RangeIntervalComparators.InclusiveEndEntryComparators;
import accord.impl.RangeIntervalComparators.InclusiveEndWithKeyComparators;
import accord.impl.RangeIntervalComparators.InclusiveEndWithRangeComparators;
import accord.primitives.Range;
import accord.utils.btree.IntervalBTree.IntervalComparators;
import accord.utils.btree.IntervalBTree.WithIntervalComparators;

public interface RangeEntry
{
    Range range();
    IdEntry id();

    IntervalComparators<RangeEntry> ENTRIES = new InclusiveEndEntryComparators<>(RangeEntry::range, (a, b) -> a.id().compareTo(b.id()));
    WithIntervalComparators<RoutingKey, RangeEntry> WITH_KEY = new InclusiveEndWithKeyComparators<>(RangeEntry::range);
    WithIntervalComparators<Range, RangeEntry> WITH_RANGE = new InclusiveEndWithRangeComparators<>(RangeEntry::range);
}
