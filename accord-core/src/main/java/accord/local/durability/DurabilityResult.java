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

package accord.local.durability;

import java.util.Collection;

import javax.annotation.Nullable;

import accord.coordinate.FailureAccumulator;
import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.Range;
import accord.primitives.SyncPoint;
import accord.utils.SortedArrays;

public class DurabilityResult
{
    public final SyncPoint<Range> syncPoint;
    public final SyncLocal achievedLocal;
    public final SyncRemote achievedRemote;
    public final @Nullable Collection<Node.Id> including;
    public final @Nullable Collection<Node.Id> excluding;
    public final Throwable failure;

    public DurabilityResult(SyncPoint<Range> syncPoint, SyncLocal achievedLocal, SyncRemote achievedRemote, @Nullable Collection<Node.Id> including, @Nullable Collection<Node.Id> excluding, Throwable failure)
    {
        this.syncPoint = syncPoint;
        this.achievedLocal = achievedLocal;
        this.achievedRemote = achievedRemote;
        this.including = including;
        this.excluding = excluding;
        this.failure = failure;
    }

    public DurabilityResult merge(DurabilityResult that)
    {
        SyncLocal achievedLocal = min(this.achievedLocal, that.achievedLocal);
        SyncRemote achievedRemote = min(this.achievedRemote, that.achievedRemote);
        Collection<Node.Id> including = merge(this.including, that.including);
        Collection<Node.Id> excluding = merge(this.excluding, that.excluding);
        Throwable failure = this.failure == null ? that.failure
                                 : that.failure == null ? this.failure
                                        : FailureAccumulator.append(this.failure, that.failure);
        return new DurabilityResult(syncPoint, achievedLocal, achievedRemote, including, excluding, failure);
    }

    private static Collection<Node.Id> merge(Collection<Node.Id> a, Collection<Node.Id> b)
    {
        return a == null ? b : b == null ? a :SortedArrays.SortedArrayList.copyUnsorted(a, Node.Id[]::new).with(SortedArrays.SortedArrayList.copyUnsorted(b, Node.Id[]::new));
    }

    @Override
    public String toString()
    {
        return syncPoint.syncId + " for " + syncPoint.route.toRanges()
               + " achieved: (" + achievedLocal + ',' + achievedRemote
               + ", including: " + including + ')';
    }

    private static <E extends Enum<E>> E min(E a, E b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
