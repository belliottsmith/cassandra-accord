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
    public final @Nullable Collection<Node.Id> excluding;
    public final Throwable failure;

    public DurabilityResult(SyncPoint<Range> syncPoint, SyncLocal achievedLocal, SyncRemote achievedRemote, @Nullable Collection<Node.Id> excluding, Throwable failure)
    {
        this.syncPoint = syncPoint;
        this.achievedLocal = achievedLocal;
        this.achievedRemote = achievedRemote;
        this.excluding = excluding;
        this.failure = failure;
    }

    public DurabilityResult merge(DurabilityResult that)
    {
        SyncLocal achievedLocal = min(this.achievedLocal, that.achievedLocal);
        SyncRemote achievedRemote = min(this.achievedRemote, that.achievedRemote);
        Collection<Node.Id> excluding = this.excluding == null ? that.excluding :
                                        that.excluding == null ? this.excluding :
                                        SortedArrays.SortedArrayList.copyUnsorted(this.excluding, Node.Id[]::new).with(SortedArrays.SortedArrayList.copyUnsorted(that.excluding, Node.Id[]::new));
        Throwable failure = this.failure == null ? that.failure
                                 : that.failure == null ? this.failure
                                        : FailureAccumulator.append(this.failure, that.failure);
        return new DurabilityResult(syncPoint, achievedLocal, achievedRemote, excluding, failure);
    }

    @Override
    public String toString()
    {
        return syncPoint.syncId + " for " + syncPoint.route.toRanges()
               + " achieved: (" + achievedLocal + ',' + achievedRemote
               + ", excluding: " + excluding + ')';
    }

    private static <E extends Enum<E>> E min(E a, E b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
