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

import accord.coordinate.FailureAccumulator;
import accord.primitives.Ranges;
import accord.primitives.MinimalSyncPoint;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;

public class DurabilityResult
{
    public final MinimalSyncPoint syncPoint;
    public final ReducingRangeMap<DurabilityLevel> achieved;
    public final DurabilityLevel min;
    public final Throwable failure;

    public DurabilityResult(MinimalSyncPoint syncPoint, DurabilityLevel result, Throwable failure)
    {
        this(syncPoint, ReducingRangeMap.create(syncPoint.route, result), failure);
    }

    public DurabilityResult(MinimalSyncPoint syncPoint, ReducingRangeMap<DurabilityLevel> achieved, Throwable failure)
    {
        this.syncPoint = syncPoint;
        this.achieved = achieved;
        this.failure = failure;
        this.min = achieved.foldl(DurabilityLevel::min);
    }

    public DurabilityResult min(DurabilityResult that)
    {
        Invariants.require(this.syncPoint.syncId.equals(that.syncPoint.syncId));
        Throwable failure = this.failure == null ? that.failure
                                                 : that.failure == null ? this.failure
                                                                        : FailureAccumulator.append(this.failure, that.failure);
        return new DurabilityResult(syncPoint, ReducingRangeMap.merge(this.achieved, that.achieved, DurabilityLevel::min), failure);
    }

    public DurabilityResult max(DurabilityResult that)
    {
        Invariants.require(this.syncPoint.syncId.equals(that.syncPoint.syncId));
        Throwable failure = this.failure == null || that.failure == null ? null : FailureAccumulator.append(this.failure, that.failure);
        return new DurabilityResult(syncPoint, ReducingRangeMap.merge(this.achieved, that.achieved, DurabilityLevel::max), failure);
    }

    @Override
    public String toString()
    {
        return syncPoint.syncId + " achieved " + achieved;
    }

    public Ranges satisfies(DurabilityLevel require)
    {
        if (require.isSatisfiedBy(min))
            return syncPoint.route.toRanges();
        return achieved.foldlWithBounds((l, rs, s, e) -> {
            if (require.isSatisfiedBy(l))
                rs = rs.with(Ranges.of(s.rangeFactory().newRange(s, e)));
            return rs;
        }, Ranges.EMPTY, ignore -> false);
    }
}
