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

package accord.coordinate.tracking;

import accord.coordinate.tracking.QuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;

import java.util.function.BiFunction;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.*;

// TODO (desired, efficiency): if any shard *cannot* take the fast path, and all shards have accepted, terminate
public class FastPathTracker extends PreAcceptTracker<FastPathTracker.FastPathShardTracker>
{
    private static final ShardOutcome<FastPathTracker> NewFastPathSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnFastPathSuccess;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    public static abstract class FastPathShardTracker extends QuorumShardTracker
    {
        protected int fastPathAccepts;
        protected int fastPathFailures;
        protected int fastPathDelayed;
        protected boolean complete;

        public FastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        // return NewQuorumSuccess ONLY once fast path is rejected
        public abstract ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node);
        final ShardOutcome<? super FastPathTracker> complete(ShardOutcome<? super FastPathTracker> result)
        {
            Invariants.checkState(!complete);
            complete = true;
            return result;
        }

        public ShardOutcome<? super FastPathTracker> onMaybeFastPathSuccess(Node.Id node)
        {
            if (complete)
                return NoChange;

            ++successes;
            if (shard.fastPathElectorate.contains(node))
            {
                ++fastPathAccepts;
                if (hasMetFastPathCriteria())
                    return complete(NewFastPathSuccess);
            }

            return quorumIfHasRejectedFastPath();
        }

        public final ShardOutcome<? super FastPathTracker> onFailure(@Nonnull Node.Id from)
        {
            if (complete)
                return NoChange;

            if (++failures > shard.maxFailures)
                return complete(Fail);

            if (shard.fastPathElectorate.contains(from))
            {
                ++fastPathFailures;

                if (hasRejectedFastPath() && hasReachedQuorum())
                    return complete(Success);
            }

            return NoChange;
        }

        public final ShardOutcome<? super FastPathTracker> onDelayed(@Nonnull Node.Id from)
        {
            if (complete)
                return NoChange;

            if (shard.fastPathElectorate.contains(from))
            {
                ++fastPathDelayed;

                if (isFastPathDelayed() && hasReachedQuorum())
                    return complete(Success);
            }

            return NoChange;
        }

        final ShardOutcome<? super FastPathTracker> quorumIfHasRejectedFastPath()
        {
            return hasReachedQuorum() && hasRejectedFastPath() ? complete(Success) : NoChange;
        }

        final boolean isFastPathDelayed()
        {
            return fastPathDelayed >= shard.fastPathRejectSize;
        }

        @VisibleForTesting
        public final boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= shard.fastPathQuorumSize;
        }

        @VisibleForTesting
        public final boolean hasRejectedFastPath()
        {
            return fastPathFailures >= shard.fastPathRejectSize;
        }
    }

    public static final class PriorFastPathShardTracker extends FastPathShardTracker
    {
        public PriorFastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            return onMaybeFastPathSuccess(node);
        }
    }

    public static final class CurrentFastPathShardTracker extends FastPathShardTracker
    {
        public CurrentFastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            if (complete)
                return NoChange;

            ++successes;
            if (shard.fastPathElectorate.contains(node))
                ++fastPathFailures; // Quorum success can not count towards fast path success

            return quorumIfHasRejectedFastPath();
        }
    }

    int waitingOnFastPathSuccess; // if we reach zero, we have succeeded on the fast path outcome for every shard
    public FastPathTracker(Topologies topologies)
    {
        super(topologies, FastPathShardTracker[]::new, (i, shard) -> i == 0 ? new CurrentFastPathShardTracker(shard) : new PriorFastPathShardTracker(shard));
        this.waitingOnFastPathSuccess = super.waitingOnShards;
    }

    public RequestStatus recordSuccess(Node.Id from, boolean withFastPathTimestamp)
    {
        if (withFastPathTimestamp)
            return recordResponse(from, FastPathShardTracker::onMaybeFastPathSuccess);

        return recordResponse(from, FastPathShardTracker::onQuorumSuccess);
    }

    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(from, FastPathShardTracker::onFailure);
    }

    public RequestStatus recordDelayed(Node.Id from)
    {
        return recordResponse(from, FastPathShardTracker::onDelayed);
    }

    protected RequestStatus recordResponse(Node.Id node, BiFunction<? super FastPathShardTracker, Node.Id, ? extends ShardOutcome<? super FastPathTracker>> function)
    {
        return recordResponse(this, node, function, node);
    }

    public boolean hasFastPathAccepted()
    {
        return waitingOnFastPathSuccess == 0;
    }

    public boolean hasFailed()
    {
        return any(FastPathShardTracker::hasFailed);
    }

    public boolean hasReachedQuorum()
    {
        return all(FastPathShardTracker::hasReachedQuorum);
    }
}
