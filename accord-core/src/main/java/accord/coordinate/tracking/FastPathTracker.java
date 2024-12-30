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
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;

import java.util.function.BiFunction;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.*;
import static accord.primitives.TxnId.FastPath.PRIVILEGED_COORDINATOR_WITHOUT_DEPS;
import static accord.primitives.TxnId.FastPath.PRIVILEGED_COORDINATOR_WITH_DEPS;
import static accord.primitives.TxnId.FastPath.UNOPTIMISED;

// TODO (desired, efficiency): if any shard *cannot* take the fast path, and all shards have accepted, terminate
public class FastPathTracker extends PreAcceptTracker<FastPathTracker.FastPathShardTracker>
{
    private static final ShardOutcome<FastPathTracker> NewFastPathSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnFastPathSuccess;
        --tracker.waitingOnMediumPathSuccess;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    private static final ShardOutcome<FastPathTracker> NewMediumPathSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnMediumPathSuccess;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    public static abstract class FastPathShardTracker extends QuorumShardTracker
    {
        final int fastQuorumSize;
        protected int fastPathAccepts;
        protected int fastPathFailures;
        protected int fastPathDelayed;
        protected boolean complete;

        public FastPathShardTracker(FastPath fastPath, Shard shard)
        {
            super(shard);
            this.fastQuorumSize = shard.fastQuorumSize(fastPath);
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
            if (shard.isInFastPath(node))
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

            if (shard.isInFastPath(from))
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

            if (shard.isInFastPath(from))
            {
                ++fastPathDelayed;

                if (isFastPathDelayed() && hasReachedQuorum())
                    return complete(Success);
            }

            return NoChange;
        }

        final ShardOutcome<? super FastPathTracker> quorumIfHasRejectedFastPath()
        {
            return hasReachedQuorum() && hasRejectedFastPath()
                   ? hasMetMediumPathCriteria() ? complete(NewMediumPathSuccess) : complete(Success)
                   : NoChange;
        }

        final boolean isFastPathDelayed()
        {
            return shard.rejectsFastPath(fastQuorumSize, fastPathDelayed);
        }

        @VisibleForTesting
        public final boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= fastQuorumSize;
        }

        @VisibleForTesting
        public final boolean hasMetMediumPathCriteria()
        {
            return fastPathAccepts >= shard.slowQuorumSize;
        }

        @VisibleForTesting
        public final boolean hasRejectedFastPath()
        {
            return shard.rejectsFastPath(fastQuorumSize, fastPathFailures);
        }
    }

    public static final class PriorFastPathShardTracker extends FastPathShardTracker
    {
        public PriorFastPathShardTracker(FastPath fastPath, Shard shard)
        {
            super(fastPath, shard);
        }

        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            return onMaybeFastPathSuccess(node);
        }
    }

    public static final class CurrentFastPathShardTracker extends FastPathShardTracker
    {
        public CurrentFastPathShardTracker(FastPath fastPath, Shard shard)
        {
            super(fastPath, shard);
        }

        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            if (complete)
                return NoChange;

            ++successes;
            if (shard.isInFastPath(node))
                ++fastPathFailures; // Quorum success can not count towards fast path success

            return quorumIfHasRejectedFastPath();
        }
    }

    int waitingOnFastPathSuccess; // if we reach zero, we have succeeded on the fast path for every shard
    int waitingOnMediumPathSuccess; // if we reach zero, we have succeeded on the medium path for every shard
    public FastPathTracker(Topologies topologies, TxnId txnId)
    {
        super(topologies, FastPathShardTracker[]::new, factory(txnId));
        this.waitingOnFastPathSuccess = this.waitingOnMediumPathSuccess = super.waitingOnShards;
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

    public boolean hasMediumPathAccepted()
    {
        return waitingOnMediumPathSuccess == 0;
    }

    public boolean hasFailed()
    {
        return any(FastPathShardTracker::hasFailed);
    }

    public boolean hasReachedQuorum()
    {
        return all(FastPathShardTracker::hasReachedQuorum);
    }

    private static ShardFactory<FastPathShardTracker> factory(TxnId txnId)
    {
        switch (txnId.fastPath())
        {
            default: throw new UnhandledEnum(txnId.fastPath());
            case UNOPTIMISED: return (i, s) -> create(UNOPTIMISED, i, s);
            case PRIVILEGED_COORDINATOR_WITHOUT_DEPS: return (i, s) -> create(PRIVILEGED_COORDINATOR_WITHOUT_DEPS, i, s);
            case PRIVILEGED_COORDINATOR_WITH_DEPS: return (i, s) -> create(PRIVILEGED_COORDINATOR_WITH_DEPS, i, s);
        }
    }

    private static FastPathShardTracker create(FastPath fastPath, int index, Shard shard)
    {
        return index == 0 ? new CurrentFastPathShardTracker(fastPath, shard) : new PriorFastPathShardTracker(fastPath, shard);
    }
}
