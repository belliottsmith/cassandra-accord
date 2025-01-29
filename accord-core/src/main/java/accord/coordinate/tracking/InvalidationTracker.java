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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.UnhandledEnum;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.*;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.Unoptimised;

public class InvalidationTracker extends AbstractTracker<InvalidationTracker.InvalidationShardTracker>
{
    public static class InvalidationShardTracker extends ShardTracker implements ShardOutcome<InvalidationTracker>
    {
        private final int fastQuorumSize;
        private int fastPathRejects;
        private int fastPathInflight;
        private int promisesOrPartPromises;
        private boolean hasDecision;
        private int inflight;
        private boolean isFinal;
        private @Nullable Participants<?> neverTruncated;

        private InvalidationShardTracker(FastPath fastPath, Shard shard)
        {
            super(shard);
            inflight = shard.rf();
            fastPathInflight = shard.fastPathElectorateSize;
            fastQuorumSize = shard.fastQuorumSize(fastPath);
        }

        public InvalidationShardTracker onSuccess(Node.Id from, @Nullable Participants<?> promised, @Nonnull Participants<?> notTruncated, @Nullable Participants<?> truncated, boolean isDecided, boolean withFastPath)
        {
            if (shard.isInFastPath(from))
            {
                --fastPathInflight;
                if (!withFastPath) ++fastPathRejects;
            }
            if (promised != null && promised.intersects(shard.range))
                ++promisesOrPartPromises;
            if (neverTruncated == null) neverTruncated = notTruncated;
            if (truncated != null && truncated.intersects(shard.range))
                neverTruncated = neverTruncated.without(truncated);

            if (isDecided) hasDecision = true;
            --inflight;
            return this;
        }

        public ShardOutcome<? super InvalidationTracker> onFailure(Node.Id from)
        {
            if (shard.isInFastPath(from))
                --fastPathInflight;
            --inflight;
            return this;
        }

        public boolean isFinal()
        {
            return hasDecision || (isFastPathDecided() && isPromiseDecided());
        }

        private boolean isFastPathDecided()
        {
            return isFastPathRejected() || !canFastPathBeRejected();
        }

        public boolean isFastPathRejected()
        {
            return shard.rejectsFastPath(fastQuorumSize, fastPathRejects);
        }

        public boolean canFastPathBeRejected()
        {
            return shard.rejectsFastPath(fastQuorumSize, fastPathRejects + fastPathInflight);
        }

        private boolean isPromiseDecided()
        {
            return isPromised() || isPromiseRejected();
        }

        public boolean isPromiseRejected()
        {
            return promisesOrPartPromises + inflight < shard.slowQuorumSize;
        }

        public boolean isPromised()
        {
            return promisesOrPartPromises >= shard.slowQuorumSize;
        }

        public boolean isAnyNotTruncated()
        {
            return neverTruncated != null && !neverTruncated.isEmpty();
        }

        public boolean isPromisedOrHasDecision()
        {
            return isPromised() || hasDecision();
        }

        public boolean hasDecision()
        {
            return hasDecision;
        }

        @Override
        public ShardOutcomes apply(InvalidationTracker tracker, int shardIndex)
        {
            if (isFinal)
                return NoChange;

            if (isFastPathRejected()) tracker.rejectsFastPath = true;
            if (isPromised() && tracker.promisedShard < 0) tracker.promisedShard = shardIndex;
            if (isFinal()) isFinal = true;

            if (tracker.rejectsFastPath && tracker.promisedShard >= 0)
            {
                tracker.waitingOnShards = 0;
                return Success;
            }

            if (isFinal && --tracker.waitingOnShards == 0)
                return tracker.all(InvalidationShardTracker::isPromisedOrHasDecision) ? Success : Fail;

            return NoChange;
        }
    }

    private int promisedShard = -1;
    private boolean rejectsFastPath;
    public InvalidationTracker(Topologies topologies, TxnId txnId)
    {
        super(topologies, InvalidationShardTracker[]::new, factory(txnId));
    }

    public Shard promisedShard()
    {
        return get(promisedShard).shard;
    }

    public boolean isPromised()
    {
        return promisedShard >= 0;
    }

    public boolean isAnyNotTruncated()
    {
        return any(InvalidationShardTracker::isAnyNotTruncated);
    }

    public boolean isSafeToInvalidate()
    {
        return rejectsFastPath;
    }

    public RequestStatus recordSuccess(Node.Id from, @Nullable Participants<?> promised, @Nonnull Participants<?> notTruncated, @Nullable Participants<?> truncated, boolean hasDecision, boolean acceptedFastPath)
    {
        return recordResponse(this, from, (shard, node) -> shard.onSuccess(node, promised, notTruncated, truncated, hasDecision, acceptedFastPath), from);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(this, from, InvalidationShardTracker::onFailure, from);
    }

    private static ShardFactory<InvalidationShardTracker> factory(TxnId txnId)
    {
        switch (txnId.fastPath())
        {
            default: throw new UnhandledEnum(txnId.fastPath());
            case Unoptimised: return (i, s) -> new InvalidationShardTracker(Unoptimised, s);
            case PrivilegedCoordinatorWithoutDeps: return (i, s) -> new InvalidationShardTracker(PrivilegedCoordinatorWithoutDeps, s);
            case PrivilegedCoordinatorWithDeps: return (i, s) -> new InvalidationShardTracker(PrivilegedCoordinatorWithDeps, s);
        }
    }
}
