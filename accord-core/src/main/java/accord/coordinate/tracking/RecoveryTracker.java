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

import javax.annotation.Nullable;

import accord.coordinate.Recover.InferredFastPath;
import accord.coordinate.tracking.QuorumTracker.AbstractQuorumShardTracker;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;

import static accord.coordinate.Recover.InferredFastPath.Accept;
import static accord.coordinate.Recover.InferredFastPath.Reject;
import static accord.coordinate.Recover.InferredFastPath.Unknown;

public class RecoveryTracker extends AbstractTracker<RecoveryTracker.RecoveryShardTracker>
{
    public static class RecoveryShardTracker extends AbstractQuorumShardTracker
    {
        protected int fastPathRejects;
        protected int fastPathAccepts;

        @Override
        public boolean hasReachedQuorum()
        {
            return successes >= shard.recoveryQuorumSize;
        }

        private RecoveryShardTracker(Shard shard)
        {
            super(shard);
        }

        private ShardOutcomes onSuccessRejectFastPath(Node.Id from)
        {
            if (shard.isInFastPath(from))
                ++fastPathRejects;
            return onSuccess(from);
        }

        public ShardOutcomes onSuccessAcceptsFastPath(Node.Id from)
        {
            if (shard.isInFastPath(from))
                ++fastPathAccepts;
            return super.onSuccess(from);
        }

        // to be invoked only if both recoverId and awaitId have privileged coordinators with Deps,
        // and applies only to other txnId whose coordinators were not in the recovery quorum of recoverId
        public boolean fastPathReliesOnUnwitnessedCoordinatorVote(TxnId recoverId, @Nullable Participants<?> selfCoordVotes)
        {
            int mustContainCoord = shard.fastQuorumSize(recoverId) + fastPathRejects + (selfCoordVotes == null || selfCoordVotes.intersects(shard.range) ? 0 : 1);
            Invariants.require(mustContainCoord <= shard.nodes.size());
            return mustContainCoord == shard.nodes.size();
        }
    }

    public RecoveryTracker(Topologies topologies)
    {
        super(topologies, RecoveryShardTracker[]::new, RecoveryShardTracker::new);
    }

    public RequestStatus recordSuccess(Node.Id node, boolean acceptsFastPath)
    {
        if (acceptsFastPath) return recordResponse(this, node, RecoveryShardTracker::onSuccessAcceptsFastPath, node);
        else return recordResponse(this, node, RecoveryShardTracker::onSuccessRejectFastPath, node);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(this, from, RecoveryShardTracker::onFailure, from);
    }

    public InferredFastPath inferFastPathDecision(TxnId txnId, @Nullable Participants<?> extraSelfVotes, @Nullable Participants<?> extraRejects)
    {
        // a fast path decision must have recorded itself to a fast quorum in an earlier epoch
        // but the fast path votes may be taken from the proposal epoch only.
        // Importantly, on recovery, since we do the full slow path we do not need to reach a fast quorum in the earlier epochs
        // So, we can effectively ignore earlier epochs wrt fast path decisions.
        Invariants.require(!txnId.hasPrivilegedCoordinator() || extraSelfVotes != null);
        Topology current = topologies.current();
        boolean accept = true;
        for (int i = 0 ; i < current.size() ; ++i)
        {
            RecoveryShardTracker shardTracker = trackers[i];
            int extraAccept = 0, extraReject = 0;
            if (txnId.hasPrivilegedCoordinator())
            {
                int delta = extraSelfVotes.intersects(shardTracker.shard.range) ? 1 : 0;
                extraAccept += delta;
                extraReject += (1 - delta);
            }
            if (extraRejects != null && extraRejects.intersects(shardTracker.shard.range))
                extraReject++;
            if (shardTracker.shard.rejectsFastPath(txnId, shardTracker.fastPathRejects + extraReject))
                return Reject;
            if (accept && !shardTracker.shard.acceptsFastPath(txnId, shardTracker.fastPathAccepts + extraAccept))
                accept = false;
        }
        return accept ? Accept : Unknown;
    }
}
