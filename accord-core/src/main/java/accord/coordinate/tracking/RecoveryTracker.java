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

import accord.coordinate.tracking.QuorumTracker.AbstractQuorumShardTracker;
import accord.local.Node;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;

public class RecoveryTracker extends AbstractTracker<RecoveryTracker.RecoveryShardTracker>
{
    public static class RecoveryShardTracker extends AbstractQuorumShardTracker
    {
        protected int fastPathRejects = 0;

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

        private boolean rejectsFastPath(TxnId txnId)
        {
            return shard.rejectsFastPath(txnId, fastPathRejects);
        }
    }

    public RecoveryTracker(Topologies topologies)
    {
        super(topologies, RecoveryShardTracker[]::new, RecoveryShardTracker::new);
    }

    public RequestStatus recordSuccess(Node.Id node, boolean acceptsFastPath)
    {
        if (acceptsFastPath)
            return recordResponse(this, node, RecoveryShardTracker::onSuccess, node);

        return recordResponse(this, node, RecoveryShardTracker::onSuccessRejectFastPath, node);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(this, from, RecoveryShardTracker::onFailure, from);
    }

    public boolean rejectsFastPath(TxnId txnId)
    {
        // a fast path decision must have recorded itself to a fast quorum in an earlier epoch
        // but the fast path votes may be taken from the proposal epoch only.
        // Importantly, on recovery, since we do the full slow path we do not need to reach a fast quorum in the earlier epochs
        // So, we can effectively ignore earlier epochs wrt fast path decisions.
        Topology current = topologies.current();
        for (int i = 0 ; i < current.size() ; ++i)
        {
            if (trackers[i].rejectsFastPath(txnId))
                return true;
        }
        return false;
    }

    public boolean mustSupersedingCoordinatorHaveIntersectedFastQuorum(TxnId self, TxnId superseding)
    {
        // TODO (required): in follow up
        return false;
    }
}
