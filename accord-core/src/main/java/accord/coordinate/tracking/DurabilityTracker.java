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

import java.util.Set;

import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListSet;
import org.agrona.collections.IntHashSet;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;

public class DurabilityTracker extends SimpleTracker<DurabilityTracker.DurabilityShardTracker> implements ResponseTracker
{
    private static final ShardOutcome<DurabilityTracker> NewQuorum = (t, i) -> {
        --t.waitingOnQuorum;
        return NoChange;
    };

    private static final ShardOutcome<DurabilityTracker> NewSuccessAndQuorum = (t, i) -> {
        --t.waitingOnQuorum;
        return Success.apply(t, i);
    };

    private static final ShardOutcome<DurabilityTracker> NewFailAndQuorum = (t, i) -> {
        --t.waitingOnQuorum;
        return Fail.apply(t, i);
    };

    public static class DurabilityShardTracker extends ShardTracker
    {
        static final IntHashSet EMPTY_SET = new IntHashSet();

        protected final IntHashSet successes = new IntHashSet();
        protected final IntHashSet exclude;
        protected int waitingOnSuccess;
        protected int waitingOn;

        public DurabilityShardTracker(SortedArrayList<Node.Id> exclude, Shard shard)
        {
            super(shard);
            if (!exclude.isEmpty() || !shard.hardRemoved.isEmpty())
            {
                this.exclude = new IntHashSet();
                for (Node.Id id : shard.hardRemoved)
                {
                    this.exclude.add(id.id);
                }
                for (Node.Id id : shard.nodes)
                {
                    if (exclude.contains(id))
                        this.exclude.add(id.id);
                }
            }
            else this.exclude = EMPTY_SET;
            this.waitingOn = shard.rf();
            this.waitingOnSuccess = waitingOn - this.exclude.size();
            Invariants.require(this.exclude.size() <= shard.maxFailures);
        }

        public ShardOutcome<? super DurabilityTracker> onSuccess(Node.Id from)
        {
            successes.add(from.id);
            if (!exclude.contains(from.id))
                --waitingOnSuccess;
            return onResponse(successes.size() == shard.slowQuorumSize);
        }

        // return true iff hasFailed()
        public ShardOutcome<? super DurabilityTracker> onFailure(Node.Id from)
        {
            return onResponse(false);
        }

        private ShardOutcome<? super DurabilityTracker> onResponse(boolean newQuorum)
        {
            if (--waitingOn > 0)
                return newQuorum ? NewQuorum : NoChange;
            return hasSucceeded() ? newQuorum ? NewSuccessAndQuorum : Success
                                  : newQuorum ? NewFailAndQuorum : Fail;
        }

        public boolean hasSucceeded()
        {
            return waitingOnSuccess == 0;
        }

        public boolean hasQuorumSuccess()
        {
            return successes.size() >= shard.slowQuorumSize;
        }

        public boolean hasMinorityQuorumSuccess()
        {
            return successes.size() >= shard.minorityQuorumSize();
        }

        boolean hasFailed()
        {
            return waitingOn == 0 && !hasSucceeded();
        }

        @Override
        public String summarise()
        {
            if (exclude == null)
                return successes.size() + "/" + shard.rf;

            return successes.size() + "/" + (shard.rf - exclude.size()) + '(' + shard.rf + ')';
        }
    }

    final SortedListSet<Node.Id> successes;
    private int waitingOnQuorum;

    public DurabilityTracker(Topologies topologies)
    {
        super(topologies, DurabilityShardTracker[]::new, topologies.staleOrRemovedIds(), (p, i, s) -> new DurabilityShardTracker(p, s));
        successes = SortedListSet.noneOf(topologies.nodes());
        waitingOnQuorum = waitingOnShards;
    }

    public RequestStatus recordSuccess(Node.Id node)
    {
        successes.add(node);
        return recordResponse(this, node, DurabilityShardTracker::onSuccess, node);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, DurabilityShardTracker::onFailure, node);
    }

    public boolean hasFailed()
    {
        return any(DurabilityShardTracker::hasFailed);
    }

    public boolean hasSucceeded()
    {
        return all(DurabilityShardTracker::hasSucceeded);
    }

    public SyncRemote achievedRemote()
    {
        if (hasSucceeded())
            return SyncRemote.All;
        if (hasQuorumSuccess())
            return SyncRemote.Quorum;
        if (hasMinorityQuorumSuccess())
            return SyncRemote.MinorityQuorum;
        return SyncRemote.NoRemote;
    }

    public SyncLocal achievedLocal(Node.Id self)
    {
        return successes.contains(self) ? SyncLocal.Self : SyncLocal.NoLocal;
    }

    public Set<Node.Id> including()
    {
        return successes;
    }

    public Set<Node.Id> excluding()
    {
        return topologies.nodes().without(successes::contains);
    }

    public boolean hasQuorumSuccess()
    {
        Invariants.require((waitingOnQuorum == 0) == all(DurabilityShardTracker::hasQuorumSuccess));
        return waitingOnQuorum == 0;
    }

    public boolean hasMinorityQuorumSuccess()
    {
        return all(DurabilityShardTracker::hasMinorityQuorumSuccess);
    }
}
