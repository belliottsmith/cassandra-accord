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

package accord.topology;

import com.google.common.annotations.VisibleForTesting;

import accord.api.TopologySorter;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;

public final class ActiveEpoch
{
    final Node.Id self;
    final Topology global, local;

    public final Ranges addedRanges, removedRanges;

    private final EpochReady epochReady;

    final QuorumTracker quorumReadyTracker;
    final SimpleBitSet shardQuorumReady, receivedNodeReady;
    private Ranges quorumReady;

    private Ranges closed = Ranges.EMPTY, retired = Ranges.EMPTY;
    private volatile boolean allRetired;

    public boolean allRetired()
    {
        if (allRetired)
            return true;

        if (!retired.containsAll(global.ranges))
            return false;

        Invariants.require(closed.containsAll(global.ranges));
        allRetired = true;
        return true;
    }

    ActiveEpoch(Node.Id self, Topology global, EpochReady epochReady, TopologySorter sorter, Ranges prevRanges)
    {
        this.self = self;
        this.global = Invariants.requireArgument(global, !global.isSubset());
        this.epochReady = epochReady;
        this.local = global.forNode(self).trim();
        this.shardQuorumReady = SimpleBitSet.allocate(global.shards.length);
        this.receivedNodeReady = SimpleBitSet.allocate(global.nodes.size());
        if (global().isEmpty()) this.quorumReadyTracker = null;
        else this.quorumReadyTracker = new QuorumTracker(new Topologies.Single(sorter, global()));

        this.addedRanges = global.ranges.without(prevRanges).mergeTouching();
        this.removedRanges = prevRanges.mergeTouching().without(global.ranges);
        this.quorumReady = addedRanges;
    }

    ActiveEpoch(Node.Id node, Topology global, SimpleBitSet shardQuorumReady, SimpleBitSet receivedNodeReady, QuorumTracker quorumReadyTracker, Ranges addedRanges, Ranges removedRanges, EpochReady epochReady, Ranges quorumReady, Ranges closed, Ranges retired)
    {
        this.self = node;
        this.global = Invariants.requireArgument(global, !global.isSubset());
        this.local = global.forNode(node).trim();
        this.shardQuorumReady = shardQuorumReady;
        this.receivedNodeReady = receivedNodeReady;
        this.quorumReadyTracker = quorumReadyTracker;
        this.addedRanges = addedRanges;
        this.removedRanges = removedRanges;
        this.epochReady = epochReady;
        this.quorumReady = quorumReady;
        this.closed = closed;
        this.retired = retired;
    }

    boolean isQuorumReady()
    {
        return quorumReadyTracker == null
               || quorumReadyTracker.hasReachedQuorum()
               || quorumReady.containsAll(global.ranges);
    }

    /**
     * determine if sync has completed for all shards intersecting with the given keys
     */
    boolean isQuorumReady(Unseekables<?> intersect)
    {
        return quorumReady.containsAll(intersect);
    }

    public Ranges quorumReady()
    {
        return quorumReady;
    }

    public Ranges closed()
    {
        return closed;
    }

    public Ranges retired()
    {
        return retired;
    }

    public EpochReady epochReady()
    {
        return epochReady;
    }

    boolean onReadyToCoordinate(Node.Id node)
    {
        if (quorumReadyTracker == null)
            return true;

        int index = global.nodes.indexOf(node);
        if (index < 0)
            return true;

        if (receivedNodeReady.get(index))
            return false;

        receivedNodeReady.set(index);
        if (quorumReadyTracker.recordSuccess(node) == Success)
        {
            quorumReady = global.ranges.mergeTouching();
        }
        else
        {
            // loop over each current shard, and test if its ranges are complete
            for (int i = 0; i < global.shards.length; ++i)
            {
                if (quorumReadyTracker.get(i).hasReachedQuorum() && !shardQuorumReady.get(i))
                {
                    quorumReady = quorumReady.union(MERGE_ADJACENT, Ranges.of(global.shards[i].range));
                    shardQuorumReady.set(i);
                }
            }
        }
        return true;
    }

    // returns those ranges that weren't already closed, so that they can be propagated to lower epochs
    Ranges recordClosed(Ranges ranges)
    {
        ranges = ranges.without(closed);
        if (ranges.isEmpty())
            return ranges;
        closed = closed.union(MERGE_ADJACENT, ranges);
        Invariants.require(closed.mergeTouching() == closed);
        return ranges.without(addedRanges);
    }

    // returns those ranges that weren't already retired, so that they can be propagated to lower epochs
    Ranges recordRetired(Ranges ranges)
    {
        ranges = ranges.without(retired);
        if (ranges.isEmpty())
            return ranges;
        quorumReady = quorumReady.union(MERGE_ADJACENT, ranges);
        closed = closed.union(MERGE_ADJACENT, ranges);
        retired = retired.union(MERGE_ADJACENT, ranges);
        Invariants.require(closed.mergeTouching() == closed);
        Invariants.require(retired.mergeTouching() == retired);
        return ranges.without(addedRanges);
    }

    public Topology global()
    {
        return global;
    }

    public Topology local()
    {
        return local;
    }

    public long epoch()
    {
        return global().epoch;
    }

    @Override
    public String toString()
    {
        return "EpochState{" +
               "epoch=" + global.epoch() +
               '}';
    }

    @VisibleForTesting
    public static ActiveEpoch unsafeNew(Node.Id self, Topology global, EpochReady epochReady, TopologySorter sorter, Ranges prevRanges)
    {
        return new ActiveEpoch(self, global, epochReady, sorter, prevRanges);
    }
}
