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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.MessageSink;
import accord.impl.DefaultTimeouts;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.impl.IntKey.range;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;
import static accord.topology.SelectShards.ALL;
import static accord.topology.Topology.NO_IDS;

/**
 * When a later epoch hard removes replicas, every retained epoch is rewritten with the hard removal adjusted topology,
 * which lowers the quorum that epoch requires. Readiness of a retained epoch must be judged against that current
 * quorum; and a ready report received from a replica that has since been hard removed must not count towards it.
 *
 * A shard [0,100) is replicated on nodes 1..5 (rf=5, slow quorum 3). Epoch 3 hard removes nodes {3,4,5}, leaving
 * nodes 1 and 2 with a slow quorum of 2, so the retained epochs 1 and 2 (rewritten by the hard removal) must become
 * quorum ready once nodes 1 and 2 report.
 */
public class HardRemovedQuorumReadyTest
{
    private static final Node.Id ID = new Node.Id(1);
    private static final Range R = range(0, 100);
    private static final Ranges RANGES = Ranges.of(R);
    private static final SortedArrayList<Node.Id> NODES = idList(1, 2, 3, 4, 5);

    private static TopologyManager createTopologyManager()
    {
        MockCluster.Clock time = new MockCluster.Clock(0);
        Node node = createNode(ID, Topology.EMPTY, new MessageSink.NoOpSink(), time, new TestAgent(time), null);
        return new TopologyManager(SUPPLIER, node, ignore -> { throw new UnsupportedOperationException(); }, time, new DefaultTimeouts(time, Runnable::run));
    }

    // mirrors AccordTopology: the shard carries the hard removed replicas it contains, the topology carries them all
    private static Topology epoch(long epoch, SortedArrayList<Node.Id> hardRemoved)
    {
        Shard shard = Shard.create(R, NODES, idSet(1, 2, 3, 4, 5), hardRemoved.intersecting(NODES), Shard.NO_FLAGS);
        return new Topology(epoch, NO_IDS, hardRemoved, NO_IDS, shard);
    }

    private static TopologyManager epochs12ThenHardRemoval()
    {
        TopologyManager tm = createTopologyManager();
        tm.reportTopology(epoch(1, NO_IDS));
        tm.reportTopology(epoch(2, NO_IDS));
        // hard removing three of five replicas lowers the slow quorum from 3 to 2 in epochs 1 and 2 as well
        tm.reportTopology(epoch(3, idList(3, 4, 5)));
        for (long e = 1; e <= 3; e++)
        {
            Shard current = tm.unsafeGetActiveEpoch(e).all().shards[0];
            Assertions.assertEquals(2, current.slowQuorumSize, "epoch " + e + " current shard " + current.toString(true));
        }
        return tm;
    }

    /**
     * Nodes 1 and 2 (the only replicas that are neither hard removed nor down) report ready. That is the current
     * quorum, so the retained epoch 2 and the new epoch 3 must both become quorum ready.
     */
    @Test
    void retainedEpochReadinessUsesCurrentQuorum() throws TopologyException
    {
        TopologyManager tm = epochs12ThenHardRemoval();
        tm.onReadyToCoordinate(id(1), 3);
        tm.onReadyToCoordinate(id(2), 3);

        for (long e = 2; e <= 3; e++)
        {
            ActiveEpoch ae = tm.unsafeGetActiveEpoch(e);
            Assertions.assertEquals(2, ae.quorumReadyTracker.get(0).shard.slowQuorumSize,
                                    "epoch " + e + " readiness tracker shard " + ae.quorumReadyTracker.get(0).shard.toString(true));
            Assertions.assertTrue(ae.isQuorumReady(),
                                  "epoch " + e + " should be quorum ready with 2 of the 2 healthy replicas, quorumReady=" + ae.quorumReady());
            Assertions.assertTrue(ae.quorumReady().containsAll(RANGES), "epoch " + e + " quorumReady=" + ae.quorumReady());
        }

        // consequence: a coordinator at epoch 3 does not have to consult any older epoch
        Assertions.assertEquals(1, tm.active().withUnsyncedEpochs(RANGES, 3, 3, ALL).size());
    }

    /**
     * Node 5 reported ready for epoch 2 before it was hard removed, and afterwards only node 1 reports. If node 5's
     * stale report were replayed into the rebuilt tracker, epoch 2 would reach the current quorum of 2 on the strength
     * of a report from a replica that can never participate again.
     */
    @Test
    void hardRemovedReadyReportsAreNotReplayed() throws TopologyException
    {
        TopologyManager tm = createTopologyManager();
        tm.reportTopology(epoch(1, NO_IDS));
        tm.reportTopology(epoch(2, NO_IDS));
        tm.onReadyToCoordinate(id(5), 2);

        tm.reportTopology(epoch(3, idList(3, 4, 5)));
        tm.onReadyToCoordinate(id(1), 3);

        for (long e = 2; e <= 3; e++)
        {
            ActiveEpoch ae = tm.unsafeGetActiveEpoch(e);
            Assertions.assertEquals(2, ae.quorumReadyTracker.get(0).shard.slowQuorumSize,
                                    "epoch " + e + " readiness tracker still uses the pre-removal quorum: "
                                    + ae.quorumReadyTracker.get(0).shard.toString(true));
            Assertions.assertFalse(ae.isQuorumReady(),
                                   "epoch " + e + " considers itself quorum ready after a single report from a healthy replica,"
                                   + " so report(s) from hard removed replicas were counted; quorumReady=" + ae.quorumReady());
        }

        // the second healthy replica completes the current quorum
        tm.onReadyToCoordinate(id(2), 3);
        for (long e = 2; e <= 3; e++)
        {
            ActiveEpoch ae = tm.unsafeGetActiveEpoch(e);
            Assertions.assertTrue(ae.isQuorumReady(), "epoch " + e + " should be quorum ready once both healthy replicas report, quorumReady=" + ae.quorumReady());
        }
    }
}
