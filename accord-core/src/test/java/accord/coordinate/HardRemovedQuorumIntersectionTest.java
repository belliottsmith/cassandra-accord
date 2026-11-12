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

package accord.coordinate;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.mock.MockCluster;
import accord.local.Command;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.topology.Shard.NO_FLAGS;
import static accord.topology.Shard.NO_NODES;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;

/**
 * With rf=5 and two hard-removed replicas, Shard previously recomputed maxFailures = 3 and slowQuorumSize = 2.
 * A slow quorum of 2 healthy replicas does not intersect a quorum of 3 formed before the mark ({C,D,E}), so a conflicting transaction committed
 * before the mark is invisible to a transaction coordinated after it.
 */
public class HardRemovedQuorumIntersectionTest
{
    @Test
    void shardArithmetic()
    {
        Range range = range(100, 200);
        SortedArrayList<Node.Id> nodes = idList(1, 2, 3, 4, 5);
        Shard original = Shard.create(range, nodes, idSet(1, 2, 3, 4, 5), NO_NODES, NO_FLAGS);
        Shard marked = Shard.create(range, nodes, idSet(1, 2, 3, 4, 5), idList(4, 5), NO_FLAGS);
        // h=2 does not exceed absoluteMaxFailures (rf - (slow - 1) = 3), so per the code's own comment a
        // post-mark quorum must still intersect every pre-mark quorum: |pre| + |post| > rf
        Assertions.assertTrue(original.slowQuorumSize + marked.slowQuorumSize > original.rf,
                              "pre-mark slowQuorumSize=" + original.slowQuorumSize + " post-mark slowQuorumSize=" + marked.slowQuorumSize
                              + " maxFailures=" + marked.maxFailures + " rf=" + original.rf + ": quorums can be disjoint");
    }

    @Test
    void conflictingTxnAfterHardRemovedMissesPreMarkQuorum() throws Throwable
    {
        scenario(idList(4, 5));
    }

    // control: identical schedule with no hard removal; t1 must not commit on {A,B} alone (passes on current code)
    @Test
    void controlWithoutHardRemoved() throws Throwable
    {
        scenario(NO_NODES);
    }

    private static void scenario(SortedArrayList<Node.Id> hardRemoved) throws Throwable
    {
        Keys keys = keys(150);
        Range range = range(100, 200);
        SortedArrayList<Node.Id> nodes = idList(1, 2, 3, 4, 5);
        Set<Node.Id> electorate = idSet(1, 2, 3, 4, 5);
        Topology topology1 = new Topology(1, Shard.create(range, nodes, electorate, NO_NODES, NO_FLAGS));
        // as AccordTopology builds it after AccordMarkHardRemoved{D,E}: same replicas, D,E hard removed
        Topology topology2 = new Topology(2, Topology.NO_IDS, hardRemoved, Topology.NO_IDS,
                                          Shard.create(range, nodes, electorate, hardRemoved, NO_FLAGS));

        Set<Node.Id> groupAB = idSet(1, 2);
        // partition {A,B} from {C,D,E}: messages across the cut fail immediately (MockCluster.send)
        AtomicBoolean partition = new AtomicBoolean(true);
        try (MockCluster cluster = MockCluster.builder().nodes(5).topology(topology1).setFetchTopology(e -> e == 2 ? topology2 : null).build())
        {
            // discard iff partition active and endpoints on opposite sides of the cut
            cluster.networkFilter.addFilter(from -> partition.get() && groupAB.contains(from), to -> !groupAB.contains(to), msg -> true);
            cluster.networkFilter.addFilter(from -> partition.get() && !groupAB.contains(from), groupAB::contains, msg -> true);

            // t2: coordinated by C in epoch 1, slow quorum 3 = {C,D,E}; A and B never see it
            Node nodeC = cluster.get(3);
            TxnId t2 = nodeC.nextTxnIdWithDefaultFlags(keys, Write, Key);
            Txn txn2 = writeTxn(keys);
            getUninterruptibly(nodeC.coordinate(t2, txn2), 10, TimeUnit.SECONDS);
            SaveStatus t2OnC = status(nodeC, t2, 1);
            Assertions.assertTrue(t2OnC.hasBeen(accord.primitives.Status.Committed), "t2 on C: " + t2OnC);

            // AccordMarkHardRemoved{D,E} -> epoch 2; TopologyManager rewrites every active epoch
            for (Node n : cluster)
                n.topology().reportTopology(topology2);
            for (Node n : cluster)
                getUninterruptibly(n.topology().await(2, null), 10, TimeUnit.SECONDS);

            // t1: same key, coordinated by A in epoch 2; only A and B are reachable (D,E are hard removed, C is slow/partitioned)
            Node nodeA = cluster.get(1);
            TxnId t1 = nodeA.nextTxnIdWithDefaultFlags(keys, Write, Key);
            Assertions.assertEquals(2, t1.epoch());
            Assertions.assertTrue(t1.compareTo(t2) > 0);
            Txn txn1 = writeTxn(keys);
            Throwable t1Failure = null;
            try
            {
                getUninterruptibly(nodeA.coordinate(t1, txn1), 10, TimeUnit.SECONDS);
            }
            catch (Throwable t)
            {
                t1Failure = t;
            }
            SaveStatus t1OnA = status(nodeA, t1, 2);
            Deps t1Deps = deps(nodeA, t1, 2);
            // intended: either t1 cannot reach a quorum without C, or (if it commits) it depends on the earlier conflicting t2
            if (t1Failure == null || t1OnA.hasBeen(accord.primitives.Status.Committed))
            {
                Assertions.assertTrue(t1Deps != null && t1Deps.contains(t2),
                                      "t1=" + t1 + " committed (" + t1OnA + ") on {A,B} after hardRemoved{D,E} with deps " + t1Deps
                                      + " that omit conflicting t2=" + t2 + " committed earlier on {C,D,E} (" + t2OnC + "); coordinate outcome="
                                      + (t1Failure == null ? "success" : t1Failure.toString()) + "; shard=" + topology2.get(0).slowQuorumSize + "/" + topology2.get(0).maxFailures);
            }
        }
    }

    private static SaveStatus status(Node node, TxnId txnId, long epoch) throws Throwable
    {
        AtomicReference<SaveStatus> out = new AtomicReference<>();
        getUninterruptibly(node.commandStores().forEach("Test", txnId, keys(150).toParticipants(), epoch, epoch, safeStore -> {
            StoreParticipants participants = StoreParticipants.update(safeStore, keys(150).toParticipants(), txnId.epoch(), txnId, txnId.epoch());
            Command command = safeStore.get(txnId, participants).current();
            out.set(command.saveStatus());
        }), 10, TimeUnit.SECONDS);
        return out.get();
    }

    private static Deps deps(Node node, TxnId txnId, long epoch) throws Throwable
    {
        AtomicReference<Deps> out = new AtomicReference<>();
        getUninterruptibly(node.commandStores().forEach("Test", txnId, keys(150).toParticipants(), epoch, epoch, safeStore -> {
            StoreParticipants participants = StoreParticipants.update(safeStore, keys(150).toParticipants(), txnId.epoch(), txnId, txnId.epoch());
            Command command = safeStore.get(txnId, participants).current();
            if (command.partialDeps() != null)
                out.set(command.partialDeps());
        }), 10, TimeUnit.SECONDS);
        return out.get();
    }
}
