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

import accord.burn.TopologyUpdates;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TestAgent;
import accord.impl.TopologyUtils;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.local.AgentExecutor;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.utils.RandomSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.RoutingKeys;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topologies;
import static accord.Utils.topology;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;
import static accord.utils.ExtendedAssertions.assertThat;
import static accord.utils.Property.qt;

public class TopologyManagerTest
{
    private static final Node.Id ID = new Node.Id(1);

    @Test
    void fastPathReconfiguration()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        service.onEpochSyncComplete(id(1), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
    }

    private static TopologyManager tracker()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(3, 4)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        return service;
    }

    @Test
    void syncCompleteFor()
    {
        TopologyManager service = tracker();

        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncCompleteFor(keys(150).toParticipants()));
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncCompleteFor(keys(250).toParticipants()));
    }

    /**
     * Epochs should only report being synced if every preceding epoch is also reporting synced
     */
    @Test
    void existingEpochPendingSync()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(1, 2)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(3).syncComplete());

        // sync epoch 3
        service.onEpochSyncComplete(id(1), 3);
        service.onEpochSyncComplete(id(2), 3);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(3).syncComplete());

        // sync epoch 2
        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(3).syncComplete());
    }

    /**
     * If a node receives sync acks for epochs it's not aware of, it should apply them when it finds out about the epoch
     */
    @Test
    void futureEpochPendingSync()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
//        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(3, 4)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);

        // sync epoch 2
        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);

        // learn of epoch 2
        service.onTopologyUpdate(topology2);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
//        Assertions.assertTrue(service.getEpochStateUnsafe(3).syncComplete());
    }

    @Test
    void forKeys()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        RoutingKeys keys = keys(150).toParticipants();
        Assertions.assertEquals(topologies(topology3.forSelection(keys), topology2.forSelection(keys), topology1.forSelection(keys)),
                                service.withUnsyncedEpochs(keys, 3, 3));

        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);
        service.onEpochSyncComplete(id(2), 3);
        service.onEpochSyncComplete(id(3), 3);
        Assertions.assertEquals(topologies(topology3.forSelection(keys)),
                                service.withUnsyncedEpochs(keys, 3, 3));
    }

    /**
     * Previous epoch topologies should only be included if they haven't been acknowledged, even
     * if the previous epoch is awaiting acknowledgement from all nodes
     */
    @Test
    void forKeysPartiallySynced()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology3 = topology(3,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);

        // no acks, so all epoch 1 shards should be included
        Assertions.assertEquals(topologies(topology2, topology1),
                                service.withUnsyncedEpochs(keys(150, 250).toParticipants(), 2, 2));

        // first topology acked, so only the second shard should be included
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochSyncComplete(id(2), 2);
        Topologies actual = service.withUnsyncedEpochs(keys(150, 250).toParticipants(), 2, 2);
        Assertions.assertEquals(topologies(topology2, topology(1, shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)))),
                                actual);
    }

    @Test
    void incompleteTopologyHistory()
    {
        Topology topology5 = topology(5,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology6 = topology(6,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology5);
        service.onTopologyUpdate(topology6);

        Assertions.assertSame(topology6, service.getEpochStateUnsafe(6).global());
        Assertions.assertSame(topology5, service.getEpochStateUnsafe(5).global());
        for (int i=1; i<=6; i++) service.onEpochSyncComplete(id(i), 6);
        Assertions.assertTrue(service.getEpochStateUnsafe(5).syncComplete());
        Assertions.assertNull(service.getEpochStateUnsafe(4));

        service.onEpochSyncComplete(id(1), 4);
    }

    private static void markTopologySynced(TopologyManager service, long epoch)
    {
        service.getEpochStateUnsafe(epoch).global().nodes().forEach(id -> service.onEpochSyncComplete(id, epoch));
    }

    private static void addAndMarkSynced(TopologyManager service, Topology topology)
    {
        service.onTopologyUpdate(topology);
        markTopologySynced(service, topology.epoch());
    }

    @Test
    void truncateTopologyHistory()
    {
        Range range = range(100, 200);
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        addAndMarkSynced(service, topology(1, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(2, shard(range, idList(1, 2, 3), idSet(2, 3))));
        addAndMarkSynced(service, topology(3, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(4, shard(range, idList(1, 2, 3), idSet(1, 3))));

        Assertions.assertTrue(service.hasEpoch(1));
        Assertions.assertTrue(service.hasEpoch(2));
        Assertions.assertTrue(service.hasEpoch(3));
        Assertions.assertTrue(service.hasEpoch(4));

        service.truncateTopologyUntil(3);
        Assertions.assertFalse(service.hasEpoch(1));
        Assertions.assertFalse(service.hasEpoch(2));
        Assertions.assertTrue(service.hasEpoch(3));
        Assertions.assertTrue(service.hasEpoch(4));

    }

    @Test
    void truncateTopologyCantTruncateUnsyncedEpochs()
    {

    }

    @Test
    void removeRanges()
    {
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        addAndMarkSynced(service, topology(1, shard(range(0, 200), idList(1, 2, 3), idSet(1, 2))));
        Topology t2 = topology(2, shard(range(0, 200), idList(1, 2, 3), idSet(1, 2)));
        Topology t3 = topology(3, shard(range(201, 400), idList(1, 2, 3), idSet(1, 2)));

        service.onTopologyUpdate(t2);
        service.onTopologyUpdate(t3);
        markTopologySynced(service, t2.epoch());
        markTopologySynced(service, t3.epoch());
    }

    /**
     * The ABA problem is a problem with registers where you set the value A, then B, then A again; when you observe you see A... which A?
     */
    @Test
    void aba()
    {
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        List<Node.Id> dc1Nodes = idList(1, 2, 3);
        Set<Node.Id> dc1Fp = idSet(1, 2);
        List<Node.Id> dc2Nodes = idList(4, 5, 6);
        Set<Node.Id> dc2Fp = idSet(4, 5);
        addAndMarkSynced(service, topology(1,
                shard(PrefixedIntHashKey.range(0, 0, 100), dc2Nodes, dc2Fp),
                shard(PrefixedIntHashKey.range(1, 0, 100), dc1Nodes, dc1Fp)));
        addAndMarkSynced(service, topology(2,
                shard(PrefixedIntHashKey.range(1, 0, 100), dc1Nodes, dc1Fp)));
        addAndMarkSynced(service, topology(3,
                shard(PrefixedIntHashKey.range(0, 0, 100), dc2Nodes, dc2Fp),
                shard(PrefixedIntHashKey.range(1, 0, 100), dc1Nodes, dc1Fp)));

        // prefix=0 was added in epoch=1, removed in epoch=2, and added back to epoch=3
        qt().withSeed(-264748563329313076L).check(rs -> checkAPI(service, rs));
    }

    @Test
    void fuzz()
    {
        // seeds to add back: 645630353491283432L
        qt().withExamples(Integer.MAX_VALUE).check(rand -> {
            PendingQueue queue = new RandomDelayQueue.Factory(rand).get();
            AgentExecutor executor = new SimulatedDelayedExecutorService(queue, new TestAgent.RethrowAgent(), rand);
            int numNodes = rand.nextInt(3, 20);
            List<Node.Id> nodes = new ArrayList<>(numNodes);
            Range[] ranges = PrefixedIntHashKey.ranges(0, numNodes);
            for (int i = 0; i < numNodes; i++)
                nodes.add(new Node.Id(i + 1));
            Topology first = TopologyUtils.initialTopology(nodes, Ranges.of(ranges), 3);
            TopologyRandomizer randomizer = new TopologyRandomizer(() -> rand, first, new TopologyUpdates(executor), null);
            TopologyManager service = new TopologyManager(SUPPLIER, ID);
            long lastFullAck = 0;
            for (int i = 0; i < 100; i++)
            {
                Topology current;
                {
                    Topology t = randomizer.updateTopology();
                    for (int attempt = 0; t == null && attempt < TopologyRandomizer.UpdateType.values().length * 2; attempt++)
                        t = randomizer.updateTopology();
                    // if no new updates can happen, stop early
                    if (t == null)
                        return;
                    current = t;
                }
                service.onTopologyUpdate(current);
                if (rand.decide(.2))
                {
                    markTopologySynced(service, current.epoch());
                    lastFullAck = current.epoch();
                }
                else if (i != 0 && lastFullAck + 1 < current.epoch() && rand.decide(.5))
                {
                    long epoch = rand.nextLong(lastFullAck + 1, current.epoch());
                    markTopologySynced(service, epoch);
                    lastFullAck = epoch;
                }
                else
                {
                    TopologyManager.EpochState state = service.getEpochStateUnsafe(current.epoch());
                    state.global().nodes().forEach(id -> {
                        if (rand.decide(.5))
                            service.onEpochSyncComplete(id, current.epoch());
                    });
                    if (state.syncComplete())
                        lastFullAck = current.epoch();
                }

                checkAPI(service, rand);
            }
        });
    }

    private static void checkAPI(TopologyManager service, RandomSource rand)
    {
        checkWithUnsyncedEpochs(service, rand);
        checkPreciseEpochs(service, rand);
    }

    private static void checkPreciseEpochs(TopologyManager service, RandomSource rand)
    {
        EpochRange range = EpochRange.from(service, rand);
        Unseekables<?> select = select(service, range, rand);
        Topologies topologies = service.preciseEpochs(select, range.min, range.max);
        assertThat(topologies)
                .isNotEmpty()
                .hasEpochsBetween(range.min, range.max)
                .containsAll(select)
                .containsAll(service, select);
    }

    private static void checkWithUnsyncedEpochs(TopologyManager service, RandomSource rand)
    {
//        EpochRange range = EpochRange.from(service, rand);
//        Unseekables<?> select = select(service, range, rand);
//        Topologies topologies = service.withUnsyncedEpochs(select, range.min, range.max);
    }

    private static Unseekables<?> select(TopologyManager service, EpochRange range, RandomSource rand)
    {
        List<Range> selected = new ArrayList<>();
        for (long epoch = range.min; epoch <= range.max; epoch++)
        {
            Topology topology = service.globalForEpoch(epoch);
            Ranges ranges = topology.ranges();
            int selectionCount = rand.nextInt(10);
            for (int i = 0; i < selectionCount; i++)
                // may have duplicates; they will be merged later
                selected.add(ranges.get(rand.nextInt(ranges.size())));
        }
        return Ranges.of(selected.toArray(new Range[selected.size()])).mergeTouching();
    }

    private static class EpochRange
    {
        final long min, max;

        private EpochRange(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        static EpochRange from(TopologyManager service, RandomSource rand)
        {
            long min = rand.nextLong(service.minEpoch(), service.epoch() + 1);
            long max = rand.nextLong(service.minEpoch(), service.epoch() + 1);
            if (min > max)
            {
                long tmp = max;
                max = min;
                min = tmp;
            }
            return new EpochRange(min, max);
        }
    }
}
