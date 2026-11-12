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

package accord.local;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.coordinate.CoordinateMaxConflict;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockTopologyService;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.topology.Topology;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChainUtils;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topology;
import static accord.impl.IntKey.range;

/**
 * Max conflicts must be merged taking the max epoch and max hlc independently.
 *
 * Replica stores merge conflicts componentwise (MaxConflicts.update / Entry.reduce use Timestamp.mergeMax,
 * "essential for propagating unique HLCs, so that bootstrap recipients don't begin serving reads too early"),
 * but GetMaxConflict.reduce (across a replica's command stores) and CoordinateMaxConflict.onSuccessInternal
 * (across replicas) merge with epoch-major Timestamp.max. When an applied conflict in a later epoch carries a
 * lower hlc than an applied conflict in an earlier epoch (coordinator clock skew across an epoch change), the
 * global max conflict returned to rebootstrap (CommandStore.rebootstrap -> node.uniqueNow(success.hlc()))
 * carries an hlc below an applied conflict's hlc.
 *
 * Conflicts are seeded with CommandStore.updateMaxConflicts(Ranges, Timestamp), the same MaxConflicts.update
 * that the apply path (updateMaxConflicts(Command, Command, boolean)) performs, so the stores hold exactly the
 * state two applied writes on different keys would leave.
 */
public class MaxConflictEpochMajorMergeTest
{
    private static final Range RANGE = range(100, 200);

    private static <V> V get(AsyncChain<V> chain)
    {
        try
        {
            return AsyncChainUtils.getUninterruptibly(chain, 10, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            throw new AssertionError(t);
        }
    }

    private static MockCluster epoch2Cluster()
    {
        Topology topology1 = topology(1, shard(RANGE, idList(1, 2, 3), idSet(1, 2, 3)));
        Topology topology2 = topology(2, shard(RANGE, idList(1, 2, 3), idSet(1, 2, 3)));
        MockCluster cluster = MockCluster.builder().nodes(3).topology(topology1).build();
        cluster.nodes(1, 2, 3).forEach(node -> node.topology().reportTopology(topology2));
        cluster.nodes(1, 2, 3).forEach(node -> {
            get(node.topology().await(2, null));
            MockTopologyService ts = (MockTopologyService) node.topology().topologyService();
            get(ts.ackFor(2).coordinate.chain());
        });
        return cluster;
    }

    private static CommandStore[] storesByRange(Node node)
    {
        CommandStore[] stores = node.commandStores().all().clone();
        Arrays.sort(stores, (x, y) -> x.unsafeGetRangesForEpoch().all().get(0).compareTo(y.unsafeGetRangesForEpoch().all().get(0)));
        return stores;
    }

    private static void seed(Node node, CommandStore store, Timestamp conflict)
    {
        get(store.chain((ExecutionContext.Empty) () -> "seed max conflict", safeStore -> {
            store.updateMaxConflicts(store.unsafeGetRangesForEpoch().all(), conflict);
        }));
    }

    private static Timestamp localStoreMax(CommandStore store)
    {
        return get(store.chain((ExecutionContext.Empty) () -> "read max conflict", safeStore -> {
            return store.maxConflict(accord.primitives.TxnId.NONE, Ranges.of(RANGE));
        }));
    }

    /** Two conflicts applied on the same replicas, in different command stores. */
    @Test
    void acrossCommandStores()
    {
        try (MockCluster cluster = epoch2Cluster())
        {
            long now = System.currentTimeMillis();
            // write A: epoch 1, coordinator clock one hour ahead; write B: epoch 2, coordinator clock at now
            Timestamp a = Timestamp.fromValues(1, now + 3_600_000L, id(1));
            Timestamp b = Timestamp.fromValues(2, now, id(2));
            for (Node node : cluster.nodes(1, 2, 3))
            {
                CommandStore[] stores = storesByRange(node);
                Assertions.assertTrue(stores.length >= 2, "need two command stores, have " + stores.length);
                seed(node, stores[0], a);
                seed(node, stores[1], b);
            }

            Timestamp result = get(CoordinateMaxConflict.maxConflict(cluster.get(1), Ranges.of(RANGE)));
            Assertions.assertTrue(result.hlc() >= a.hlc(),
                                  "CoordinateMaxConflict returned " + result + " (hlc " + result.hlc() + ") below applied conflict "
                                  + a + " (hlc " + a.hlc() + "); other conflict " + b);
        }
    }

    /** Variant: one store per conflict set, conflicts on different replica quorums, quorum {n1,n3} answers. */
    @Test
    void acrossReplicas()
    {
        try (MockCluster cluster = epoch2Cluster())
        {
            long now = System.currentTimeMillis();
            Timestamp a = Timestamp.fromValues(1, now + 3_600_000L, id(1)); // applied on {n1, n2}
            Timestamp b = Timestamp.fromValues(2, now, id(3));              // applied on {n2, n3}
            for (CommandStore cs : cluster.get(1).commandStores().all()) seed(cluster.get(1), cs, a);
            for (CommandStore cs : cluster.get(2).commandStores().all()) { seed(cluster.get(2), cs, a); seed(cluster.get(2), cs, b); }
            for (CommandStore cs : cluster.get(3).commandStores().all()) seed(cluster.get(3), cs, b);

            // control: replica-side componentwise merge keeps the max hlc
            Timestamp n2 = localStoreMax(cluster.get(2).commandStores().all()[0]);
            Assertions.assertTrue(n2.hlc() >= a.hlc() && n2.epoch() >= 2, "replica-side mergeMax control failed: " + n2);

            // n2 is slow/unreachable: the quorum {n1, n3} answers
            cluster.networkFilter.isolate(id(2));
            Timestamp result = get(CoordinateMaxConflict.maxConflict(cluster.get(1), Ranges.of(RANGE)));
            Assertions.assertTrue(result.hlc() >= a.hlc(),
                                  "CoordinateMaxConflict returned " + result + " (hlc " + result.hlc() + ") below conflict "
                                  + a + " (hlc " + a.hlc() + ") applied on quorum {n1,n2}; replica n2 itself holds " + n2);
        }
    }
}
