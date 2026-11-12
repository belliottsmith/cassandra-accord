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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.coordinate.CoordinateMaxConflict;
import accord.impl.mock.MockCluster;
import accord.messages.GetMaxConflict;
import accord.messages.Request;
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
 * {@link CoordinateMaxConflict} calculates the maximum TxnId that could have been agreed before it started, so it must
 * cover transactions agreed in any epoch a replica tells it about. If a reply reports a newer epoch than the one
 * queried, a transaction may have been agreed by a quorum of that epoch which does not intersect the quorum that
 * answered, so the replicas of the newer epoch must be queried as well.
 *
 * Epoch 1 replicates the range on {1,2,3} and epoch 2 on {2,3,4}. Node 1 stays on epoch 1 and so queries the epoch 1
 * quorum; node 2's request is lost, leaving {1,3}. A conflict has been applied at node 4 in epoch 2, which the epoch 1
 * quorum cannot know about, but node 3's reply tells node 1 that epoch 2 has been established.
 *
 * Conflicts are seeded with CommandStore.updateMaxConflicts(Ranges, Timestamp), the same MaxConflicts.update that the
 * apply path performs, so the stores hold exactly the state an applied write would leave.
 */
public class MaxConflictNewEpochTest
{
    private static final Range RANGE = range(100, 200);

    private static <V> V get(AsyncChain<V> chain)
    {
        try
        {
            return AsyncChainUtils.getUninterruptibly(chain, 20, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            throw new AssertionError(t);
        }
    }

    private static void seed(Node node, Timestamp conflict)
    {
        for (CommandStore store : node.commandStores().all())
        {
            get(store.chain((ExecutionContext.Empty) () -> "seed max conflict", safeStore -> {
                store.updateMaxConflicts(store.unsafeGetRangesForEpoch().all(), conflict);
                return null;
            }));
        }
    }

    @Test
    void queriesNewerEpochReportedByReplicas()
    {
        Topology topology1 = topology(1, shard(RANGE, idList(1, 2, 3), idSet(1, 2, 3)));
        Topology topology2 = topology(2, shard(RANGE, idList(2, 3, 4), idSet(2, 3, 4)));
        try (MockCluster cluster = MockCluster.builder()
                                              .nodes(4)
                                              .topology(topology1)
                                              .setFetchTopology(epoch -> epoch == 2 ? topology2 : null)
                                              .build())
        {
            // node 1 lags behind: no peer manages to deliver it a request, so it never hears about epoch 2
            cluster.networkFilter.addFilter(from -> !from.equals(id(1)), to -> to.equals(id(1)), m -> m instanceof Request);

            // nodes 2, 3 and 4 learn about epoch 2, so transactions there need no longer consult epoch 1
            cluster.nodes(2, 3, 4).forEach(node -> node.topology().reportTopology(topology2));
            for (Node node : cluster.nodes(2, 3, 4))
                get(node.topology().await(2, null));
            for (Node node : cluster.nodes(2, 3, 4))
                for (int ready : new int[]{ 2, 3, 4 })
                    node.topology().onReadyToCoordinate(id(ready), 2);

            // a conflict has been applied in epoch 2 at node 4, which is not a replica in epoch 1
            long now = System.currentTimeMillis();
            Timestamp conflict = Timestamp.fromValues(2, now + TimeUnit.HOURS.toMillis(1), id(4));
            seed(cluster.get(4), conflict);

            // node 1 (still on epoch 1) computes the max conflict; node 2's request is lost, so only {1,3} answer
            Node node1 = cluster.get(1);
            Assertions.assertEquals(1, node1.epoch());
            cluster.networkFilter.addFilter(from -> from.equals(id(1)), to -> to.equals(id(2)), m -> m instanceof GetMaxConflict);
            Timestamp result = get(CoordinateMaxConflict.maxConflict(node1, Ranges.of(RANGE)));

            // node 1 does learn from the replies that epoch 2 has been established
            Assertions.assertTrue(node1.epoch() >= 2, "node1 should have awaited epoch 2, epoch=" + node1.epoch());
            Assertions.assertTrue(result.compareTo(conflict) >= 0,
                                  "CoordinateMaxConflict returned " + result + " which is below " + conflict
                                  + ", a conflict applied in epoch 2 at replicas the epoch 1 quorum does not cover");
        }
    }
}
