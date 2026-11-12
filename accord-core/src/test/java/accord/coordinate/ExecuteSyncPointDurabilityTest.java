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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.AbstractRequest;
import accord.messages.SetShardDurable;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.NetworkFilter.anyId;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.keys;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sync point that is applied by a quorum, with the remaining replica failing, completes with
 * {@link SyncRemote#QuorumAndWaitedForAll}. This is still quorum durability, so every replica must be told the shard
 * is durable at a quorum (SetShardDurable), including the replica that failed to apply the sync point.
 */
public class ExecuteSyncPointDurabilityTest
{
    @Test
    void quorumDurabilityIsReportedWhenAReplicaFails() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            // no background durability rounds, so that only the round under test can report durability
            for (int i = 1 ; i <= 3 ; ++i)
                cluster.get(i).durability().shards().setShardCycleTime(5, TimeUnit.MINUTES);

            Node coordinator = cluster.get(1);
            Keys keys = keys(10);
            TxnId txnId = coordinator.nextTxnIdWithDefaultFlags(keys, Txn.Kind.Write, Key);
            getUninterruptibly(coordinator.coordinate(txnId, writeTxn(keys)), 10, TimeUnit.SECONDS);

            // node 3 receives none of the sync point's messages, except SetShardDurable
            cluster.networkFilter.addFilter(anyId(), to -> to.equals(id(3)),
                                            msg -> msg instanceof AbstractRequest && !(msg instanceof SetShardDurable)
                                                   && ((AbstractRequest<?, ?>) msg).txnId.is(Txn.Kind.ExclusiveSyncPoint));

            Ranges ranges = coordinator.topology().current().ranges();
            SyncPoint syncPoint = getUninterruptibly(CoordinateSyncPoint.exclusive(coordinator, ranges), 10, TimeUnit.SECONDS);
            DurabilityResult result = getUninterruptibly(ExecuteSyncPoint.coordinate(coordinator, syncPoint, 1).onDone(), 15, TimeUnit.SECONDS);
            assertEquals(SyncRemote.QuorumAndWaitedForAll, result.min.remote, "precondition: " + result);

            RoutingKeys participants = keys.toParticipants();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            for (int i = 1 ; i <= 3 ; ++i)
            {
                Node node = cluster.get(i);
                while (node.durableBefore().min(txnId, participants).compareTo(Quorum) < 0 && System.nanoTime() < deadline)
                    Thread.sleep(10);
                assertTrue(node.durableBefore().min(txnId, participants).compareTo(Quorum) >= 0,
                           "node " + i + " was not told " + txnId + " is durable at a quorum: " + node.durableBefore());
            }
        }
    }
}
