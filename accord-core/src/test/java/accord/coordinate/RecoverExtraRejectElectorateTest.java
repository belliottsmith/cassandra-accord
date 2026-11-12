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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockTopologyService;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.AbstractRequest;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.BeginRecovery;
import accord.messages.Message;
import accord.messages.PreAccept;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.ProgressToken;
import accord.primitives.Range;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.spinUntilSuccess;
import static accord.Utils.topology;
import static accord.Utils.writeTxn;
import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;

/**
 * Recovery must only infer a fast-path reject from a later privileged coordinator that is a fast-path elector in our epoch.
 *
 * Epoch 1: shard [100,200) replicas {1..5}, fast-path electorate {1,2,3,4} (simple fast quorum 4).
 * Epoch 2: same replicas, electorate {1..5}.
 *
 * T (epoch 1, Unoptimised, coordinated by 6, which is not a replica of T's key) PreAccepts at electors 1..4, all vote fast,
 * and 6 executes T on the fast path at txnId (no Accept round) and returns the result to the client; only
 * replica 4 receives the execution messages and applies T. Node 5 (not an epoch-1 elector) never sees T.
 * Epoch 2 arrives; replicas 1-4 finish their electorate-change visibility sync and report ready, node 5's sync
 * is still pending, so epoch 2 is quorum-ready and node 5 coordinates T' (PrivilegedCoordinatorWithDeps) on the same key
 * without T in its coordinator deps; T' reaches replica 1 only. Recovery of T with quorum {1,2,3}
 * sees three fast votes, and replica 1 reports T' as a superseding coordinator reject. Recover counts
 * that as an extra fast-path reject for the epoch-1 shard (RecoveryTracker.inferFastPathDecision)
 * although node 5 is not in T's epoch-1 electorate, and invalidates the fast-committed T.
 */
public class RecoverExtraRejectElectorateTest
{
    private static Command command(Node node, RoutingKey key, TxnId txnId)
    {
        CommandStore store = node.unsafeForKey(key);
        if (!inMemory(store).hasCommand(txnId))
            return null;
        return inMemory(store).command(txnId).value();
    }

    private static TxnId txnIdOf(Message m)
    {
        return m instanceof AbstractRequest ? ((AbstractRequest<?, ?>) m).txnId : null;
    }

    @Test
    void recoveryMustNotInvalidateFastCommitAfterElectorateChange() throws Throwable
    {
        Keys keys = keys(150);
        RoutingKey key = keys.get(0).toUnseekable();
        Range range = range(100, 200);
        // node 6 replicates an unrelated range only (so it has command stores to coordinate from),
        // and is not a replica of T's key, so T is Unoptimised
        Range other = range(200, 300);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3, 4, 5), idSet(1, 2, 3, 4)),
                                         shard(other, idList(1, 2, 6), idSet(1, 2, 6)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3, 4, 5), idSet(1, 2, 3, 4, 5)),
                                         shard(other, idList(1, 2, 6), idSet(1, 2, 6)));

        Id n1 = id(1), n4 = id(4), n5 = id(5), n6 = id(6);
        try (MockCluster cluster = MockCluster.builder()
                                              .nodes(6)
                                              .topology(topology1)
                                              .setFetchTopology(e -> e == 2 ? topology2 : null)
                                              .build())
        {
            // stop background shard-durability sync points so they do not spread T to node 5 before T'
            // is coordinated (in production they run on a much longer period than in this mock cluster)
            for (Node node : cluster)
                node.durability().stop();

            Node coordT = cluster.get(6);
            TxnId t = coordT.nextTxnIdWithDefaultFlags(keys, Write, Key);
            Txn txnT = writeTxn(keys);
            Assertions.assertEquals(1, t.epoch());
            Assertions.assertEquals(Unoptimised, t.fastPath());

            // phase 1: T. Node 5 sees nothing about T; T's coordinator delivers only PreAccept to 1..4 and
            // any post-decision message only to node 4 (so 1..3 stay PreAccepted, as if 6 crashed mid-commit).
            // Throughout, replicas do not send requests about T (no autonomous progress-log recovery), and
            // nothing about T reaches node 5. In phase 4 only 6 -> {1,2,3} is open for T.
            AtomicInteger phase = new AtomicInteger(1);
            AtomicBoolean fastDecided = new AtomicBoolean(), acceptSent = new AtomicBoolean();
            cluster.networkFilter.addFilter(from -> true, to -> to.equals(n5), m -> t.equals(txnIdOf(m)));
            cluster.networkFilter.addFilter(from -> !from.equals(n6), to -> true, m -> t.equals(txnIdOf(m)));
            cluster.networkFilter.addFilter(from -> from.equals(n6), to -> true, m -> {
                if (!t.equals(txnIdOf(m)))
                    return false;
                // T executes at txnId with no Accept round: the coordinator took the fast path
                if (m instanceof Accept)
                    acceptSent.set(true);
                if (m instanceof Apply && t.equals(((Apply) m).executeAt))
                    fastDecided.set(true);
                return false;
            });
            cluster.networkFilter.addFilter(from -> from.equals(n6), to -> !to.equals(n4),
                                            m -> phase.get() == 1 && t.equals(txnIdOf(m)) && !(m instanceof PreAccept));
            cluster.networkFilter.addFilter(from -> from.equals(n6), to -> to.equals(n4),
                                            m -> phase.get() == 4 && t.equals(txnIdOf(m)));

            // AsyncChain is lazy: begin it; its outcome is irrelevant (6 never completes the commit)
            AtomicReference<Throwable> tFailure = new AtomicReference<>();
            AtomicReference<Object> tResult = new AtomicReference<>();
            coordT.coordinate(t, txnT).begin((r, f) -> { tFailure.set(f); tResult.set(r); });
            spinUntilSuccess(() -> {
                Assertions.assertTrue(fastDecided.get(), "T's coordinator did not decide the fast path; failure=" + tFailure.get());
                Command c4 = command(cluster.get(n4), key, t);
                Assertions.assertNotNull(c4);
                Assertions.assertTrue(c4.hasBeen(Status.Applied), "replica 4: " + c4.saveStatus());
                for (int i = 1 ; i <= 3 ; ++i)
                {
                    Command c = command(cluster.get(i), key, t);
                    Assertions.assertNotNull(c, "replica " + i + " has no T");
                    Assertions.assertEquals(Status.PreAccepted, c.status());
                    Assertions.assertEquals(t, c.executeAt(), "replica " + i + " did not vote fast");
                }
            }, 10);
            Assertions.assertFalse(acceptSent.get(), "T's coordinator ran an Accept round");
            Assertions.assertEquals(t, command(cluster.get(n4), key, t).executeAt());
            Assertions.assertNull(command(cluster.get(n5), key, t));

            // phase 2: epoch 2 (electorate now includes 5). The electorate change requires each replica to run a
            // visibility sync point before it is ready to coordinate epoch 2 (CommandStores.requiresSync,
            // CommandStore.refreshReadyToCoordinate). Node 5's sync is slow (its sync-point traffic is lost), so it
            // has not yet witnessed T; the other replicas complete theirs and report ready, so epoch 2 is
            // quorum-ready at node 5 and node 5 may allocate privileged TxnIds in epoch 2 (ActiveEpochs.selectFastPath).
            cluster.networkFilter.addFilter(from -> from.equals(n5), to -> true, m -> { TxnId id = txnIdOf(m); return id != null && id.isSyncPoint(); });
            cluster.networkFilter.addFilter(from -> true, to -> to.equals(n5), m -> { TxnId id = txnIdOf(m); return id != null && id.isSyncPoint(); });
            for (Node node : cluster)
                node.topology().reportTopology(topology2);
            Node coordTp = cluster.get(5);
            getUninterruptibly(coordTp.topology().await(2, null), 10, TimeUnit.SECONDS);
            for (Node node : cluster)
            {
                if (node.id().equals(n5)) continue;
                getUninterruptibly(node.topology().await(2, null), 10, TimeUnit.SECONDS);
                MockTopologyService ts = (MockTopologyService) node.topology().topologyService();
                getUninterruptibly(ts.ackFor(2).coordinate, 10, TimeUnit.SECONDS);
            }
            Assertions.assertFalse(((MockTopologyService) coordTp.topology().topologyService()).ackFor(2).coordinate.isDone(),
                                   "node 5 completed its epoch 2 visibility sync");
            // as the topology service does in production (and burn's TopologyUpdates), deliver the readiness of
            // every node that is ready to coordinate epoch 2
            for (Node node : cluster)
                for (Node peer : cluster)
                    if (!peer.id().equals(n5))
                        node.topology().onReadyToCoordinate(peer.id(), 2);
            Assertions.assertNull(command(coordTp, key, t), "node 5 witnessed T");

            // phase 3: T' coordinated by 5 (privileged, with deps); only its PreAccept to 1 is delivered
            // allocate T' the way Node.coordinate(Txn) does (best permitted fast path for this coordinator)
            Txn txnTp = writeTxn(keys);
            TxnId tp = coordTp.nextTxnId(txnTp);
            Assertions.assertEquals(2, tp.epoch());
            Assertions.assertEquals(PrivilegedCoordinatorWithDeps, tp.fastPath());
            cluster.networkFilter.addFilter(from -> from.equals(n5), to -> true,
                                            m -> !(m instanceof PreAccept && tp.equals(txnIdOf(m))));
            cluster.networkFilter.addFilter(from -> from.equals(n5), to -> !to.equals(n1), m -> true);
            // T' stays undecided: no other node sends requests about T' (no progress-log recovery of T')
            cluster.networkFilter.addFilter(from -> !from.equals(n5), to -> true, m -> tp.equals(txnIdOf(m)));
            coordTp.coordinate(tp, txnTp).begin((r, f) -> {});
            spinUntilSuccess(() -> {
                Command c1 = command(cluster.get(n1), key, tp);
                Assertions.assertNotNull(c1, "replica 1 has no T'");
                Assertions.assertEquals(Status.PreAccepted, c1.status(), "replica 1 T'=" + c1.saveStatus() + " deps=" + c1.partialDeps());
                // for PrivilegedCoordinatorWithDeps replica 1 records the coordinator's deps (Commands.preaccept)
                Assertions.assertNotNull(c1.partialDeps(), "replica 1 has no coordinator deps for T'");
                Assertions.assertFalse(c1.partialDeps().contains(t), "T' coordinator deps contain T: " + c1.partialDeps());
            }, 10);
            cluster.networkFilter.isolate(n5);

            // phase 4: recover T from 6 with quorum {1,2,3}; record the RecoverOk replies it receives
            phase.set(4);
            Map<Id, BeginRecovery.RecoverOk> recoverOks = new ConcurrentHashMap<>();
            ThreadLocal<Id> sender = new ThreadLocal<>(); // NetworkFilter predicates are evaluated in order on one thread
            cluster.networkFilter.addFilter(from -> { sender.set(from); return true; }, to -> to.equals(n6), m -> {
                if (m instanceof BeginRecovery.RecoverOk && t.equals(((BeginRecovery.RecoverOk) m).txnId))
                    recoverOks.put(sender.get(), (BeginRecovery.RecoverOk) m);
                return false;
            });
            FullRoute<?> route = coordT.computeRoute(t, txnT.keys());
            CompletableFuture<Object> outcome = new CompletableFuture<>();
            Recover.recover(coordT, t, txnT, route, false, (o, f) -> {
                if (f != null) outcome.completeExceptionally(f);
                else outcome.complete(o);
            });
            Object result = outcome.get(20, TimeUnit.SECONDS);

            Command c4 = command(cluster.get(n4), key, t);
            Command c1 = command(cluster.get(n1), key, t);
            String state = "T client result=" + tResult.get() + " failure=" + tFailure.get() + "; recovery outcome=" + result + "; replica 1 T=" + c1.saveStatus()
                           + "; replica 4 T=" + c4.saveStatus() + " executeAt=" + c4.executeAt();
            // the recovery quorum saw: three fast votes from epoch-1 electors, no superseding rejects, and
            // replica 1 reporting T' (coordinator 5, not an epoch-1 elector) as a superseding coordinator reject
            Assertions.assertEquals(Set.of(id(1), id(2), id(3)), recoverOks.keySet(), state);
            for (BeginRecovery.RecoverOk ok : recoverOks.values())
            {
                Assertions.assertTrue(ok.selfAcceptsFastPath, state);
                Assertions.assertFalse(ok.supersedingRejects, state);
            }
            Assertions.assertTrue(recoverOks.get(n1).supersedingCoordRejects.contains(tp), state);
            Assertions.assertFalse(topology1.forKey(key).isInFastPath(n5));
            // T was decided on the fast path (replica 4 has it Applied at txnId); recovery must not invalidate it
            Assertions.assertNotEquals(Status.Invalidated, c1.status(), state);
            Assertions.assertNotSame(ProgressToken.INVALIDATED, result, state);
        }
    }
}
