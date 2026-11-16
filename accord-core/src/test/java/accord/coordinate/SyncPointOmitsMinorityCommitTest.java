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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import accord.api.AsyncExecutor;
import accord.api.MessageSink;
import accord.api.RoutingKey;
import accord.impl.mock.MockCluster;
import accord.impl.mock.Network;
import accord.impl.mock.SimpleMessageSink;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.MaxDecidedRX.DecidedRX;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.AbstractRequest;
import accord.messages.Accept;
import accord.messages.BeginRecovery;
import accord.messages.Callback;
import accord.messages.Message;
import accord.messages.PreAccept;
import accord.messages.Request;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.ProgressToken;
import accord.primitives.Routable.Domain;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Status;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.async.Cancellable;

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
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code SummaryLoader.isRelevant} skips a key whose latest transaction is decided and below a decided RX, without
 * considering durability. A transaction committed only at a minority can then be omitted by a later RX, and recovery
 * of that transaction treats the RX as evidence it did not take the fast path.
 * <p>
 * One shard, replicas {1..5}, all in the fast-path electorate: simple fast quorum 4, slow/recovery quorum 3. Node 6
 * replicates an unrelated range only, and coordinates T.
 * <ol>
 * <li>T (single-key write, Unoptimised) is coordinated by 6: PreAccept reaches 1..4 (all vote fast), the PreAccept
 *     to 5 is delayed. T takes the fast path; only 1 and 2 receive Stable/Apply. T is not durable.</li>
 * <li>RX1 is coordinated by 1: PreAccept and Accept reach {1,2,3}, Stable reaches only {1,2}. RX1 depends on T and
 *     is decided at 1 and 2 with an HLC_BOUND executeAt.</li>
 * <li>RX2 is coordinated by 1 with quorum {1,2,5}. 1 and 2 skip T's key (T decided, below RX1, no undecided txn),
 *     5 has never seen T: RX2 is decided with deps that omit T.</li>
 * <li>T is recovered by 3 with quorum {3,4,5}: 3 and 4 report fast votes, 5 reports RX2 (a later, decided
 *     transaction that did not witness T) as a superseding reject, so recovery invalidates T, which was committed
 *     (and applied) on the fast path at 1 and 2.</li>
 * </ol>
 */
public class SyncPointOmitsMinorityCommitTest
{
    private static final int TIMEOUT_SECONDS = 10;

    private static TxnId txnIdOf(Message m)
    {
        return m instanceof AbstractRequest ? ((AbstractRequest<?, ?>) m).txnId : null;
    }

    private static Command command(Node node, RoutingKey key, TxnId txnId)
    {
        CommandStore store = node.unsafeForKey(key);
        if (!inMemory(store).hasCommand(txnId))
            return null;
        return inMemory(store).command(txnId).value();
    }

    /**
     * Which requests are delivered in each phase (self-addressed requests always are).
     */
    private static boolean permit(Id from, Id to, Message m, TxnId t, TxnId[] rx, int phase, Set<Id> committed, Set<Id> votedOnly)
    {
        if (!(m instanceof Request) || from.equals(to))
            return true;
        TxnId txnId = txnIdOf(m);
        if (txnId == null)
            return true;
        if (txnId.equals(t))
        {
            if (phase == 4) // recovery: only among {3,4,5}
                return (votedOnly.contains(from) || from.equals(id(5))) && (votedOnly.contains(to) || to.equals(id(5)));
            // T's coordinator (6) sends only PreAccept to 3 and 4; anything about T among 1, 2 and 6
            if ((committed.contains(to) || to.equals(id(6))) && (committed.contains(from) || from.equals(id(6))))
                return true;
            return phase == 1 && from.equals(id(6)) && votedOnly.contains(to) && m instanceof PreAccept;
        }
        if (txnId.equals(rx[0])) // RX1: anything among 1 and 2; only PreAccept and Accept to 3
        {
            if (committed.contains(to) && committed.contains(from))
                return true;
            return from.equals(id(1)) && to.equals(id(3)) && (m instanceof PreAccept || m instanceof Accept);
        }
        if (txnId.equals(rx[1])) // RX2: only among {1,2,5}
            return !votedOnly.contains(from) && !votedOnly.contains(to);
        return false;
    }

    @Test
    void recoveryMustNotInvalidateMinorityCommitOmittedBySyncPoint() throws Throwable
    {
        Range range = range(0, 1000);
        // T is coordinated by 6, which does not replicate T's key, so that T's coordinator has no (possibly slow)
        // self vote: any delayed fast-path vote makes the coordinator abandon the fast path once it has a quorum
        Topology topology = topology(1, shard(range, idList(1, 2, 3, 4, 5), idSet(1, 2, 3, 4, 5)),
                                        shard(range(1000, 2000), idList(1, 2, 6), idSet(1, 2, 6)));
        Keys keys = keys(150);
        RoutingKey key = keys.get(0).toUnseekable();
        Set<Id> committed = Set.of(id(1), id(2)), votedOnly = Set.of(id(3), id(4)); // 5 never sees T before recovery

        // T's PreAccept to 5 is delayed indefinitely (until after recovery): unlike a dropped message, which MockCluster
        // fails immediately, a delayed PreAccept does not make T's coordinator abandon the fast path. T's other
        // messages to 5 are dropped by the filter below.
        AtomicReference<TxnId> delayToFive = new AtomicReference<>();
        List<Request> delayed = new CopyOnWriteArrayList<>();
        BiFunction<Id, Network, MessageSink> sinks = (self, network) -> new SimpleMessageSink(self, network)
        {
            @Override
            public void send(Id to, Request request)
            {
                if (!delay(to, request))
                    super.send(to, request);
            }

            @Override
            public Cancellable send(Id to, Request request, int attempt, AsyncExecutor executor, Callback callback)
            {
                return delay(to, request) ? () -> {} : super.send(to, request, attempt, executor, callback);
            }

            private boolean delay(Id to, Request request)
            {
                if (!to.equals(id(5)) || !(request instanceof PreAccept) || !((PreAccept) request).txnId.equals(delayToFive.get()))
                    return false;
                delayed.add(request);
                return true;
            }
        };

        try (MockCluster cluster = MockCluster.builder().nodes(6).topology(topology).messageSink(sinks).build())
        {
            // no background sync points
            for (Node node : cluster)
                node.durability().stop();

            Node n1 = cluster.get(1), n3 = cluster.get(3), n6 = cluster.get(6);
            Ranges ranges = Ranges.of(range);
            TxnId t = n6.nextTxnIdWithDefaultFlags(keys, Txn.Kind.Write, Key, cardinality(keys));
            Txn txnT = writeTxn(keys);
            assertEquals(Unoptimised, t.fastPath());
            delayToFive.set(t);

            TxnId[] rx = new TxnId[2]; // RX1, RX2 once allocated
            AtomicInteger phase = new AtomicInteger(1);
            // observe: T must take the fast path (no Accept round); record the remote RecoverOk replies to 3
            AtomicBoolean acceptSent = new AtomicBoolean();
            Map<Id, BeginRecovery.RecoverOk> recoverOks = new ConcurrentSkipListMap<>();
            // a single filter, as NetworkFilter evaluates the (from, to, message) predicates of one filter in order
            ThreadLocal<Id> sender = new ThreadLocal<>(), receiver = new ThreadLocal<>();
            cluster.networkFilter.addFilter(from -> { sender.set(from); return true; },
                                            to -> { receiver.set(to); return true; },
                                            m -> {
                                                if (m instanceof Accept && t.equals(txnIdOf(m)))
                                                    acceptSent.set(true);
                                                if (m instanceof BeginRecovery.RecoverOk && t.equals(((BeginRecovery.RecoverOk) m).txnId) && receiver.get().equals(id(3)))
                                                    recoverOks.put(sender.get(), (BeginRecovery.RecoverOk) m);
                                                return !permit(sender.get(), receiver.get(), m, t, rx, phase.get(), committed, votedOnly);
                                            });

            // 1. T: fast path, Stable/Apply only at 1 and 2
            getUninterruptibly(n6.coordinate(t, txnT), TIMEOUT_SECONDS, TimeUnit.SECONDS);
            spinUntilSuccess(() -> {
                for (Id id : committed)
                {
                    Command c = command(cluster.get(id), key, t);
                    assertTrue(c.hasBeen(Status.Applied), id + ": " + c.saveStatus());
                    assertEquals(t, c.executeAt(), "T must be fast path committed at " + id);
                    assertFalse(c.durability().isDurablyCommitted(), "T must not be durable at " + id);
                }
            }, TIMEOUT_SECONDS);
            for (Id id : votedOnly)
            {
                Command c = command(cluster.get(id), key, t);
                assertEquals(Status.PreAccepted, c.status(), id + ": " + c.saveStatus());
                assertEquals(t, c.executeAt(), id + " must vote for T's fast path");
            }
            assertNull(command(cluster.get(5), key, t));
            assertFalse(acceptSent.get(), "T must take the fast path");

            // 2. RX1: PreAccept/Accept at {1,2,3}, decided only at 1 and 2; its coordinator never completes
            rx[0] = n1.nextTxnIdWithDefaultFlags(ranges, Txn.Kind.ExclusiveSyncPoint, Domain.Range, cardinality(ranges));
            phase.set(2);
            CoordinateSyncPoint.exclusive(n1, rx[0], ranges).begin((s, f) -> {});
            spinUntilSuccess(() -> {
                for (Id id : committed)
                {
                    Command c = command(cluster.get(id), key, rx[0]);
                    assertNotNull(c, id + " has no RX1");
                    assertTrue(c.hasBeen(Status.Committed), id + ": " + c.saveStatus());
                    assertTrue(c.executeAt().is(HLC_BOUND), "RX1 must be HLC_BOUND: " + c.executeAt());
                    assertTrue(c.partialDeps().contains(t), "RX1 must depend on T: " + c.partialDeps());
                    DecidedRX decided = cluster.get(id).unsafeForKey(key).unsafeGetMaxDecidedRX().get(key);
                    assertTrue(decided.excludeDecided(t), id + " must consider T decided by RX1: " + decided);
                }
            }, TIMEOUT_SECONDS);

            // 3. RX2 with quorum {1,2,5}
            rx[1] = n1.nextTxnIdWithDefaultFlags(ranges, Txn.Kind.ExclusiveSyncPoint, Domain.Range, cardinality(ranges));
            phase.set(3);
            SyncPoint second = getUninterruptibly(CoordinateSyncPoint.exclusive(n1, rx[1], ranges), TIMEOUT_SECONDS, TimeUnit.SECONDS);
            spinUntilSuccess(() -> {
                Command c = command(cluster.get(5), key, rx[1]);
                assertNotNull(c, "5 has no RX2");
                assertTrue(c.hasBeen(Status.Stable), "5: " + c.saveStatus());
            }, TIMEOUT_SECONDS);
            boolean rx2WitnessesT = second.waitFor.contains(t);

            // 4. recover T from 3 with quorum {3,4,5}; T's original PreAccept to 5 is still in flight
            assertFalse(delayed.isEmpty(), "T's PreAccept to 5 must be delayed");
            delayToFive.set(null);
            phase.set(4);
            FullRoute<?> route = n3.computeRoute(t, txnT.keys());
            CompletableFuture<Object> outcome = new CompletableFuture<>();
            Recover.recover(n3, t, txnT, route, false, (o, f) -> {
                if (f != null) outcome.completeExceptionally(f);
                else outcome.complete(o);
            });
            Object result = outcome.get(2 * TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Command c1 = command(n1, key, t), c3 = command(n3, key, t);
            String state = "T=" + t + " RX1=" + rx[0] + " RX2=" + rx[1] + "; RX2 deps=" + second.waitFor
                           + "; recovery outcome=" + result + "; replica 1 T=" + c1.saveStatus() + " executeAt=" + c1.executeAt()
                           + "; replica 3 T=" + c3.saveStatus() + " executeAt=" + c3.executeAt() + "; RecoverOk replies:";
            for (Map.Entry<Id, BeginRecovery.RecoverOk> e : recoverOks.entrySet())
                state += " " + e.getKey() + "{selfAcceptsFastPath=" + e.getValue().selfAcceptsFastPath + ",supersedingRejects=" + e.getValue().supersedingRejects + '}';
            // T was fast-path committed and applied at 1 and 2: it must be neither invalidated nor executed elsewhere
            assertNotSame(ProgressToken.INVALIDATED, result, state);
            assertNotEquals(Status.Invalidated, c3.status(), state);
            assertTrue(c3.executeAt() == null || c3.executeAt().equals(t), state);
            // the precondition: RX2 should witness T, which is committed but not durable
            assertTrue(rx2WitnessesT, state);
        }
    }
}
