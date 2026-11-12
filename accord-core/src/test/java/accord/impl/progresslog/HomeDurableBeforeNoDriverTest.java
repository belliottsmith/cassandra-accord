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

package accord.impl.progresslog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import accord.api.ProtocolModifiers;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.ExecuteSyncPoint;
import accord.impl.mock.MockCluster;
import accord.local.Command;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.messages.AbstractRequest;
import accord.messages.Apply;
import accord.messages.InformDurable;
import accord.messages.SetShardDurable;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.IsolatedJvmTest;

import static accord.NetworkFilter.anyId;
import static accord.NetworkFilter.isMessageType;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.keys;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A home replica that learns from DurableBefore that a ReadyToExecute command is durable must still fetch its outcome.
 *
 * A home-shard replica R of write T reaches ReadyToExecute, the Apply (and InformDurable) to R is lost, and a
 * durability round (ExclusiveSyncPoint + SetShardDurable) makes R's DurableBefore cover T before R's home progress
 * run succeeds. HomeState.runHome then marks home Done via SafeCommand.incidentalUpdate, which does not call
 * ProgressLog.update, so (previously) DefaultProgressLog.maybeFetch never registered a CanApply waiting state
 * and T stayed ReadyToExecute at R with no local driver.
 *
 * Real 3-node MockCluster, rf=3 (every node replicates the home key), DefaultProgressLog, TestAgent 1s delays,
 * send_stable=TO_ALL (Cassandra accord.send_stable) so R receives Stable. Message loss: to node 3 every Apply,
 * InformDurable and every ExclusiveSyncPoint message except SetShardDurable is discarded; node 3's outbound
 * messages are discarded until its DurableBefore covers T (its home MaybeRecover attempts time out), then healed.
 * Intended: node 3 applies T on its own. The test bounds the wait to 20s (20 retry periods) and then fails.
 *
 * The key-domain write is a control, as CommandsForKey.notifyManaged registers CanApply on T at Stable; the
 * range-domain read has no such driver. Background shard durability rounds (whose sync points at node 3 depend on T)
 * would otherwise rescue it, so the cycle is set to Cassandra's 5 minute default.
 */
@IsolatedJvmTest
public class HomeDurableBeforeNoDriverTest
{
    static
    {
        ProtocolModifiers.Configure.setSendStableMessages(ProtocolModifiers.SendStableMessages.TO_ALL);
    }

    static final class Observed
    {
        final List<String> perStore = new ArrayList<>();
        boolean allApplied = true, anyReadyToExecute = false, anyDurableField = false;
        Route<?> route;

        @Override
        public String toString() { return perStore.toString(); }
    }

    private static Observed observe(Node node, TxnId txnId, Seekables<?, ?> keys) throws Exception
    {
        Observed o = new Observed();
        List<String> lines = Collections.synchronizedList(o.perStore);
        getUninterruptibly(node.commandStores().forEach("Observe", txnId, keys.toParticipants(), txnId.epoch(), txnId.epoch(), safeStore -> {
            StoreParticipants participants = StoreParticipants.read(safeStore, keys.toParticipants(), txnId);
            Command command = safeStore.get(txnId, participants).current();
            DefaultProgressLog log = (DefaultProgressLog) safeStore.commandStore().unsafeProgressLog();
            TxnState state = log.get(txnId);
            String driver = state == null ? "no progress-log state"
                                          : "waiting.blockedUntil=" + state.blockedUntil() + " waitingProgress=" + state.waitingProgress()
                                            + " homePhase=" + state.homePhase() + " homeProgress=" + state.homeProgress();
            synchronized (o)
            {
                o.allApplied &= command.hasBeen(Status.Applied);
                o.anyReadyToExecute |= command.saveStatus() == SaveStatus.ReadyToExecute;
                o.anyDurableField |= command.durability().isDurable();
                if (command.route() != null) o.route = command.route();
            }
            lines.add("store " + safeStore.commandStore().id() + ": " + command.saveStatus() + " durability=" + command.durability() + " " + driver);
        }));
        return o;
    }

    /** Key-domain write: CommandsForKey.notifyManaged registers CanApply on T at Stable, a driver independent of the home state. */
    @Test
    void keyWriteHomeReplicaLearningDurableBeforeStillApplies() throws Throwable
    {
        run(keys(10), Key);
    }

    /** Range-domain read (range writes are rejected, Node): no CommandsForKey manages it, so the home state is the only local driver. */
    @Test
    void rangeReadHomeReplicaLearningDurableBeforeStillApplies() throws Throwable
    {
        Ranges ranges = accord.Utils.ranges(accord.impl.IntKey.range(10, 11));
        run(ranges, accord.primitives.Routable.Domain.Range, Txn.Kind.Read,
            new Txn.InMemory(ranges, accord.impl.mock.MockStore.read(ranges), accord.impl.mock.MockStore.QUERY));
    }

    private static void run(Seekables<?, ?> keys, accord.primitives.Routable.Domain domain) throws Throwable
    {
        run(keys, domain, Write, writeTxn((Keys) keys));
    }

    private static void run(Seekables<?, ?> keys, accord.primitives.Routable.Domain domain, Txn.Kind kind, Txn txn) throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            // Cassandra's default accord.shard_durability_cycle is 5 minutes (AccordConfig applied at
            // AccordService); MockCluster's ShardDurability default is 30s
            for (int i = 1 ; i <= 3 ; ++i)
                cluster.get(i).durability().shards().setShardCycleTime(5, TimeUnit.MINUTES);

            AtomicBoolean node3OutboundLost = new AtomicBoolean(true);
            // lost to node 3: every Apply and InformDurable (T's outcome and durability notification) ...
            cluster.networkFilter.addFilter(anyId(), to -> to.equals(id(3)), isMessageType(Apply.class));
            cluster.networkFilter.addFilter(anyId(), to -> to.equals(id(3)), isMessageType(InformDurable.class));
            // ... and the durability round's sync point messages, except its SetShardDurable
            cluster.networkFilter.addFilter(anyId(), to -> to.equals(id(3)),
                                            msg -> msg instanceof AbstractRequest && !(msg instanceof SetShardDurable)
                                                   && ((AbstractRequest<?, ?>) msg).txnId.is(Txn.Kind.ExclusiveSyncPoint));
            // node 3's outbound messages (its home MaybeRecover) are lost until it has learned DurableBefore
            cluster.networkFilter.addFilter(from -> from.equals(id(3)) && node3OutboundLost.get(), anyId(), msg -> true);

            Node coordinator = cluster.get(1);
            Node replica = cluster.get(3);
            TxnId txnId = coordinator.nextTxnIdWithDefaultFlags(keys, kind, domain);
            getUninterruptibly(coordinator.coordinate(txnId, txn), 10, TimeUnit.SECONDS);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Observed o = observe(replica, txnId, keys);
            while (!o.anyReadyToExecute && System.nanoTime() < deadline)
            {
                Thread.sleep(2);
                o = observe(replica, txnId, keys);
            }
            assertTrue(o.anyReadyToExecute, "precondition: node 3 never reached ReadyToExecute: " + o);
            assertTrue(replica.topology().current().rangesForNode(id(3)).contains(o.route.homeKey()), "precondition: node 3 is not a home-shard replica");

            // durability round from node 1: ExclusiveSyncPoint over all ranges, executed (applied at nodes 1,2);
            // its execution reports the shard durable at a quorum, which reaches node 3 as SetShardDurable is not lost
            Ranges ranges = coordinator.topology().current().ranges();
            SyncPoint syncPoint = getUninterruptibly(CoordinateSyncPoint.exclusive(coordinator, ranges), 10, TimeUnit.SECONDS);
            ExecuteSyncPoint.DurabilityResults results = ExecuteSyncPoint.coordinate(coordinator, syncPoint, 1);
            getUninterruptibly(results.onDone(), 15, TimeUnit.SECONDS);

            Route<?> route = o.route;
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!replica.durableBefore().min(txnId, route).isDurable() && System.nanoTime() < deadline)
                Thread.sleep(2);
            assertTrue(replica.durableBefore().min(txnId, route).isDurable(), "precondition: node 3 DurableBefore never covered " + txnId + "; " + replica.durableBefore());
            o = observe(replica, txnId, keys);
            assertTrue(!o.allApplied, "precondition: node 3 already applied T before DurableBefore: " + o);
            node3OutboundLost.set(false); // heal node 3's outbound link

            // intended: node 3's progress log drives T to Applied (bounded: 20 x 1s retry delay)
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!o.allApplied && System.nanoTime() < deadline)
            {
                Thread.sleep(100);
                o = observe(replica, txnId, keys);
            }
            if (!o.allApplied)
                fail("node 3 (home-shard replica) left " + txnId + " unapplied 20s after DurableBefore covered it with its Apply lost: " + o);
        }
    }
}
