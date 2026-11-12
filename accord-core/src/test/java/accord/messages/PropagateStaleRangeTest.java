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

package accord.messages;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import accord.api.ProtocolModifiers;
import accord.coordinate.CoordinateTransaction;
import accord.coordinate.FetchData;
import accord.impl.IntKey;
import accord.impl.mock.MockCluster;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.Commands;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.IsolatedJvmTest;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.NetworkFilter.isId;
import static accord.NetworkFilter.isMessageType;
import static accord.Utils.id;
import static accord.Utils.shard;
import static accord.Utils.writeTxn;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;

/**
 * A lagging replica that finds a transaction truncated on one of its shards marks itself stale there, and still
 * applies the transaction on its other shards.
 *
 * Shard A = (0,50] on {1,2,3}; shard B = (50,100] on {1,4,5}. Node 1 (L) has one command store spanning both, so a
 * single Propagate for T covers both shards. While L is partitioned, T commits and applies everywhere; A's replicas
 * then GC T to TruncatedApply and L learns (as it would from SetShardDurable) that shard A is universally durable
 * past T. When L fetches T it receives quorum truncated replies for k0 (shard A) and applied replies carrying deps
 * for k1 (shard B).
 *
 * Propagate.tryUpgradeTruncated marks shard A stale. T then no longer touches shard A, as with a pre-bootstrap
 * range, so L applies T on shard B with the deps it received for it. Previously the participants still contained
 * k0, which no reply covers, so reconstituting the deps threw an IllegalArgumentException.
 */
@IsolatedJvmTest
public class PropagateStaleRangeTest
{
    @BeforeAll
    static void configure()
    {
        // With local delivery permitted, L's own reply would be part of the fetch's reply set and its NotDefined k0
        // reply would prevent shard A from being seen as fully truncated.
        ProtocolModifiers.Configure.setPermitLocalDelivery(false);
    }

    @Test
    void fetchAfterMarkingTruncatedShardStaleAppliesRemainingRanges() throws Throwable
    {
        run();
    }

    private static void run() throws Throwable
    {
        Range a = IntKey.range(0, 50), b = IntKey.range(50, 100);
        Topology topology = new Topology(1,
                                         shard(a, SortedArrayList.ofSorted(id(1), id(2), id(3))),
                                         shard(b, SortedArrayList.ofSorted(id(1), id(4), id(5))));
        try (MockCluster cluster = MockCluster.builder().nodes(5).topology(topology).build())
        {
            Node lagging = cluster.get(1);
            Node coordinator = cluster.get(2);

            // find the command store on L spanning both shards, and a key of each shard inside it
            CommandStore spanning = null;
            int k0 = -1, k1 = -1;
            for (CommandStore store : lagging.commandStores().all())
            {
                Ranges owned = store.unsafeGetRangesForEpoch().all();
                int c0 = -1, c1 = -1;
                for (int i = 1 ; i <= 100 ; ++i)
                {
                    if (!owned.contains(IntKey.routing(i))) continue;
                    if (a.contains(IntKey.routing(i))) c0 = i;
                    else if (b.contains(IntKey.routing(i)) && c1 < 0) c1 = i;
                }
                if (c0 > 0 && c1 > 0) { spanning = store; k0 = c0; k1 = c1; break; }
            }
            Assertions.assertNotNull(spanning, "expected one store on node 1 to own ranges of both shards");

            // 1) T commits and applies on 2,3 (shard A) and 4,5 (shard B) while L is partitioned
            cluster.networkFilter.isolate(id(1));
            Keys keys = IntKey.keys(k0, k1);
            TxnId txnId = coordinator.nextTxnIdWithDefaultFlags(keys, Txn.Kind.Write, Domain.Key);
            Txn txn = writeTxn(keys);
            getUninterruptibly(CoordinateTransaction.coordinate(coordinator, txnId, txn), 30, TimeUnit.SECONDS);

            AtomicReference<FullRoute<?>> route = new AtomicReference<>();
            for (int n = 2 ; n <= 5 ; ++n)
                awaitApplied(cluster.get(n), txnId, route);

            // 2) shard A's replicas GC T to TruncatedApply (the state Cleanup produces once A is durable)
            for (int n = 2 ; n <= 3 ; ++n)
            {
                for (CommandStore store : cluster.get(n).commandStores().all())
                {
                    getUninterruptibly(store.chain(ExecutionContext.contextFor(txnId, null, keys.toParticipants(), accord.local.LoadKeys.SYNC, accord.local.FindKeys.DECLARED, "truncate"), safeStore -> {
                        SafeCommand safeCommand = safeStore.ifInitialised(txnId);
                        if (safeCommand == null) return;
                        Command command = safeCommand.current();
                        Commands.purge(safeStore, safeCommand, command, command.participants(), Cleanup.TRUNCATE, true);
                    }), 10, TimeUnit.SECONDS);
                }
            }

            // 3) L learns (SetShardDurable's effect) that shard A is universally applied past T
            Ranges aRanges = Ranges.of(a);
            TxnId syncId = coordinator.nextTxnIdWithDefaultFlags(aRanges, Txn.Kind.ExclusiveSyncPoint, Domain.Range);
            getUninterruptibly(spanning.<Void>chain(ExecutionContext.unsequenced(syncId, "mark durable"), safeStore -> {
                safeStore.commandStore().markShardDurable(safeStore, syncId, aRanges, HasOutcome.Universal);
            }), 10, TimeUnit.SECONDS);
            final int fk0 = k0;
            Boolean shardOnlyApplied = getUninterruptibly(spanning.<Boolean>submit(ExecutionContext.unsequenced(txnId, "check"), safeStore ->
                       safeStore.redundantBefore().isShardOnlyApplied(txnId, IntKey.scope(fk0))), 10, TimeUnit.SECONDS);
            Assertions.assertTrue(shardOnlyApplied, "L should see shard A as SHARD_APPLIED past T");

            // 4) L reconnects and fetches T; its request to itself is dropped, so its own NotDefined reply does not
            //    prevent shard A from appearing fully truncated
            cluster.networkFilter.clear();
            cluster.networkFilter.addFilter(isId(Collections.singleton(id(1))), isId(Collections.singleton(id(1))), isMessageType(CheckStatus.class));

            CompletableFuture<Throwable> done = new CompletableFuture<>();
            FetchData.fetchSpecific(Status.PreApplied.minKnown, lagging, txnId, null, route.get(), route.get(), LatentStoreSelector.standard(),
                                    (result, failure) -> done.complete(failure));
            Throwable failure = done.get(20, TimeUnit.SECONDS);
            Assertions.assertNull(failure, "Propagate failed at the lagging store after marking the truncated shard stale: " + failure);

            // shard A is now recorded as stale at L
            Boolean stale = getUninterruptibly(spanning.<Boolean>submit(ExecutionContext.unsequenced(txnId, "stale"), safeStore ->
                        !safeStore.redundantBefore().removeLostOrStale(IntKey.scope(fk0).toRanges()).contains(IntKey.routing(fk0))), 10, TimeUnit.SECONDS);
            Assertions.assertTrue(stale, "shard A should have been marked stale");

            // T no longer touches shard A at L, so is applied for shard B
            final int fk1 = k1;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Command command;
            while (true)
            {
                command = getUninterruptibly(spanning.submit(ExecutionContext.unsequenced(txnId, "applied"), safeStore -> safeStore.ifInitialised(txnId).current()), 10, TimeUnit.SECONDS);
                if (command.hasBeen(Status.Applied) || System.nanoTime() > deadline)
                    break;
                Thread.sleep(20);
            }
            Assertions.assertTrue(command.hasBeen(Status.Applied), "T was not applied for shard B at L: " + command.saveStatus() + ' ' + command.participants());
            Assertions.assertFalse(command.participants().stillTouches().contains(IntKey.routing(fk0)), "T still touches stale shard A: " + command.participants());
            Assertions.assertTrue(command.participants().stillTouches().contains(IntKey.routing(fk1)), "T no longer touches shard B: " + command.participants());
        }
    }

    private static void awaitApplied(Node node, TxnId txnId, AtomicReference<FullRoute<?>> route) throws Throwable
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true)
        {
            boolean all = true;
            for (CommandStore store : node.commandStores().all())
            {
                SaveStatus status = getUninterruptibly(store.submit(ExecutionContext.unsequenced(txnId, "await apply"), safeStore -> {
                    SafeCommand safeCommand = safeStore.ifInitialised(txnId);
                    if (safeCommand == null) return null;
                    Command command = safeCommand.current();
                    if (command.route() instanceof FullRoute) route.compareAndSet(null, (FullRoute<?>) command.route());
                    return command.saveStatus();
                }), 10, TimeUnit.SECONDS);
                if (status != null && status.compareTo(SaveStatus.Applied) < 0) all = false;
            }
            if (all) return;
            if (System.nanoTime() > deadline) throw new AssertionError(node.id() + " did not apply " + txnId);
            Thread.sleep(20);
        }
    }
}
