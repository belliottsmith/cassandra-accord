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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.InMemoryCommandStore;
import accord.impl.mock.MockCluster;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandSummaries.SummaryLoader;
import accord.local.FindKeys;
import accord.local.RedundantStatus;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.local.Node;
import accord.local.cfk.CommandsForKey;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.TxnId.Cardinality;

import static accord.NetworkFilter.anyId;
import static accord.NetworkFilter.isMessageType;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.keys;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Once an exclusive sync point is decided for a range, a later exclusive sync point over that range should neither
 * load nor depend on any key with no new transaction since: the earlier sync point already covers those keys.
 * <p>
 * The sync points are deliberately not made durable, so that GC cannot remove the earlier writes. This test checks
 * the {@link accord.local.MaxDecidedRX} filtering, not GC.
 */
public class SyncPointKeyFilteringTest
{
    private static final int KEY_COUNT = 16;
    private static final int MAX_KEY = 10000;
    private static final long TIMEOUT_SECONDS = 10;

    /**
     * Multi-key writes are informed of their durability, so every write before the first sync point can be omitted
     * from the second sync point's dependencies.
     */
    @Test
    void durableWrites() throws Throwable
    {
        run(Cardinality.Any, true, false);
    }

    /**
     * Single-key writes with few dependencies are not informed of their durability, but can still be omitted once a
     * quorum has applied the first sync point, as their recovery will find their outcome. The sync point is applied
     * by only a quorum, so nothing is GC'd. Keys with no new work need not even be loaded.
     */
    @Test
    void notDurableSingleKeyWritesBeforeQuorumApplied() throws Throwable
    {
        run(Cardinality.SingleKey, false, true);
    }

    private static void run(Cardinality writeCardinality, boolean durable, boolean quorumApplied) throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).maxKey(MAX_KEY).build())
        {
            // no background durability rounds, so the only sync points are ours and nothing is GC'd
            for (Node node : cluster)
                node.durability().shards().setShardCycleTime(5, TimeUnit.MINUTES);

            Node coordinator = cluster.get(1);
            Ranges ranges = coordinator.topology().current().ranges();

            int[] ks = new int[KEY_COUNT];
            for (int i = 0 ; i < KEY_COUNT ; ++i)
                ks[i] = 1 + i * (MAX_KEY / KEY_COUNT);
            RoutingKeys allKeys = keys(ks).toParticipants();
            RoutingKey touchedKey = allKeys.get(0);

            // a separate write per key, so each key has its own latest write
            List<TxnId> oldWrites = new ArrayList<>();
            for (int k : ks)
                oldWrites.add(write(coordinator, keys(k), writeCardinality));

            SyncPoint first = syncPoint(coordinator, ranges);
            assertEquals(allKeys, first.waitFor.keyDeps.keys(), "precondition: the first sync point must depend on every key");
            for (TxnId oldWrite : oldWrites)
                assertTrue(first.waitFor.keyDeps.contains(oldWrite), "precondition: the first sync point must depend on " + oldWrite);

            awaitDecided(cluster, first.syncId, ranges);
            if (durable)
                awaitDurablyCommitted(cluster, oldWrites);
            if (quorumApplied)
                makeQuorumApplied(cluster, coordinator, first, ranges);

            TxnId newWrite = write(coordinator, keys(ks[0]), writeCardinality);

            // loading: only the key with new work should be loaded by a new sync point (checked only once quorum
            // applied, as whether writes known only to be durable can be skipped depends on what the loading filter
            // can know about them)
            checkScan(cluster, coordinator, ranges, allKeys, touchedKey, first.syncId, quorumApplied);

            // dependencies: only the new write should be reported
            SyncPoint second = syncPoint(coordinator, ranges);
            KeyDeps keyDeps = second.waitFor.keyDeps;
            assertEquals(List.of(newWrite), txnIds(keyDeps), "the second sync point should depend only on " + newWrite + "; found " + keyDeps);
            assertTrue(second.waitFor.rangeDeps.contains(first.syncId), "the second sync point should depend on " + first.syncId + "; found " + second.waitFor.rangeDeps);
        }
    }

    /**
     * Check every replica still holds the writes before {@code firstId}, and if {@code checkLoading} that a new sync
     * point would load only {@code touchedKey}.
     */
    private static void checkScan(MockCluster cluster, Node coordinator, Ranges ranges, RoutingKeys allKeys, RoutingKey touchedKey, TxnId firstId, boolean checkLoading) throws Exception
    {
        TxnId probe = coordinator.nextTxnIdWithDefaultFlags(ranges, Txn.Kind.ExclusiveSyncPoint, Range, cardinality(ranges));
        for (Node node : cluster)
        {
            for (CommandStore store : node.commandStores().all())
            {
                Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
                TreeSet<RoutingKey> ownedKeys = new TreeSet<>();
                allKeys.slice(owned, Minimal).forEach(ownedKeys::add);
                Scan scan = scan((InMemoryCommandStore) store, probe, owned);
                String desc = "node " + node.id() + " store " + store.id();
                assertEquals(ownedKeys, scan.retained, "precondition: " + desc + " must still hold the writes before " + firstId);
                if (!checkLoading)
                    continue;

                TreeSet<RoutingKey> expect = new TreeSet<>();
                if (ownedKeys.contains(touchedKey))
                    expect.add(touchedKey);
                assertEquals(expect, scan.relevant, desc + " would load keys with no new work since " + firstId);
            }
        }
    }

    private static TxnId write(Node coordinator, Keys keys, Cardinality cardinality) throws Exception
    {
        TxnId txnId = coordinator.nextTxnIdWithDefaultFlags(keys, Txn.Kind.Write, Key, cardinality);
        getUninterruptibly(coordinator.coordinate(txnId, writeTxn(keys)), TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return txnId;
    }

    private static SyncPoint syncPoint(Node coordinator, Ranges ranges) throws Exception
    {
        return getUninterruptibly(CoordinateSyncPoint.exclusive(coordinator, ranges), TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static List<TxnId> txnIds(KeyDeps keyDeps)
    {
        List<TxnId> txnIds = new ArrayList<>();
        for (int i = 0 ; i < keyDeps.txnIdCount() ; ++i)
            txnIds.add(keyDeps.txnId(i));
        return txnIds;
    }

    /**
     * Wait until every command store has recorded {@code syncId} as decided for the ranges it owns.
     */
    private static void awaitDecided(MockCluster cluster, TxnId syncId, Ranges ranges) throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (Node node : cluster)
        {
            for (CommandStore store : node.commandStores().all())
            {
                Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
                while (!isDecided(store, syncId, owned))
                {
                    assertTrue(System.nanoTime() < deadline, store + " did not decide " + syncId + ": " + store.unsafeGetMaxDecidedRX());
                    Thread.sleep(10);
                }
            }
        }
    }

    private static boolean isDecided(CommandStore store, TxnId syncId, Ranges owned) throws Exception
    {
        return getUninterruptibly(store.chain(() -> store.unsafeGetMaxDecidedRX().foldlWithDefault(owned, (v, prev) -> prev && v != null && v.any.compareTo(syncId) >= 0, null, true)),
                                  TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Have a quorum, but not every replica, report {@code syncPoint} applied, so that every replica marks it
     * QUORUM_APPLIED but not SHARD_APPLIED (which would GC the earlier writes).
     */
    private static void makeQuorumApplied(MockCluster cluster, Node coordinator, SyncPoint syncPoint, Ranges ranges) throws Exception
    {
        // every replica applies the sync point itself, so can accept the durability report
        awaitStatus(cluster, syncPoint.syncId, ranges, LOCALLY_APPLIED);
        cluster.networkFilter.addFilter(anyId(), to -> to.equals(id(3)), isMessageType(ApplyThenWaitUntilApplied.class));
        try
        {
            DurabilityResult result = getUninterruptibly(ExecuteSyncPoint.coordinate(coordinator, syncPoint, 1).onDone(), TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals(SyncRemote.QuorumAndWaitedForAll, result.min.remote, "precondition: " + result);
        }
        finally
        {
            cluster.networkFilter.clear();
        }
        awaitStatus(cluster, syncPoint.syncId, ranges, QUORUM_APPLIED);
    }

    /**
     * Wait until every command store's {@code RedundantBefore} has {@code property} for {@code syncId} on the ranges it owns.
     */
    private static void awaitStatus(MockCluster cluster, TxnId syncId, Ranges ranges, RedundantStatus.Property property) throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (Node node : cluster)
        {
            for (CommandStore store : node.commandStores().all())
            {
                Ranges owned = store.unsafeGetRangesForEpoch().all().slice(ranges, Minimal);
                while (!getUninterruptibly(store.chain(() -> store.unsafeGetRedundantBefore().status(syncId, null, owned).all(property)), TIMEOUT_SECONDS, TimeUnit.SECONDS))
                {
                    assertTrue(System.nanoTime() < deadline, store + " did not reach " + property + " for " + syncId + ": " + store.unsafeGetRedundantBefore());
                    Thread.sleep(10);
                }
            }
        }
    }

    /**
     * Wait until every replica of {@code txnIds} knows they are durably committed.
     */
    private static void awaitDurablyCommitted(MockCluster cluster, List<TxnId> txnIds) throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (Node node : cluster)
        {
            for (CommandStore store : node.commandStores().all())
            {
                InMemoryCommandStore inMemory = (InMemoryCommandStore) store;
                for (TxnId txnId : txnIds)
                {
                    while (!getUninterruptibly(store.chain(() -> {
                        InMemoryCommandStore.GlobalCommand global = inMemory.commandIfPresent(txnId);
                        Command command = global == null ? null : global.value();
                        return command == null || command.durability().isDurablyCommitted();
                    }), TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    {
                        assertTrue(System.nanoTime() < deadline, store + " was not informed " + txnId + " is durable");
                        Thread.sleep(10);
                    }
                }
            }
        }
    }

    static class Scan
    {
        /** keys with a non-empty {@link CommandsForKey} */
        final TreeSet<RoutingKey> retained = new TreeSet<>();
        /** keys whose {@link CommandsForKey} a sync point would load */
        final TreeSet<RoutingKey> relevant = new TreeSet<>();
    }

    /**
     * Apply the {@link SummaryLoader} predicate that implementations use to skip keys when scanning a range for
     * {@code probe}, to every {@link CommandsForKey} the store has.
     */
    private static Scan scan(InMemoryCommandStore store, TxnId probe, Ranges owned) throws Exception
    {
        return getUninterruptibly(store.chain(() -> {
            SummaryLoader loader = SummaryLoader.loader(null, store.unsafeGetRedundantBefore(), store.unsafeGetMaxDecidedRX(),
                                                        probe, probe, FindKeys.CONFLICTS, owned,
                                                        (p, rb, mx, primary, search, kinds, min, executeAt, find) -> new SummaryLoader(rb, mx, primary, search, kinds, min, executeAt, find));
            Scan scan = new Scan();
            for (InMemoryCommandStore.GlobalCommandsForKey global : store.unsafeCommandsForKey().values())
            {
                CommandsForKey cfk = global.value();
                if (cfk == null || cfk.size() == 0 || !owned.contains(cfk.key()))
                    continue;
                scan.retained.add(cfk.key());
                if (loader.isRelevant(cfk))
                    scan.relevant.add(cfk.key());
            }
            return scan;
        }), TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
