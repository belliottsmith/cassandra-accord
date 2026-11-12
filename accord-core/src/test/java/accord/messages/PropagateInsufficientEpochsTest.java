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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.Utils;
import accord.coordinate.Infer.InvalidIf;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.local.ShardDistributor;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KnownMap;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.WithQuorum;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncChainUtils;

import static accord.primitives.Routables.Slice.Minimal;

/**
 * A Propagate of Stable must not throw when commit validation reports InsufficientEpochs.
 *
 * Node N1 has a single command store owning [0,100) in epoch 1. In epoch 2 it keeps only [0,10);
 * [10,100) moves to N2, so the store's RedundantBefore gets a lost-ownership bound (endEpoch 2) on
 * [10,100) via the real CommandStores.updateTopology -> CommandStore.unbootstrap path.
 *
 * An exclusive sync point in epoch 2 over [0,100) is PreAccepted on N1 (scope [0,10), full route),
 * then N1 learns Stable through Propagate with the partial route [0,10) (the shape FetchData produces
 * when its query/maxRoute are partial; Propagate.applyInternal explicitly supports a partial route
 * plus a locally known full route). Commands.commit's validate() folds the lost bound over the full
 * route, finds [10,100) outside stillWaitsOn, and returns INSUFFICIENT_EPOCHS; Propagate.confirm has
 * no case for CommitOutcome.InsufficientEpochs and throws IllegalStateException, which fails the
 * Propagate callback. Commit.applyInternal treats the same outcome as an ordinary nack.
 */
public class PropagateInsufficientEpochsTest
{
    static final Id N1 = new Id(1), N2 = new Id(2);
    static final SortedArrayList<Id> ALL = new SortedArrayList<>(new Id[] { N1, N2 });
    static final Range FULL = IntKey.range(0, 100), KEPT = IntKey.range(0, 10), LOST = IntKey.range(10, 100);

    private static Node nodeThatLosesRange() throws Exception
    {
        Topology t1 = TopologyUtils.initialTopology(ALL, Ranges.single(FULL), ALL.size());
        Topology t2 = new Topology(t1.epoch() + 1,
                                   Utils.shard(KEPT, ALL),
                                   Utils.shard(LOST, new SortedArrayList<>(new Id[] { N2 })));
        Node n1 = Utils.createNode(N1, t1, new RecordingMessageSink(N1, Network.BLACK_HOLE), new MockCluster.Clock(100),
                                   new TestAgent.RethrowAgent(), new ShardDistributor.EvenSplit<>(1, ignore -> new IntKey.Splitter()));
        n1.topology().reportTopology(t2);

        CommandStore store = n1.unsafeForKey(IntKey.routing(5));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true)
        {
            RedundantBefore.Bounds lost = store.unsafeGetRedundantBefore().get(IntKey.routing(50));
            if (lost != null && lost.endEpoch == 2 && store.unsafeGetRangesForEpoch().currentRanges().equals(Ranges.single(KEPT)))
                break;
            if (System.nanoTime() > deadline)
                Assertions.fail("store did not unbootstrap " + LOST + ": " + store.unsafeGetRedundantBefore() + " / " + store.unsafeGetRangesForEpoch());
            Thread.sleep(10);
        }
        return n1;
    }

    private static Throwable stableViaPropagate(Node n1, boolean partialPropagateRoute, boolean preAcceptFirst) throws Exception
    {
        TxnId txnId = new TxnId(2, n1.now(), 0, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, N2);
        FullRoute<?> route = n1.computeRoute(txnId, Ranges.single(FULL));
        Txn txn = n1.agent().emptySystemTxn(txnId.kind(), txnId.domain());
        Ranges kept = Ranges.single(KEPT);
        Route<?> propagateRoute = partialPropagateRoute ? route.slice(kept, Minimal) : route;

        if (preAcceptFirst)
        {
            // coordinator N2 PreAccepts on N1 with N1's epoch-2 scope and the full route
            PreAccept preAccept = PreAccept.SerializerSupport.create(txnId, route.slice(kept, Minimal), 2, 2, 2, txn.slice(kept, true), null, false, route);
            preAccept.process(n1, N2, Network.replyCtxFor(0));
        }

        // Peer reply: Stable for the propagated route, executeAt == txnId, empty deps covering it
        CheckStatusOkFull full = new CheckStatusOkFull(KnownMap.create(propagateRoute, SaveStatus.Stable), SaveStatus.Stable, SaveStatus.Stable,
                                                       Ballot.ZERO, Ballot.ZERO, Ballot.ZERO, txnId, false, Status.Durability.NotDurable,
                                                       propagateRoute, route.homeKey(), InvalidIf.NotKnownToBeInvalid,
                                                       txn.slice(Ranges.single(FULL), true), Deps.NONE.intersecting(route), null, null);

        CompletableFuture<Throwable> done = new CompletableFuture<>();
        Propagate.propagate(n1, txnId, InvalidIf.NotKnownToBeInvalid, txnId.epoch(), WithQuorum.NoQuorum,
                            propagateRoute, propagateRoute, LatentStoreSelector.standard(), null, full,
                            (result, failure) -> done.complete(failure), null);
        Throwable failure = done.get(10, TimeUnit.SECONDS);

        AtomicReference<SaveStatus> local = new AtomicReference<>();
        AsyncChainUtils.getUninterruptibly(n1.unsafeForKey(IntKey.routing(5)).execute(ExecutionContext.unsequenced(txnId, "check"), safeStore -> {
            Command command = safeStore.ifInitialised(txnId) == null ? null : safeStore.ifInitialised(txnId).current();
            local.set(command == null ? null : command.saveStatus());
        }), 10, TimeUnit.SECONDS);
        return failure;
    }

    @Test
    public void stablePropagateWithPartialRouteAfterRangeLossDoesNotThrow() throws Exception
    {
        Node n1 = nodeThatLosesRange();
        try
        {
            Throwable failure = stableViaPropagate(n1, true, true);
            Assertions.assertNull(failure, "Propagate of Stable failed instead of treating the commit outcome as a nack: " + failure);
        }
        finally
        {
            n1.shutdown();
        }
    }

    @Test
    public void controlStablePropagateWithFullRouteAfterRangeLoss() throws Exception
    {
        Node n1 = nodeThatLosesRange();
        try
        {
            Throwable failure = stableViaPropagate(n1, false, true);
            Assertions.assertNull(failure, "control: Propagate with full route failed: " + failure);
        }
        finally
        {
            n1.shutdown();
        }
    }
}
