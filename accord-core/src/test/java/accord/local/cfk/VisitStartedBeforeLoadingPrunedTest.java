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

package accord.local.cfk;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.Result.PersistableResult;
import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.mock.MockStore;
import accord.local.Command;
import accord.local.CommandBuilder;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;

import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.Cardinality.SingleKey;

/**
 * CommandsForKey.visit(startedBefore) must report every visible loadingPruned dependency before startedBefore.
 *
 * CommandsForKey.visit(startedBefore, testKind, ...) (the deps-calculation visitor used by
 * DepsCalculator / PreAccept) states that loadingPruned entries are returned
 * (CommandsForKey "we'll make sure to return them"), but the loadingPruned iterator is
 * only drained inside the byId loop, before a byId entry greater than the pruned id, and there is no
 * flush after the loop. With l loading-pruned, l &lt; s &lt; prunedBefore and no byId entry in (l, s),
 * visit(s) never reports l; the same call with a managed write m in (l, s) does.
 *
 * loadingPruned entries are only reported when isVisible, i.e. witnessed by a VisibilitySyncPoint
 * (ProtocolModifiers.transitiveDependenciesAreVisible default; Pruning.loadPruned). So l is registered the way
 * production does it for a range VisibilitySyncPoint v: Stable v with key dep l is registered on the CFK via
 * registerUnmanaged(REGISTER) (SafeCommandStore.updateUnmanaged -> Updating.updateUnmanaged, Updating).
 */
public class VisitStartedBeforeLoadingPrunedTest
{
    private static final RoutingKey KEY = IntKey.routing(1);
    private static final Keys KEYS = IntKey.keys(1);
    private static final Ranges RANGES = Ranges.of(IntKey.range(0, 2));
    private static final FullRoute<?> KEY_ROUTE = KEYS.toRoute(IntKey.routing(1));
    private static final Txn KEY_TXN = new Txn.InMemory(KEYS, MockStore.read(KEYS), MockStore.QUERY);
    private static final FullRoute<?> RANGE_ROUTE = RANGES.toRoute(IntKey.routing(1));
    private static final Txn RANGE_TXN = new Txn.InMemory(RANGES, MockStore.read(RANGES), MockStore.QUERY);
    private static final Node.Id N1 = new Node.Id(1);

    private static TxnId write(long hlc)
    {
        return new TxnId(1, hlc, 0, Write, Key, SingleKey, N1);
    }

    private static CommandBuilder builder(TxnId txnId)
    {
        return new CommandBuilder(txnId)
               .durability(NotDurable)
               .participants(StoreParticipants.all(KEY_ROUTE))
               .partialTxn(KEY_TXN.slice(RANGES, true));
    }

    private static Deps keyDeps(TxnId... ids)
    {
        try (Deps.Builder b = new Deps.Builder(true))
        {
            for (TxnId id : ids) b.add(KEY, id);
            return b.build();
        }
    }

    private static Command applied(TxnId txnId, TxnId... deps)
    {
        Deps d = keyDeps(deps);
        Command.WaitingOn.Update waitingOn = Command.WaitingOn.Update.unsafeInitialise(txnId, RANGES, KEY_ROUTE, d);
        for (int i = 0 ; i < waitingOn.txnIdCount() ; ++i)
            waitingOn.removeWaitingOn(waitingOn.txnId(i));
        return builder(txnId).durability(AllQuorums) // durably applied at a quorum: eligible for pruning
                             .partialDeps(d.intersecting(KEY_ROUTE))
                             .executeAt(txnId)
                             .waitingOn(waitingOn.build())
                             .writes(new Writes(txnId, txnId, Keys.EMPTY, null))
                             .result(new PersistableResult(){})
                             .build(SaveStatus.Applied);
    }

    // a0(5) < l(10) < [m(15)] < s(20) < p(50) < q(60) < v(70); a0..q key writes on KEY, v a range VisibilitySyncPoint.
    // a0, p, q applied durably; maybePrune prunes at p. v is Stable with key dep l; l was never seen on this
    // store and is below prunedBefore, so registerUnmanaged puts it in loadingPruned (LoadPruned not yet run).
    static final TxnId A0 = write(5), L = write(10), M = write(15), S = write(20), P = write(50), Q = write(60);
    static final TxnId V = new TxnId(1, 70, 0, VisibilitySyncPoint, Range, Any, N1);

    private static Command stableSyncPoint(TxnId txnId, TxnId... keyDeps)
    {
        Deps d = keyDeps(keyDeps);
        Command.WaitingOn.Update waitingOn = Command.WaitingOn.Update.unsafeInitialise(txnId, RANGES, RANGE_ROUTE, d);
        return new CommandBuilder(txnId)
               .durability(NotDurable)
               .participants(StoreParticipants.all(RANGE_ROUTE))
               .partialTxn(RANGE_TXN.slice(RANGES, true))
               .partialDeps(d.intersecting(RANGE_ROUTE))
               .executeAt(txnId)
               .waitingOn(waitingOn.build())
               .build(SaveStatus.Stable);
    }

    private static CommandsForKey build(boolean withM)
    {
        CommandsForKeyTest.TestSafeCommandsForKey safeCfk = new CommandsForKeyTest.TestSafeCommandsForKey(new CommandsForKey(KEY));
        CommandsForKeyTest.TestSafeStore safeStore = new CommandsForKeyTest.TestSafeStore(null, null, safeCfk);

        CommandsForKey cfk = safeCfk.current();
        cfk = cfk.update(safeStore, applied(A0)).cfk();
        if (withM) // control: an undecided managed write in (l, s) that pruning retains
            cfk = cfk.update(safeStore, builder(M).build(SaveStatus.PreAccepted)).cfk();
        cfk = cfk.update(safeStore, applied(P, A0)).cfk();
        cfk = cfk.update(safeStore, applied(Q, A0, P)).cfk();
        cfk = cfk.maybePrune(0, 0);
        Assertions.assertEquals(P, cfk.prunedBefore().plainTxnId(), "precondition: CFK pruned before p; " + cfk);

        Command stableV = stableSyncPoint(V, L);
        safeCfk.set(cfk);
        cfk = cfk.registerUnmanaged(safeStore, new CommandsForKeyTest.TestSafeCommand(V, null, stableV), REGISTER).cfk();
        Assertions.assertNull(cfk.get(L), "precondition: l not in byId; " + cfk);
        Pruning.LoadingPruned loading = Pruning.find(cfk.loadingPruned, L);
        Assertions.assertNotNull(loading, "precondition: l is loading-pruned; " + cfk);
        Assertions.assertTrue(loading.isVisible, "precondition: loadingPruned(l) is visible (witnessed by a VisibilitySyncPoint)");
        return cfk;
    }

    private static List<TxnId> visit(CommandsForKey cfk, Timestamp startedBefore, Txn.Kind.Kinds kinds)
    {
        List<TxnId> out = new ArrayList<>();
        cfk.visit(startedBefore, kinds, (p1, p2, status, durability, keyOrRange, txnId) -> out.add(txnId), null, null);
        return out;
    }

    @Test
    public void controlLoadingPrunedReportedWhenByIdEntryFollows()
    {
        CommandsForKey cfk = build(true);
        Assertions.assertNotNull(cfk.get(M), "precondition: m retained in byId; " + cfk);
        List<TxnId> visited = visit(cfk, S, S.witnesses());
        Assertions.assertTrue(visited.contains(L), "control: visit(s) = " + visited + " contains loadingPruned l=" + L);
    }

    @Test
    public void loadingPrunedBelowStartedBeforeOmittedWithoutLaterByIdEntry()
    {
        CommandsForKey cfk = build(false);
        List<TxnId> visited = visit(cfk, S, S.witnesses());
        // intended: loadingPruned entries below startedBefore are returned
        Assertions.assertTrue(visited.contains(L), "visit(s=" + S + ") = " + visited
                                                   + " omits loadingPruned l=" + L + " (l < startedBefore < prunedBefore=" + cfk.prunedBefore() + ")");
    }
}
