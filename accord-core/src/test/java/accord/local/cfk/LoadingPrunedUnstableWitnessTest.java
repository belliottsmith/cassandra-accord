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
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.Result.PersistableResult;
import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.mock.MockStore;
import accord.local.Command;
import accord.local.CommandBuilder;
import accord.local.CommandSummaries.IsDep;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;

import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.SingleKey;

/**
 * A witness of a loading pruned dependency must retain its UNSTABLE flag.
 *
 * A CFK has pruned past d (d never seen locally). X is Accepted with deps containing d|UNSTABLE, as
 * Propose.mergeNewDeps produces via Deps.markUnstableBefore(txnId). Updating.computeInfoAndAdditions
 * records d|UNSTABLE in X.missing but strips UNSTABLE from d before Pruning.loadPruned, so
 * loadingPruned(d).witnessedBy = [X] without the UNSTABLE flag. Until LoadPruned(d) runs, the
 * recovery visitor CommandsForKey.visit(d, ...) then reports X as IS_PROPOSED_OR_STABLE_DEP, while X's
 * own state (missing contains d) and the post-load answer say IS_NOT_PROPOSED_OR_STABLE_DEP.
 */
public class LoadingPrunedUnstableWitnessTest
{
    private static final RoutingKey KEY = IntKey.routing(1);
    private static final Keys KEYS = IntKey.keys(1);
    private static final Ranges RANGES = Ranges.of(IntKey.range(0, 2));
    private static final FullRoute<?> KEY_ROUTE = KEYS.toRoute(IntKey.routing(1));
    private static final Txn KEY_TXN = new Txn.InMemory(KEYS, MockStore.read(KEYS), MockStore.QUERY);
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
        for (int i = 0 ; i < waitingOn.txnIdCount() ; ++i) // all deps applied (key-managed deps are tracked by the CFK, not WaitingOn)
            waitingOn.removeWaitingOn(waitingOn.txnId(i));
        return builder(txnId).durability(AllQuorums) // durably applied at a quorum: eligible for pruning
                             .partialDeps(d.intersecting(KEY_ROUTE))
                             .executeAt(txnId)
                             .waitingOn(waitingOn.build())
                             .writes(new Writes(txnId, txnId, Keys.EMPTY, null))
                             .result(new PersistableResult(){})
                             .build(SaveStatus.Applied);
    }

    private static List<Object[]> visit(CommandsForKey cfk, TxnId testTxnId)
    {
        List<Object[]> out = new ArrayList<>();
        cfk.visit(testTxnId, testTxnId.witnessedBy(), (keyOrRange, txnId, executeAt, status, dep, durability) -> {
            out.add(new Object[]{ txnId, status, dep });
            return true;
        });
        return out;
    }

    private static IsDep depOf(List<Object[]> visited, TxnId txnId)
    {
        for (Object[] v : visited)
            if (v[0].equals(txnId)) return (IsDep) v[2];
        return null;
    }

    @Test
    public void unstableWitnessOfPrunedDepIsReportedAsStable()
    {
        TxnId w0 = write(10), d = write(15), w1 = write(20), w2 = write(30), x = write(40);
        CommandsForKeyTest.TestSafeCommandsForKey safeCfk = new CommandsForKeyTest.TestSafeCommandsForKey(new CommandsForKey(KEY));
        // safeStore is only consulted by InternalStatus.from for PREACCEPTED_WITH_DEPS, which this test never uses
        CommandsForKeyTest.TestSafeStore safeStore = new CommandsForKeyTest.TestSafeStore(null, null, safeCfk);

        // 1. three applied writes; d is never witnessed on this store
        CommandsForKey cfk = safeCfk.current();
        cfk = cfk.update(safeStore, applied(w0)).cfk();
        cfk = cfk.update(safeStore, applied(w1, w0)).cfk();
        cfk = cfk.update(safeStore, applied(w2, w0, w1)).cfk();

        // 2. prune (Pruning.maybePrune with interval 0 / delta 0): prune point w1 > d
        cfk = cfk.maybePrune(0, 0);
        Assertions.assertEquals(w1, cfk.prunedBefore().plainTxnId(), "precondition: CFK pruned before w1");
        Assertions.assertNull(cfk.get(d), "precondition: d unknown locally");

        // 3. Accept(X) with deps as Propose.mergeNewDeps builds them: markUnstableBefore(X) => d|UNSTABLE
        Deps xDeps = keyDeps(d, w2).markUnstableBefore(x);
        Assertions.assertTrue(xDeps.keyDeps.txnIdWithFlags(0).is(UNSTABLE), "precondition: X's dep d is UNSTABLE");
        Command acceptedX = builder(x).partialDeps(xDeps.intersecting(KEY_ROUTE))
                                      .executeAt(x)
                                      .build(SaveStatus.AcceptedMediumWithDefinition);
        CommandsForKeyUpdate upd = cfk.update(safeStore, acceptedX);
        cfk = upd.cfk();

        TxnInfo xInfo = cfk.get(x);
        TxnId[] xMissing = xInfo.missing();
        boolean xMissesD = Arrays.binarySearch(xMissing, d) >= 0;
        TxnId[] loadingWitnesses = Pruning.loadingPrunedFor(cfk.loadingPruned, d, null);

        // 4. BeginRecovery(d) arrives before LoadPruned(d) has run (it is a separate PostProcess task)
        IsDep beforeLoad = depOf(visit(cfk, d), x);

        // 5. LoadPruned(d) completes: the store loads d (never seen => NotDefined) and calls back into the CFK
        Command dNotDefined = builder(d).build(SaveStatus.NotDefined);
        CommandsForKey loaded = cfk.callback(safeStore, dNotDefined).cfk();
        IsDep afterLoad = depOf(visit(loaded, d), x);

        String evidence = "X.missing=" + Arrays.toString(xMissing)
                          + " loadingPruned(d).witnessedBy=" + Arrays.toString(loadingWitnesses)
                          + " visit(d) before LoadPruned: X->" + beforeLoad
                          + ", after LoadPruned: X->" + afterLoad;

        Assertions.assertTrue(xMissesD, "precondition: X records d|UNSTABLE as missing; " + evidence);
        Assertions.assertNotNull(loadingWitnesses, "precondition: d is loading-pruned; " + evidence);
        Assertions.assertEquals(IsDep.IS_NOT_PROPOSED_OR_STABLE_DEP, afterLoad, "post-load answer; " + evidence);
        // intended: X only holds d|UNSTABLE, so recovery evidence must not claim X stably witnessed d
        Assertions.assertEquals(IsDep.IS_NOT_PROPOSED_OR_STABLE_DEP, beforeLoad,
                                "visit(d) while d is loading-pruned must agree with X.missing; " + evidence);
    }
}
