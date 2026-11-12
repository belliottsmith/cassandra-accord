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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import accord.coordinate.FetchData.FetchResult;
import accord.coordinate.Infer.InvalidIf;
import accord.impl.IntKey;
import accord.impl.IntKey.Raw;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster.Clock;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.IncludingSpecificStoreSelector;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.StoreParticipants;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.WithQuorum;
import accord.topology.Topology;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.routing;

import static accord.utils.async.AsyncChainUtils.getUninterruptibly;

/**
 * Propagate of a recovered PreCommitted/Committed command must install the definition at the home store.
 *
 * A transaction was recovered (BeginRecovery at ballot b > ZERO) and committed by the recovery coordinator on a
 * remote replica. The home replica missed every message (NotDefined locally) and its progress log fetches the
 * transaction; the remote CheckStatus reply carries the definition, executeAt and maxPromised = b.
 *
 * Propagate handles Committed/PreCommitted by first calling Commands.precommit with the
 * remote promised ballot (raising the local promise to b, Command), then falling through to
 * Commands.preaccept, which always uses Ballot.ZERO and is rejected with RejectedBallot by
 * preacceptOrRecover. The outcome is ignored, and updateFetchResult (Propagate 379-396)
 * reports the found knowledge (including the definition) for the owned participants.
 *
 * Intended behaviour: when the Propagate carries the definition to the home (coordinating) store, the store ends up
 * with the definition installed (or at least does not report it as found/achieved). The control case with no
 * recovery ballot (promised == ZERO) shows the same Propagate does install the definition.
 */
public class PropagatePreCommitRejectedPreAcceptTest
{
    private static final Id LOCAL = id(1);
    private static final Id REMOTE = id(2);
    private static final Id COORDINATOR = id(3);
    private static final List<Id> IDS = java.util.Arrays.asList(LOCAL, REMOTE, COORDINATOR);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));
    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    static final class Outcome
    {
        FetchResult fetchResult;
        Throwable failure;
        SaveStatus localStatus;
        Ballot localPromised;
        boolean localDefinitionKnown;
        boolean localHasPartialTxn;
        boolean reportedDefinition;
        boolean ownedReportedAchieved;

        @Override
        public String toString()
        {
            return "local saveStatus=" + localStatus + ", local promised=" + localPromised
                   + ", local definitionKnown=" + localDefinitionKnown + ", local partialTxn!=null=" + localHasPartialTxn
                   + ", fetchResult.foundAtLeast=" + (fetchResult == null ? null : fetchResult.foundAtLeast)
                   + ", fetchResult reports definition=" + reportedDefinition
                   + ", fetchResult.achievedTarget=" + (fetchResult == null ? null : fetchResult.achievedTarget)
                   + ", failure=" + failure;
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void propagateOfRecoveredCommitInstallsDefinitionAtHomeStore() throws Throwable
    {
        Outcome outcome = run(true);
        Assertions.assertNull(outcome.failure, outcome.toString());
        Assertions.assertTrue(outcome.localDefinitionKnown && outcome.localHasPartialTxn,
                              "Propagate carried the definition (remote Committed after recovery ballot) to the home store and reported it, but the definition was not installed: " + outcome);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void propagateReportsDefinitionOnlyIfInstalled() throws Throwable
    {
        Outcome outcome = run(true);
        Assertions.assertNull(outcome.failure, outcome.toString());
        Assertions.assertFalse(outcome.reportedDefinition && outcome.ownedReportedAchieved && !outcome.localDefinitionKnown,
                               "FetchResult reports the definition as found/achieved for the home store, but the store does not have it: " + outcome);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void controlNoRecoveryBallotInstallsDefinition() throws Throwable
    {
        Outcome outcome = run(false);
        Assertions.assertNull(outcome.failure, outcome.toString());
        Assertions.assertTrue(outcome.localDefinitionKnown && outcome.localHasPartialTxn, outcome.toString());
    }

    private static Outcome run(boolean recovered) throws Throwable
    {
        Clock clock = new Clock(100);
        RecordingMessageSink localSink = new RecordingMessageSink(LOCAL, Network.BLACK_HOLE);
        RecordingMessageSink remoteSink = new RecordingMessageSink(REMOTE, Network.BLACK_HOLE);
        Node local = createNode(LOCAL, TOPOLOGY, localSink, clock);
        Node remote = createNode(REMOTE, TOPOLOGY, remoteSink, clock);
        try
        {
            Raw key = IntKey.key(10);
            Txn txn = writeTxn(Keys.of(key));
            FullRoute<?> route = txn.keys().toRoute(key.toUnseekable()); // home key is on LOCAL's store too
            TxnId txnId = clock.idForNode(1, COORDINATOR);
            clock.increment(10);
            Ballot ballot = recovered ? Ballot.fromValues(txnId.epoch(), clock.now(), REMOTE) : Ballot.ZERO;
            PartialTxn partialTxn = txn.slice(FULL_RANGE, true);
            ExecutionContext ctx = ExecutionContext.unsequencedReadWrite(txnId, route, "Test");

            // Remote replica: either recovery (BeginRecovery -> Commands.recover at ballot b) or the original PreAccept,
            // followed by the coordinator's Commit (Commit.apply -> Commands.commit) at that same ballot.
            CommandStore remoteStore = remote.unsafeForKey(key.toUnseekable());
            getUninterruptibly(remoteStore.chain(ctx, safeStore -> {
                StoreParticipants p = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, txnId.epoch());
                SafeCommand sc = safeStore.get(txnId, p);
                Commands.AcceptOutcome pre = recovered ? Commands.recover(safeStore, sc, p, txnId, partialTxn, ballot)
                                                       : Commands.preaccept(safeStore, sc, p, txnId, partialTxn, null, false);
                Assertions.assertEquals(Commands.AcceptOutcome.Success, pre);
                StoreParticipants pe = StoreParticipants.execute(safeStore, route, txnId, txnId.epoch());
                Commands.CommitOutcome commit = Commands.commit(safeStore, sc, pe, SaveStatus.Committed, ballot, txnId, route, partialTxn, txnId, Deps.NONE, Commit.Kind.CommitSlowPath);
                Assertions.assertEquals(Commands.CommitOutcome.Success, commit);
            }));

            // Home replica's fetch: CheckStatus(IncludeInfo.All) served by the remote replica.
            remoteSink.clearHistory();
            new CheckStatus(txnId, route, txnId.epoch(), IncludeInfo.All, null).process(remote, LOCAL, REPLY_CONTEXT);
            Assertions.assertEquals(1, remoteSink.responses.size(), "expected one CheckStatus reply");
            CheckStatusOkFull full = (CheckStatusOkFull) remoteSink.responses.get(0).payload;
            Assertions.assertEquals(ballot, full.maxPromised, "remote reply maxPromised");

            CommandStore localStore = local.unsafeForKey(key.toUnseekable());
            AtomicReference<FetchResult> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            // as FetchData/CheckShards.onSuccess does for the waiting home store (WaitingState.fetch, IncludingSpecificStoreSelector)
            Propagate.propagate(local, txnId, InvalidIf.NotKnownToBeInvalid, txnId.epoch(), WithQuorum.HasQuorum, route, route,
                                new IncludingSpecificStoreSelector(localStore.id()), SaveStatus.PreCommitted.known, full,
                                (r, f) -> { result.set(r); failure.set(f); }, null);

            Outcome outcome = new Outcome();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (result.get() == null && failure.get() == null && System.nanoTime() < deadline)
                Thread.sleep(10);
            Assertions.assertTrue(result.get() != null || failure.get() != null, "Propagate callback not invoked within 10s");
            outcome.fetchResult = result.get();
            outcome.failure = failure.get();
            if (outcome.fetchResult != null)
            {
                outcome.reportedDefinition = outcome.fetchResult.foundAtLeast != null && outcome.fetchResult.foundAtLeast.isDefinitionKnown();
                outcome.ownedReportedAchieved = !outcome.fetchResult.achievedTarget.isEmpty();
            }

            getUninterruptibly(localStore.chain(ctx, safeStore -> {
                Command command = safeStore.ifInitialised(txnId).current();
                outcome.localStatus = command.saveStatus();
                outcome.localPromised = command.promised();
                outcome.localDefinitionKnown = command.known().isDefinitionKnown();
                outcome.localHasPartialTxn = command.partialTxn() != null;
            }));
            return outcome;
        }
        finally
        {
            local.shutdown();
            remote.shutdown();
        }
    }
}
