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

import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Accept;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptNack;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.EpochSupplier;
import accord.primitives.Status;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.local.Node;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;

import static accord.coordinate.CoordinateTransaction.LocalExecuteState.PENDING;
import static accord.coordinate.CoordinateTransaction.LocalExecuteState.SUCCESS;
import static accord.coordinate.CoordinateTransaction.LocalExecuteState.TIMEOUT;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.Propose.NotAccept.proposeAndCommitInvalidate;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateTransaction extends CoordinatePreAccept<Result>
{
    private CoordinateTransaction(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(node, txnId, txn, route);
    }

    public static AsyncResult<Result> coordinate(Node node, FullRoute<?> route, TxnId txnId, Txn txn)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), txn.keys());
        if (mismatch != null)
            return AsyncResults.failure(mismatch);
        CoordinateTransaction coordinate = new CoordinateTransaction(node, txnId, txn, route);
        coordinate.start();
        return coordinate;
    }

    @Override
    void start()
    {
        if (txnId != null && txnId.hasPrivilegedCoordinator()) new LocalExecute().start();
        else super.start();
    }

    @Override
    void onPreAccepted(Topologies topologies, Timestamp executeAt, SortedListMap<Node.Id, PreAcceptOk> oks)
    {
        if (tracker.hasFastPathAccepted())
        {
            Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
            // note: we merge all Deps regardless of witnessedAt. While we only need fast path votes,
            // we must include Deps from fast path votes from earlier epochs that may have witnessed later transactions
            // TODO (desired): we might mask some bugs by merging more responses than we strictly need, so optimise this to optionally merge minimal deps
            executeAdapter().execute(node, topologies, route, FAST, txnId, txn, txnId, deps, settingCallback());
            node.agent().metricsEventsListener().onFastPathTaken(txnId, deps);
        }
        else if (tracker.hasMediumPathAccepted() && txnId.hasMediumPath())
        {
            Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
            proposeAdapter().propose(node, topologies, route, Accept.Kind.MEDIUM, Ballot.ZERO, txnId, txn, txnId, deps, this);
            node.agent().metricsEventsListener().onMediumPathTaken(txnId, deps);
        }
        else
        {
            // TODO (low priority, efficiency): perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //                                  but by sending accept we rule out hybrid fast-path
            // TODO (low priority, efficiency): if we receive an expired response, perhaps defer to permit at least one other
            //                                  node to respond before invalidating
            if (executeAt.is(REJECTED) || executeAt.hlc() - txnId.hlc() >= node.agent().preAcceptTimeout())
            {
                proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt,this);
                node.agent().metricsEventsListener().onRejected(txnId);
            }
            else
            {
                if (PreAccept.rejectExecuteAt(txnId, topologies))
                {
                    proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt, this);
                    node.agent().metricsEventsListener().onRejected(txnId);
                }
                else
                {
                    Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
                    proposeAdapter().propose(node, topologies, route, Accept.Kind.SLOW, Ballot.ZERO, txnId, txn, executeAt, deps, this);
                    node.agent().metricsEventsListener().onSlowPathTaken(txnId, deps);
                }
            }
        }
    }

    protected CoordinationAdapter<Result> proposeAdapter()
    {
        return Adapters.standard();
    }

    protected CoordinationAdapter<Result> executeAdapter()
    {
        return node.coordinationAdapter(txnId, Standard);
    }

    enum LocalExecuteState { PENDING, SUCCESS, TIMEOUT}
    class LocalExecute implements PreLoadContext, MapReduceConsume<SafeCommandStore, PreAcceptReply>, Timeouts.Timeout
    {
        LocalExecuteState state = LocalExecuteState.PENDING;
        Cancellable cancel;
        RegisteredTimeout timeout;

        void start()
        {
            Cancellable cancel = node.mapReduceConsumeLocal(this, route, topologies.oldestEpoch(), topologies.currentEpoch(), this);
            long expiresAt = node.agent().localExpiresAt(txnId, Status.Phase.PreAccept, MICROSECONDS);
            RegisteredTimeout timeout = expiresAt <= 0 ? null : node.timeouts().registerAt(this, expiresAt, MICROSECONDS);
            synchronized (this)
            {
                switch (state)
                {
                    case PENDING:
                        this.cancel = cancel;
                        this.timeout = timeout;
                        return;
                    case TIMEOUT:
                        timeout = null;
                    case SUCCESS:
                        cancel = null;
                }
            }
            if (cancel != null) cancel.cancel();
            if (timeout != null) timeout.cancel();
        }

        @Override
        public void accept(PreAcceptReply result, Throwable failure)
        {
            success();
            if (failure != null)
            {
                CoordinateTransaction.this.accept(null, failure);
            }
            else
            {
                if (result.isOk())
                {
                    CommandStore commandStore = CommandStore.currentOrElseSelect(node, route);
                    PreAcceptOk ok = (PreAcceptOk) result;
                    // TODO (desired): we can probably still process and record fast path votes from peers, just with different quorum requirements
                    boolean hasCoordinatorVote = txnId.equals(ok.witnessedAt);
                    if (!hasCoordinatorVote) fastPathEnabled = false;
                    Deps deps = hasCoordinatorVote && txnId.is(PrivilegedCoordinatorWithDeps) ? ok.deps : null;
                    onSuccess(node.id(), ok);
                    for (Node.Id id : topologies.nodes())
                    {
                        if (id.equals(node.id())) continue;
                        node.send(id, new PreAccept(id, topologies, txnId, txn, deps, hasCoordinatorVote, route), commandStore, CoordinateTransaction.this);
                    }
                }
                else
                {
                    CoordinateTransaction.this.accept(null, new Preempted(txnId, route.homeKey()));
                }
            }

        }

        @Override
        public PreAcceptReply apply(SafeCommandStore safeStore)
        {
            EpochSupplier minEpoch = topologies.size() == 1 ? txnId : EpochSupplier.constant(topologies.oldestEpoch());
            StoreParticipants participants = StoreParticipants.update(safeStore, route, minEpoch.epoch(), txnId, txnId.epoch());
            SafeCommand safeCommand = safeStore.get(txnId, participants);

            Deps deps;
            if (txnId.is(PrivilegedCoordinatorWithDeps))
            {
                deps = PreAccept.calculateDeps(safeStore, txnId, participants, minEpoch, txnId, true);
                if (deps == null)
                    return PreAcceptNack.INSTANCE;

                Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, txn, deps, true, route);
                if (outcome != Success)
                    return PreAcceptNack.INSTANCE;
            }
            else
            {
                Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, txn, null, true, route);
                if (outcome != Success)
                    return PreAcceptNack.INSTANCE;

                deps = PreAccept.calculateDeps(safeStore, txnId, participants, minEpoch, txnId, true);
                if (deps == null)
                    return PreAcceptNack.INSTANCE;
            }

            Command command = safeCommand.current();
            return new PreAcceptOk(txnId, command.executeAt(), deps);
        }

        @Override
        public PreAcceptReply reduce(PreAcceptReply r1, PreAcceptReply r2)
        {
            return PreAcceptReply.reduce(r1, r2);
        }

        @Override
        public void timeout()
        {
            Cancellable cancel;
            synchronized (this)
            {
                if (state != PENDING)
                    return;

                state = TIMEOUT;
                timeout = null;
                if (this.cancel == null)
                    return;
                cancel = this.cancel;
                this.cancel = null;
            }
            cancel.cancel();
        }

        void success()
        {
            RegisteredTimeout cancel;
            synchronized (this)
            {
                if (state != PENDING)
                    return;

                state = SUCCESS;
                this.cancel = null;
                if (timeout == null)
                    return;
                cancel = timeout;
                timeout = null;
            }
            cancel.cancel();
        }

        @Override
        public int stripe()
        {
            return txnId == null ? 0 : txnId.hashCode();
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public KeyHistory keyHistory()
        {
            return KeyHistory.SYNC;
        }

        @Override
        public Unseekables<?> keys()
        {
            return route;
        }
    }
}
