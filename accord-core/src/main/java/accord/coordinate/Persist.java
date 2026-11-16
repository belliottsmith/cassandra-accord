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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.agrona.collections.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result.PersistableResult;
import accord.api.Scheduler;
import accord.api.TopologySorter.NodeStatus;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.coordinate.tracking.SimpleTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProtocolModifiers.Faults;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.local.cfk.ExecuteTxnBacklog;
import accord.messages.ApplyRelay;
import accord.messages.ApplyRelay.ApplyRelayReply;
import accord.messages.BacklogApplied;
import accord.messages.Callback;
import accord.messages.InformDurable;
import accord.messages.ParticipantsRequest;
import accord.messages.SimpleReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.api.ProtocolModifiers.relayRemoteApply;
import static accord.api.ProtocolModifiers.sendMinimalNoDeps;
import static accord.api.TopologySorter.NodeStatus.HEALTHY;
import static accord.api.TopologySorter.NodeStatus.UNAVAILABLE;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.Kind.Minimal;
import static accord.messages.Apply.Kind.NoDeps;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.topology.SelectShards.ALL;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public abstract class Persist extends AbstractCoordination<FullRoute<?>, Void, ApplyReply, Void> implements ExecuteTxnBacklog.AppliedListener
{
    private static final Logger logger = LoggerFactory.getLogger(Persist.class);

    protected final Ballot ballot;
    protected final Route<?> sendTo;
    protected final Txn txn;
    protected final Timestamp executeAt;
    protected final Deps stableDeps;
    protected final Writes writes;
    protected final PersistableResult result;
    protected final CoordinationFlags flags;
    // TODO (expected): track separate ALL and Quorum, so we can report Universal durability to permit faster GC
    protected final SimpleTracker<?> tracker;
    protected final Apply.Factory factory;
    protected final Apply.Kind applyKind;
    protected final boolean informDurableOnDone;

    /*
     * Persisting only to some replicas, and reporting which have applied the transaction: either nearby replicas, as a
     * designated replica (see CoordinationFlags#persistNearbyOnly); or those we relay to (see ApplyRelay)
     */
    // whether we are (still) persisting only to some replicas
    private boolean partial;
    // the replicas we persist to, if partial
    private @Nullable Predicate<Id> partialTo;
    // if relaying, to whom we report which replicas have applied the transaction (otherwise, we are a designated replica)
    private @Nullable BiConsumer<SortedArrayList<Id>, SortedArrayList<Id>> relayedTo;
    // the number of replicas we await a reply from, if partial
    private int awaitingPartial;
    // whether we await the original coordinator's acknowledgement of our report (see BacklogApplied)
    private boolean awaitingAck;
    // the replicas that have (or have not) applied the transaction, if partial
    private @Nullable List<Id> appliedPartial, failedPartial;
    // the replicas we did not contact as they are UNAVAILABLE (recorded as failures when contacting), if partial
    private @Nullable List<Id> unavailablePartial;

    /*
     * Relaying Apply to remote DCs (see ProtocolModifiers#relayRemoteApply)
     */
    // the number of relays we await a reply from
    private int relaysInFlight;

    /*
     * Deferring Apply to remote replicas (see Agent#backlogRemoteApplyDelay)
     */
    // the remote replicas we have not sent Apply (as we expect a designated replica in their DC to apply the transaction
    // and report it to us), and have not been reported to have applied it; only accessed by our executor
    private @Nullable IntHashSet deferred;
    private @Nullable Scheduler.Scheduled sendDeferred;
    // when we began deferring (per Node#elapsed(MICROSECONDS))
    private long deferredSince;

    protected Persist(Node node, ExclusiveAsyncExecutor executor, Topologies all, TxnId txnId, Ballot ballot, Route<?> sendTo, Txn txn, Timestamp executeAt, Deps stableDeps, Writes writes, PersistableResult result, FullRoute<?> route, CoordinationFlags flags, boolean informDurableOnDone, Apply.Factory factory, Apply.Kind applyKind)
    {
        this(node, executor, all, txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, route, flags, informDurableOnDone, factory, applyKind, QuorumTracker::new, node.agent());
    }

    protected Persist(Node node, ExclusiveAsyncExecutor executor, Topologies all, TxnId txnId, Ballot ballot, Route<?> sendTo, Txn txn, Timestamp executeAt, Deps stableDeps, Writes writes, PersistableResult result, FullRoute<?> route, CoordinationFlags flags, boolean informDurableOnDone, Apply.Factory factory, Apply.Kind applyKind, Function<Topologies, SimpleTracker<?>> trackerFactory, BiConsumer<? super Void, Throwable> callback)
    {
        super(node, executor, txnId, route, all.nodes(), callback);
        this.ballot = ballot;
        this.sendTo = sendTo;
        this.txn = txn;
        this.executeAt = executeAt;
        this.stableDeps = stableDeps;
        this.writes = writes;
        this.result = result;
        this.flags = flags;
        this.tracker = trackerFactory.apply(all);
        this.factory = factory;
        this.informDurableOnDone = informDurableOnDone;
        this.applyKind = applyKind;
        if (flags.persistNearbyOnly())
        {
            this.partial = true;
            this.partialTo = node::isNearby;
        }
        Invariants.require((writes != null) == txnId.is(Txn.Kind.Write), "%s: writes %s", txnId, writes);
    }

    @Override
    public void onSuccessInternal(Id from, int fromIndex, ApplyReply reply)
    {
        switch (reply.kind)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Applied:
                if (partial) onPartialReply(from, true);
                else recordApplied(from);
                break;
            case RaceWithRecovery:
                if (partial)
                {
                    onPartialReply(from, false);
                    break;
                }
                // don't count this towards durability; otherwise it is possible (though very unlikely)
                // for InformDurable to reach a replica, the CommandsForKey to prune this transaction as durably applied
                // and some superseding transaction to be coordinated and not witness it before the in-progress recovery
                // reached another response in its quorum; in this case it may incorrectly determine the transaction should be invalidated
                onFailureInternal(from, fromIndex, null);
                break;
            case Insufficient:
                Invariants.expect(applyKind != Maximal, "Received Insufficient reply from %s, but already sent Maximal", from);
                resend(from, factory.create(Maximal, from, tracker.topologies(), txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(from)));
                break;

            case InsufficientEpochs:
                Invariants.requireArgument(txnId.isSyncPoint());
                Topologies topologies = tracker.topologies();
                try { topologies = node.topology().active().preciseEpochs(scope, reply.minEpoch(), tracker.topologies().currentEpoch(), ALL); }
                catch (TopologyException e) { node.agent().onException(e); }
                Apply maximal = factory.create(Maximal, from, topologies, txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(from));
                if (Faults.persistInsufficientEpochsIsFinal) node.send(from, maximal, tracing);
                else resend(from, maximal);
                break;
        }
    }

    @Override
    public void onFailureInternal(Id from, int fromIndex, Throwable failure)
    {
        if (partial)
        {
            onPartialReply(from, false);
            return;
        }

        // TODO (desired, consider): send knowledge of partial persistence?
        // TODO (expected): we should presumably report total request failure somewhere?
        if (tracker.recordFailure(from) == RequestStatus.Failed)
            finishOnFailure();
    }

    private void recordApplied(Id from)
    {
        if (tracker.recordSuccess(from) == Success)
        {
            // note: we enter this branch whether or not sendTo == route,
            // since we should only invoke Persist with sendTo != route when the remainder of the route is already persisted and truncated
            // but we make this explicit for the caller with informDurableOnDone
            finishWithSuccess(null);
            if (informDurableOnDone)
                InformDurable.informDefault(node, tracker.topologies(), txnId, scope, ballot, executeAt, stableDeps, AllQuorums, tracing);
        }
    }

    /*
     * Persisting only to some replicas: nearby replicas, on behalf of an original coordinator that is responsible for
     * durability (see ProtocolModifiers#coordinatorBacklogExecutionPersistLocally); or those we relay to (see ApplyRelay)
     */

    /**
     * Relay the transaction to {@code forwardTo}, reporting to {@code onDone} which have applied it, on behalf of the
     * coordinator that sent us {@link accord.messages.ApplyRelay}
     */
    void relayTo(SortedArrayList<Id> forwardTo, BiConsumer<SortedArrayList<Id>, SortedArrayList<Id>> onDone)
    {
        this.partial = true;
        this.partialTo = forwardTo::contains;
        this.relayedTo = onDone;
    }

    private void contactPartial()
    {
        Topologies topologies = tracker.topologies();
        appliedPartial = new ArrayList<>();
        failedPartial = new ArrayList<>();
        unavailablePartial = new ArrayList<>();
        for (Id to : nodes())
        {
            if (!partialTo.test(to))
                continue;
            // see AbstractCoordination.contact: we do not contact UNAVAILABLE replicas (except ourselves)
            if (topologies.status(to) != UNAVAILABLE || to.equals(node.id())) ++awaitingPartial;
            else unavailablePartial.add(to);
        }

        // note: UNAVAILABLE replicas are recorded as failures (without being contacted)
        contact(partialTo);
        if (awaitingPartial == 0)
            finishPartial();
    }

    private void onPartialReply(Id from, boolean applied)
    {
        if (applied) appliedPartial.add(from);
        else failedPartial.add(from);
        if (--awaitingPartial == 0)
            finishPartial();
    }

    /**
     * We have heard from every replica we persist to; report which have applied the transaction
     */
    private void finishPartial()
    {
        if (isDone())
            return;

        SortedArrayList<Id> applied = SortedArrayList.copyUnsorted(appliedPartial, Id[]::new);
        List<Id> failed = new ArrayList<>(failedPartial);
        failed.addAll(unavailablePartial);
        SortedArrayList<Id> failedAt = SortedArrayList.copyUnsorted(failed, Id[]::new);
        if (relayedTo != null)
        {
            // the coordinator that asked us to relay remains responsible for durability
            if (logger.isTraceEnabled())
                logger.trace("{}: relayed Apply; applied at {}, failed at {}", txnId, applied, failedAt);
            finishWithSuccess(null);
            relayedTo.accept(applied, failedAt);
            return;
        }
        reportToCoordinator(applied, failedAt);
    }

    /**
     * As designated replica, we have heard from every nearby replica; tell the original coordinator which have applied
     * the transaction, and which we failed to apply it to, so that it may update its durability tracker. If it does not
     * acknowledge our report (e.g. because it has failed, or failed to execute the transaction itself), we persist the
     * transaction to every other replica ourselves.
     * TODO (desired): report sooner (e.g. once a quorum of nearby replicas have replied)
     */
    private void reportToCoordinator(SortedArrayList<Id> applied, SortedArrayList<Id> failedAt)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: reporting applied at {}, failed at {} to {}", txnId, applied, failedAt, txnId.node);
        awaitingAck = true;
        node.send(txnId.node, new BacklogApplied(txnId, applied, failedAt, SortedArrayList.empty()), executor, new Callback<SimpleReply>()
        {
            @Override
            public void onSuccess(Id from, SimpleReply reply)
            {
                awaitingAck = false;
                if (isDone()) return;
                if (reply == SimpleReply.Ok) finishWithSuccess(null);
                else persistGlobally("coordinator replied " + reply);
            }

            @Override
            public void onFailure(Id from, @Nullable Throwable failure)
            {
                awaitingAck = false;
                if (!isDone())
                    persistGlobally(failure == null ? "coordinator did not acknowledge" : "coordinator failed: " + failure);
            }

            @Override
            public boolean onCallbackFailure(Id from, Throwable failure)
            {
                node.agent().onException(failure);
                return true;
            }
        }, tracing);
    }

    /**
     * The original coordinator has not taken responsibility for durability, so persist to every other replica ourselves
     */
    private void persistGlobally(String reason)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: persisting globally as {}", txnId, reason);
        partial = false;
        for (Id id : appliedPartial)
        {
            if (isDone()) return;
            recordApplied(id);
        }
        for (Id id : failedPartial)
        {
            if (isDone()) return;
            if (tracker.recordFailure(id) == RequestStatus.Failed)
                finishOnFailure();
        }
        if (!isDone())
            contactRemote(to -> !node.isNearby(to));
    }

    /*
     * Deferring Apply to remote replicas, as original coordinator (see Agent#backlogRemoteApplyDelay)
     */

    /**
     * @return true iff we may defer sending Apply to remote replicas, as designated replicas in their DCs are expected
     * to execute the transaction, persist it locally, and report to us (see {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionPersistLocally()}),
     * or relay our Apply to them (see {@link accord.api.ProtocolModifiers#relayRemoteApply()}); i.e. we use the standard
     * Apply factory, so that a relay may construct the same messages
     */
    protected boolean mayDeferRemote()
    {
        return false;
    }

    /*
     * Relaying Apply to remote DCs (see ProtocolModifiers#relayRemoteApply)
     */

    /**
     * Contact the replicas {@code include} admits (or every replica, if null) as {@link #contact(Predicate)} does, except
     * that (if enabled) for each other DC we send a single {@link ApplyRelay} to the DC's designated replica, asking it to
     * apply the transaction to the replicas in its DC
     */
    void contactRemote(@Nullable Predicate<Id> include)
    {
        IntHashSet relayed = relay(include, true);
        if (relayed == null) contact(include);
        else contact(to -> (include == null || include.test(to)) && !relayed.contains(to.id));
    }

    /**
     * Send ApplyRelay to the replicas {@code include} admits, grouped by DC, where we can
     * @param awaitReply whether to await the relay's reply (and record it in our tracker), and contact the replicas
     *                   ourselves if it does not reply; otherwise we do not track the outcome
     * @return the replicas we have relayed to, or null if none
     */
    private @Nullable IntHashSet relay(@Nullable Predicate<Id> include, boolean awaitReply)
    {
        if (!relayRemoteApply() || !mayDeferRemote() || partial)
            return null;

        List<List<Id>> groups = null;
        for (Id to : nodes())
        {
            if ((include != null && !include.test(to)) || node.isNearby(to))
                continue;

            if (groups == null) groups = new ArrayList<>();
            List<Id> group = null;
            for (List<Id> g : groups)
            {
                if (node.areNearby(g.get(0), to))
                {
                    group = g;
                    break;
                }
            }
            if (group == null) groups.add(group = new ArrayList<>());
            group.add(to);
        }
        if (groups == null)
            return null;

        Topologies topologies = tracker.topologies();
        IntHashSet relayed = null;
        for (List<Id> group : groups)
        {
            if (group.size() < 2)
                continue;

            Id first = group.get(0);
            Id relay = ExecuteTxnBacklog.designatedExecutorIn(node, scope.homeKey(), id -> node.areNearby(first, id));
            if (relay == null || !group.contains(relay) || topologies.status(relay) != HEALTHY)
                continue;

            sendRelay(relay, group, awaitReply);
            if (relayed == null) relayed = new IntHashSet();
            for (Id id : group)
                relayed.add(id.id);
        }
        return relayed;
    }

    private void sendRelay(Id relay, List<Id> group, boolean awaitReply)
    {
        Topologies topologies = tracker.topologies();
        // the union of the scopes of the Apply messages we would send (the relay uses the same epochs, so constructs the
        // same messages, each of which is a slice of the definition and deps we send it)
        Route<?> participants = null;
        List<Id> noDeps = new ArrayList<>();
        for (Id id : group)
        {
            Route<?> add = ParticipantsRequest.computeScope(id, topologies, scope);
            participants = participants == null ? add : participants.with((Route) add);
            if (applyKind(id, topologies.status(id)) == NoDeps)
                noDeps.add(id);
        }
        SortedArrayList<Id> forwardTo = SortedArrayList.copyUnsorted(group, Id[]::new);
        ApplyRelay send = new ApplyRelay(txnId, scope, ballot, applyKind, executeAt, txn == null ? null : txn.intersecting(participants, true),
                                         stableDeps.intersecting(participants), writes, result, forwardTo, SortedArrayList.copyUnsorted(noDeps, Id[]::new),
                                         topologies.oldestEpoch(), topologies.currentEpoch());
        if (logger.isTraceEnabled())
            logger.trace("{}: relaying Apply via {} to {}", txnId, relay, forwardTo);

        if (!awaitReply)
        {
            node.send(relay, send, tracing);
            return;
        }

        ++relaysInFlight;
        node.send(relay, send, executor, new Callback<ApplyRelayReply>()
        {
            @Override
            public void onSuccess(Id from, ApplyRelayReply reply)
            {
                --relaysInFlight;
                for (Id id : forwardTo)
                {
                    if (isDone()) return;
                    if (reply.appliedAt.contains(id)) recordApplied(id);
                    else if (tracker.recordFailure(id) == RequestStatus.Failed) finishOnFailure();
                }
            }

            @Override
            public void onFailure(Id from, @Nullable Throwable failure)
            {
                --relaysInFlight;
                if (logger.isTraceEnabled())
                    logger.trace("{}: relay {} failed ({}); contacting {} directly", txnId, relay, failure, forwardTo);
                if (!isDone()) contact(forwardTo::contains);
                else sendDirect(forwardTo::contains);
            }

            @Override
            public boolean onCallbackFailure(Id from, Throwable failure)
            {
                node.agent().onException(failure);
                return true;
            }
        }, tracing);
    }

    /**
     * @return true iff we are responsible for the transaction's durability (as original coordinator, see
     * {@link ExecuteTxnBacklog#coordinatorOwnsDurability}), in which case we have contacted every replica we have not
     * deferred sending Apply to
     */
    private boolean tryDeferRemote()
    {
        if (!mayDeferRemote() || !informDurableOnDone || !sendTo.equals(scope))
            return false;

        if (!ExecuteTxnBacklog.coordinatorOwnsDurability(node, txnId, ballot))
            return false;

        long delay = node.agent().backlogRemoteApplyDelay(txnId, MICROSECONDS);
        IntHashSet deferred = new IntHashSet();
        if (delay > 0)
        {
            // TODO (desired): do not defer to DCs without a replica of the home shard, as they have no designated replica
            for (Id to : nodes())
            {
                if (!node.isNearby(to))
                    deferred.add(to.id);
            }
        }

        if (deferred.isEmpty())
        {
            // we contact every replica ourselves, but acknowledge any reports, as we are responsible for durability
            contactRemote(null);
            node.executeBacklogSink().resolveApplied(txnId, null);
            return true;
        }

        this.deferred = deferred;
        this.deferredSince = node.elapsed(MICROSECONDS);
        contact(to -> !deferred.contains(to.id));
        if (logger.isTraceEnabled())
            logger.trace("{}: deferring Apply to {} for {}us", txnId, deferred, delay);
        node.executeBacklogSink().registerAppliedListener(txnId, this);
        if (this.deferred != null)
            sendDeferred = node.scheduler().once(() -> executor.execute(this::sendDeferred), delay, MICROSECONDS);
        return true;
    }

    /**
     * A designated replica has reported that {@code appliedAt} have applied the transaction, that it failed to apply it to
     * {@code failedAt}, and that it did not send Apply to {@code unsentAt} (see {@link BacklogApplied}). We update our
     * tracker as if we had contacted the former two ourselves, and send our Apply to the latter.
     */
    @Override
    public void onReported(SortedArrayList<Id> appliedAt, SortedArrayList<Id> failedAt, SortedArrayList<Id> unsentAt)
    {
        executor.executeMaybeImmediately(() -> {
            if (deferred == null)
                return;

            if (!appliedAt.isEmpty())
                node.agent().coordinatorEvents().onBacklogApplied(txnId, Math.max(0, node.elapsed(MICROSECONDS) - deferredSince), MICROSECONDS);

            for (Id id : appliedAt)
            {
                if (deferred.remove(id.id) && !isDone())
                    recordApplied(id);
            }

            for (Id id : failedAt)
            {
                if (deferred.remove(id.id) && !isDone() && tracker.recordFailure(id) == RequestStatus.Failed)
                    finishOnFailure();
            }

            IntHashSet send = null;
            for (Id id : unsentAt)
            {
                if (deferred.remove(id.id))
                    (send == null ? send = new IntHashSet() : send).add(id.id);
            }
            if (send != null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("{}: sending Apply to {}, which the designated replica did not", txnId, send);
                sendApply(send);
            }

            if (deferred.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("{}: every deferred Apply reported", txnId);
                deferred = null;
                if (sendDeferred != null)
                    sendDeferred.cancel();
                sendDeferred = null;
                node.executeBacklogSink().resolveApplied(txnId, this);
            }
        });
    }

    @Override
    protected boolean awaitsExternal()
    {
        return deferred != null || awaitingAck || relaysInFlight > 0;
    }

    /**
     * We have waited long enough for designated replicas to report they have applied the transaction; send Apply to
     * the remote replicas that have not
     */
    private void sendDeferred()
    {
        sendDeferred = null;
        IntHashSet send = deferred;
        if (send == null)
            return;

        deferred = null;
        node.executeBacklogSink().resolveApplied(txnId, this);
        if (logger.isTraceEnabled())
            logger.trace("{}: sending deferred Apply to {}{}", txnId, send, isDone() ? " after done" : "");
        sendApply(send);
    }

    /**
     * Send Apply to replicas we had deferred sending it to
     */
    private void sendApply(IntHashSet send)
    {
        if (!isDone())
        {
            contactRemote(to -> send.contains(to.id));
            return;
        }

        // we have already achieved durability, so do not await replies
        IntHashSet relayed = relay(to -> send.contains(to.id), false);
        sendDirect(to -> send.contains(to.id) && (relayed == null || !relayed.contains(to.id)));
    }

    /**
     * Send Apply to {@code include} without awaiting replies (as we have already achieved durability)
     */
    private void sendDirect(Predicate<Id> include)
    {
        Topologies topologies = tracker.topologies();
        for (Id to : nodes())
        {
            if (!include.test(to))
                continue;
            NodeStatus status = topologies.status(to);
            if (status == UNAVAILABLE)
                continue;
            // we will not see an Insufficient reply, so do not assume the replica has our deps
            Apply.Kind kind = applyKind(to, status);
            if (kind == NoDeps) kind = Minimal;
            node.send(to, factory.create(kind, to, topologies, txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(to)), tracing);
        }
    }

    private Apply.Kind applyKind(Node.Id to, NodeStatus nodeStatus)
    {
        if (nodeStatus != HEALTHY)
            return Maximal;
        if (applyKind == Minimal && sendMinimalNoDeps() && flags.isStableSent(to))
            return NoDeps;
        return applyKind;
    }

    @Override
    protected void start()
    {
        // a relay has not executed the transaction
        if (relayedTo == null)
            node.agent().coordinatorEvents().onExecuted(txnId, ballot);
        super.start();
        if (partial) contactPartial();
        else if (!tryDeferRemote()) contactRemote(null);
    }

    void contact()
    {
        // applyMinimal is used for transaction execution by the original coordinator so it's important to use
        // Node's Apply factory in case the factory has to do synchronous Apply.
        contact((Predicate<Id>) null);
    }

    void contact(@Nullable Predicate<Node.Id> include)
    {
        // applyMinimal is used for transaction execution by the original coordinator so it's important to use
        // Node's Apply factory in case the factory has to do synchronous Apply.
        contact((to, nodeStatus) -> factory.create(applyKind(to, nodeStatus), to, tracker.topologies(), txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(to)), include);
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Persist;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}
