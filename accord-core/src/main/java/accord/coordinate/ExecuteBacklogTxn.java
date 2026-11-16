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

import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import org.agrona.collections.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Result;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.cfk.ExecuteTxnBacklog;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadyWithDefinition;
import accord.messages.ReadTxnData;
import accord.messages.ReadTxnDataIfReady;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TimestampWithUniqueHlc;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.ActiveEpochs;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionWait;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionDesignated;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.coordinate.ExecuteFlag.NO_WAIT;
import static accord.coordinate.ExecutePath.BACKLOG;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;

/**
 * Opportunistically execute a transaction that is already ready to execute, on behalf of its original coordinator
 * (see {@link accord.local.cfk.ExecuteTxnBacklog}).
 * <p>
 * We only proceed if every replica we read from is already ready to execute; if any is not, we abandon our attempt
 * ({@link NotReady}) without sending any further messages, leaving the transaction to its original coordinator or
 * to whichever backlog executor unblocks it. Similarly, if any replica has already executed the transaction we
 * abandon ({@link Redundant}).
 * <p>
 * If we know the full definition and deps of the transaction (e.g. a single key transaction we replicate) we send a
 * plain {@link ReadTxnData} with {@link ExecuteFlag#NO_WAIT}. Otherwise, we send {@link ReadTxnDataIfReady}, which
 * replies first with each replica's definition and deps, from which we reconstitute the full transaction.
 * <p>
 * We do not send any Stable messages: every replica we read from is already Stable, and any other replica will be
 * sent our deps with {@link accord.messages.Apply}.
 */
public class ExecuteBacklogTxn extends ReadCoordinator<Result, ReadReply>
{
    private static final Logger logger = LoggerFactory.getLogger(ExecuteBacklogTxn.class);

    final FullRoute<?> route;
    final Ballot ballot;
    final Timestamp executeAt;
    final Topologies allTopologies;
    final boolean collectDefinition;
    // whether we may claim the transaction (see BacklogClaim) once we know every replica we read from is ready
    final boolean mayClaim;
    // invoked before we persist the transaction, e.g. to claim the next transaction in the backlog
    final @Nullable Runnable beforePersist;
    // whether we have claimed the transaction, so must try to complete it rather than abandon it when not ready
    private boolean obligated;
    // whether we wait for replicas to become ready (see ProtocolModifiers#coordinatorBacklogExecutionWait), rather than abandon
    private final boolean waitForReady;
    private @Nullable Txn txn;
    private @Nullable Deps deps;

    private @Nullable PartialTxn collectTxn;
    private @Nullable PartialDeps collectDeps;
    private final IntHashSet readFrom = new IntHashSet();
    private Data data;
    private long uniqueHlc;

    private ExecuteBacklogTxn(Node node, Topologies allTopologies, FullRoute<?> route, Ballot ballot, TxnId txnId, @Nullable Txn txn, Timestamp executeAt, @Nullable Deps deps,
                              boolean obligated, boolean mayClaim, @Nullable Runnable beforePersist, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, node.someExclusiveExecutor(), allTopologies.forEpoch(executeAt.epoch()), txnId, route, callback);
        Invariants.require((txn == null) == (deps == null));
        this.route = route;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.allTopologies = allTopologies;
        this.txn = txn;
        this.deps = deps;
        this.collectDefinition = txn == null;
        this.obligated = obligated;
        // a designated executor always waits, as the original coordinator may have deferred to it
        this.waitForReady = obligated || coordinatorBacklogExecutionWait() || coordinatorBacklogExecutionDesignated();
        this.mayClaim = mayClaim;
        this.beforePersist = beforePersist;
    }

    /**
     * @param txn           the full transaction definition, or null if it must be collected from the replicas we read from
     * @param deps          the full stable deps, or null if they must be collected from the replicas we read from
     * @param obligated     if we have already claimed the transaction, so must try to complete it even if not ready
     * @param mayClaim      if we should claim the transaction once we know every replica we read from is ready
     * @param beforePersist invoked before we persist the transaction (e.g. to claim the next transaction in the backlog)
     */
    public static void execute(Node node, FullRoute<?> route, Ballot ballot, TxnId txnId, @Nullable Txn txn, Timestamp executeAt, @Nullable Deps deps,
                               boolean obligated, boolean mayClaim, @Nullable Runnable beforePersist, BiConsumer<? super Result, Throwable> callback)
    {
        ExecuteBacklogTxn execute;
        try
        {
            ActiveEpochs epochs = node.topology().active();
            Topologies all = epochs.reselect(null, QuorumEpochIntersections.preacceptOrCommit,
                                             route, txnId, executeAt, txnId.selectsShards(), QuorumEpochIntersections.stable);
            execute = new ExecuteBacklogTxn(node, all, route, ballot, txnId, txn, executeAt, deps, obligated, mayClaim, beforePersist, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        execute.start();
    }

    @Override
    protected void startOnceInitialised()
    {
        if (deps != null)
            node.agent().coordinatorEvents().onExecuting(txnId, ballot, deps, BACKLOG);
        super.startOnceInitialised();
    }

    @Override
    protected void contact(Id to)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: backlog read from {} on {} ({})", txnId, to, node.id(), node.isNearby(to) ? "nearby" : "remote");
        if (txn == null) node.send(to, new ReadTxnDataIfReady(to, allTopologies, txnId, route, executeAt, waitForReady), executor, this, tracing);
        else node.send(to, new ReadTxnData(to, allTopologies, txnId, route, null, executeAt, executeAt.epoch(), waitForReady ? ExecuteFlags.none() : ExecuteFlags.none().with(NO_WAIT)), executor, this, tracing);
    }

    /**
     * Tell the original coordinator we intend to execute the transaction, after which we must try to complete it
     */
    private void claim(String reason)
    {
        obligated = true;
        node.executeBacklogSink().claim(txnId, reason);
    }

    @Override
    protected Action process(Id from, ReadReply reply)
    {
        if (reply instanceof ReadyWithDefinition)
        {
            ReadyWithDefinition definition = (ReadyWithDefinition) reply;
            if (definition.partialTxn != null) collectTxn = PartialTxn.merge(collectTxn, definition.partialTxn);
            if (definition.partialDeps != null) collectDeps = collectDeps == null ? definition.partialDeps : collectDeps.with(definition.partialDeps);
            if (txn == null && tryReconstitute() && mayClaim && !obligated)
            {
                // every shard has told us it is ready, so we expect to succeed; claim the transaction
                claim("ready");
            }
            return Action.None;
        }

        if (reply.isOk())
        {
            ReadOk ok = (ReadOk) reply;
            // with NO_WAIT, unavailable means some store was not ready (or not safe to read); either way we abandon
            // unless we are waiting for readiness, in which case we try another replica for the unavailable ranges
            if (ok.unavailable != null)
            {
                if (!waitForReady)
                    return abandon(from, reply, NotReady.notReady(txnId, route));
                mergeAndRecord(from, ok);
                return ApprovePartial;
            }

            mergeAndRecord(from, ok);
            return Approve;
        }

        CommitOrReadNack nack = (CommitOrReadNack) reply;
        switch (nack.kind)
        {
            default: throw UnhandledEnum.unknown(nack.kind);
            case InsufficientEpochs: throw UnhandledEnum.invalid(nack.kind);
            // the replica will reply again once it is ready to execute
            case Waiting:
                return waitForReady ? Action.None : abandon(from, reply, NotReady.notReady(txnId, route));
            // the replica has not yet recorded the transaction as Stable, and we do not send Stable messages
            case InsufficientAndWaiting:
                return waitForReady ? Action.TryAlternative : abandon(from, reply, NotReady.notReady(txnId, route));
            // the replica was not ready (without NO_WAIT: has not yet recorded the transaction as Stable)
            case NotReady:
                return obligated || coordinatorBacklogExecutionDesignated() ? Action.Reject : abandon(from, reply, NotReady.notReady(txnId, route));

            case Redundant:
                return abandon(from, reply, new Redundant(txnId, route.homeKey(), executeAt));

            case Rejected:
                return abandon(from, reply, Preempted.preempted(node.agent(), txnId, route.homeKey()));
        }
    }

    private void mergeAndRecord(Id from, ReadOk ok)
    {
        if (ok.data != null)
            data = data == null ? ok.data : data.merge(ok.data);

        if (txnId.is(Txn.Kind.Write) && ok.uniqueHlc > 0)
        {
            Invariants.require(ok.uniqueHlc > executeAt.hlc());
            uniqueHlc = Math.max(uniqueHlc, ok.uniqueHlc);
        }
        readFrom.add(from.id);
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk) reply).unavailable;
    }

    /**
     * @return true iff we have collected a definition and deps covering the whole route, in which case txn and deps are set
     */
    private boolean tryReconstitute()
    {
        if (collectTxn == null || !collectTxn.covers(route) || collectDeps == null || !collectDeps.covers(route))
            return false;

        txn = collectTxn.reconstitute(route);
        deps = collectDeps.reconstitute(route);
        collectTxn = null;
        collectDeps = null;
        node.agent().coordinatorEvents().onExecuting(txnId, ballot, deps, BACKLOG);
        return true;
    }

    private Action abandon(Id from, ReadReply reply, Throwable reason)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: abandoning backlog execution on {} as {} replied {}", txnId, node.id(), from, reply);
        invokeCallback(null, reason);
        return Action.Aborted;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null)
        {
            invokeCallback(null, failure);
            return;
        }

        if (txn == null && !tryReconstitute())
        {
            // we may receive our data before the definition from the same replica if messages are reordered;
            // rather than wait, we simply abandon (as we would for any other failure)
            // TODO (expected): if obligated, wait for the definition (or fetch it)
            if (logger.isTraceEnabled())
                logger.trace("{}: abandoning backlog execution as definition incomplete: {}, {}", txnId, collectTxn, collectDeps);
            invokeCallback(null, NotReady.notReady(txnId, route));
            return;
        }

        // we have every read, so claim the transaction (if we have not already) so that the original coordinator may
        // stop its own attempt, and claim any successor we will execute next
        if (mayClaim && !obligated)
            claim("read");
        if (beforePersist != null)
            beforePersist.run();

        Timestamp executeAt = this.executeAt;
        if (txnId.is(Txn.Kind.Write) && uniqueHlc != 0)
        {
            Invariants.require(uniqueHlc > executeAt.hlc());
            executeAt = new TimestampWithUniqueHlc(executeAt, uniqueHlc);
        }

        // Always compute Writes before Result to provide integrations with a predictable invocation order
        // in case there is shared state between Result and Update.
        Writes writes = txnId.is(Txn.Kind.Write) ? txn.execute(txnId, executeAt, data) : null;
        Result result = txn.result(txnId, executeAt, data);
        // every replica we read from was ready to execute, so is Stable and need not be sent our deps again
        CoordinationFlags flags = CoordinationFlags.withStableSent(CoordinationFlags.none(), readFrom);
        // if the original coordinator is responsible for durability, we need only apply the transaction in our own DC
        if (ExecuteTxnBacklog.persistsLocally(node, txnId, ballot))
            flags = CoordinationFlags.withPersistNearbyOnly(flags);
        CoordinationAdapter<Result> adapter = node.coordinationAdapter(txnId, Standard);
        adapter.persist(node, executor, allTopologies, route, ballot, flags, txnId, txn, executeAt, deps, writes, result, takeCallback());
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.ExecuteBacklog;
    }

    @Override
    public Participants<?> scope()
    {
        return route;
    }

    @Override
    public @Nullable Ballot ballot()
    {
        return ballot;
    }

    @Override
    public String describe()
    {
        return "collectDefinition=" + collectDefinition +
               ", obligated=" + obligated +
               ", waitForReady=" + waitForReady +
               ", executeAt=" + executeAt +
               ", readFrom=" + readFrom +
               ", uniqueHlc=" + uniqueHlc;
    }

    @Override
    public String toString()
    {
        return "ExecuteBacklogTxn{" + txnId + '}';
    }
}
