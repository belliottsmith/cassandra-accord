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

import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProgressLog.BlockedUntil;
import accord.api.RoutingKey;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecutePath;
import accord.coordinate.NotReady;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.ActiveEpoch;
import accord.topology.Topology;

import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionMultiKey;
import static accord.coordinate.Coordination.CoordinationKind.COORDINATES_STATE_MACHINE;
import static accord.coordinate.ExecutePath.BACKLOG;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.local.CommandSummaries.SummaryStatus.STABLE;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link NotifySink} installed on a {@link CommandsForKey} (see {@link SafeCommandsForKey#overrideSink(NotifySink)})
 * when this node is coordinating a transaction that has a backlog of STABLE transactions queued behind it.
 * As each transaction in the backlog becomes unblocked we coordinate its execution ourselves, rather than waiting
 * for its (possibly distant) original coordinator to do so.
 * <p>
 * The sink remains installed until the backlog is exhausted (see {@link #notWaiting}) or another local coordinator
 * decides there is no eligible backlog (see {@link #maybeInstall}). Transactions we decline to execute
 * (e.g. our own, or multi-key transactions we cannot coordinate) do not terminate the backlog: once they apply
 * we will be notified of the next transaction.
 * <p>
 * We only execute transactions that are already ready to execute: if a transaction is still waiting on some other
 * key or dependency, whoever unblocks it is a better candidate to execute it (and the original coordinator remains
 * waiting to do so in any case). So, a transaction must be {@link SaveStatus#ReadyToExecute} in every local command
 * store that executes it, and remote replicas are read with {@link accord.coordinate.ExecuteFlag#NO_WAIT} so that
 * we abandon our attempt if any is not ready (see {@code ExecuteTxn.abandonNotReady}).
 * <p>
 * Multi-key transactions (if enabled by {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionMultiKey()})
 * may only be executed if this node replicates all of their participants, so that we hold the complete definition
 * and dependencies, which we collect from each local command store.
 * <p>
 * TODO (expected): support multi-key transactions we do not fully replicate, by collecting their definition from
 *   the replicas we read from (see EXECUTE_BACKLOG_PLAN.md)
 * TODO (expected): notify/yield to competing coordinators and backlog executors (see EXECUTE_BACKLOG_PLAN.md)
 */
public class ExecuteTxnBacklog implements NotifySink
{
    // TODO (desired): configurable
    private static final long ACTIVE_EXPIRES_AFTER_NANOS = MINUTES.toNanos(1);
    private static final int PRUNE_ACTIVE_AT_SIZE = 1 << 10;

    private static final Logger logger = LoggerFactory.getLogger(ExecuteTxnBacklog.class);

    final Node node;

    /**
     * Transactions we have decided to execute, mapped to the time (per {@link Node#elapsed}) we made this decision,
     * so that we do not execute the same transaction twice (e.g. because we have been notified by multiple keys or
     * command stores). Entries are removed on completion, but we also expire them in case we somehow fail to clean up.
     */
    private final ConcurrentHashMap<TxnId, Long> active = new ConcurrentHashMap<>();

    public ExecuteTxnBacklog(Node node)
    {
        this.node = node;
    }

    /**
     * @return true iff {@code txnId} is a kind of transaction that may be executed as part of a backlog
     */
    public static boolean mayExecute(TxnId txnId)
    {
        return txnId.is(Key)
               && (txnId.is(Read) || txnId.is(Write))
               && (txnId.is(SingleKey) || coordinatorBacklogExecutionMultiKey());
    }

    /**
     * @return true iff the next transaction to execute after {@code executeAt} is STABLE and eligible for backlog execution
     */
    public static boolean hasBacklogAfter(CommandsForKey cfk, Timestamp executeAt)
    {
        int i = cfk.unappliedCommittedIndexOf(executeAt);
        if (i < 0) i = -1 - i;
        else ++i;

        if (i >= cfk.committedSize())
            return false;

        TxnInfo next = cfk.committedByExecuteAt(i);
        return next.is(STABLE) && mayExecute(next);
    }

    /**
     * Invoked by a local coordinator that is about to execute {@code executeAt} on each of {@code keys};
     * install this node's backlog sink if there is an eligible backlog queued behind it, and remove it otherwise.
     */
    public static void maybeInstall(Node node, SafeCommandStore safeStore, Participants<?> keys, Timestamp executeAt)
    {
        for (int i = 0, size = keys.size() ; i < size ; ++i)
        {
            SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(keys.get(i).asRoutingKey());
            if (safeCfk == null)
                continue;

            if (hasBacklogAfter(safeCfk.current(), executeAt)) safeCfk.overrideSink(node.executeBacklogSink());
            else safeCfk.overrideSink(null);
        }
    }

    @Override
    public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
    {
        DefaultNotifySink.INSTANCE.notWaiting(safeStore, txnId, key, uniqueHlc);
        if (!mayExecute(txnId) || txnId.node.equals(node.id()))
            return;

        // if nothing eligible is queued behind this transaction then the backlog is exhausted once it is executed,
        // so stop listening on this key; a later coordinator may decide to install us again
        SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(key);
        if (safeCfk != null && safeCfk.overrideSink() == this)
        {
            CommandsForKey cfk = safeCfk.current();
            TxnInfo info = cfk.get(txnId);
            if (info != null && !hasBacklogAfter(cfk, info.executeAt))
                safeCfk.overrideSink(null);
        }

        execute(safeStore.commandStore(), txnId);
    }

    @Override
    public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
    {
        DefaultNotifySink.INSTANCE.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
    }

    private void execute(CommandStore commandStore, TxnId txnId)
    {
        if (!tryMarkActive(txnId))
            return;

        commandStore.execute(ExecutionContext.unsequenced(txnId, "Load for ExecuteBacklog"), safeStore -> {
            if (!tryExecute(safeStore, txnId))
                active.remove(txnId);
        }, (success, fail) -> {
            if (fail != null)
            {
                active.remove(txnId);
                node.agent().onException(fail);
            }
        });
    }

    /**
     * @return true if we have started (or may yet start) execution
     */
    private boolean tryExecute(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.ifInitialised(txnId);
        if (safeCommand == null)
            return false;

        Command command = safeCommand.current();
        if (!isCandidate(command))
            return false;

        if (!txnId.is(SingleKey) && !locallyReplicatesAll(command.route(), txnId, command.executeAt()))
            return false;

        if (coversRoute(command))
        {
            // fast path: this store holds everything we need (e.g. a single key transaction)
            FullRoute<?> route = (FullRoute<?>) command.route();
            start(txnId, route, command.partialTxn().reconstitute(route), command.executeAt(),
                  command.partialDeps().reconstitute(route), command.promised(), command.acceptedOrCommitted());
        }
        else
        {
            new Gather(txnId, (FullRoute<?>) command.route(), command.executeAt()).begin();
        }
        return true;
    }

    private static boolean isCandidate(Command command)
    {
        return command.saveStatus() == ReadyToExecute
               && executesLocally(command)
               && Route.isFullRoute(command.route());
    }

    private static boolean executesLocally(Command command)
    {
        Participants<?> stillExecutes = command.participants().stillExecutes();
        return stillExecutes != null && !stillExecutes.isEmpty();
    }

    private static boolean coversRoute(Command command)
    {
        FullRoute<?> route = (FullRoute<?>) command.route();
        PartialTxn txn = command.partialTxn();
        PartialDeps deps = command.partialDeps();
        return txn != null && deps != null && txn.covers(route) && deps.covers(route);
    }

    private boolean locallyReplicatesAll(Route<?> route, TxnId txnId, Timestamp executeAt)
    {
        return locallyReplicatesAll(route, txnId.epoch()) && (executeAt.epoch() == txnId.epoch() || locallyReplicatesAll(route, executeAt.epoch()));
    }

    private boolean locallyReplicatesAll(Route<?> route, long epoch)
    {
        ActiveEpoch active = node.topology().active().ifExists(epoch);
        if (active == null)
            return false;

        Topology.NodeInfo info = active.all().nodeInfo(node.id());
        return info != null && info.ranges.containsAll(route);
    }

    private boolean tryMarkActive(TxnId txnId)
    {
        if (node.coordinations().anyMatch(txnId, COORDINATES_STATE_MACHINE))
            return false;

        long now = node.elapsed(NANOSECONDS);
        if (active.size() >= PRUNE_ACTIVE_AT_SIZE)
            active.values().removeIf(at -> now - at > ACTIVE_EXPIRES_AFTER_NANOS);

        Long prev = active.putIfAbsent(txnId, now);
        return prev == null || (now - prev > ACTIVE_EXPIRES_AFTER_NANOS && active.replace(txnId, prev, now));
    }

    private void start(TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Ballot promised, Ballot acceptedOrCommitted)
    {
        ExecutePath path = promised.equals(Ballot.ZERO) ? BACKLOG : RECOVER;
        Node.Id notify = (promised.equals(Ballot.ZERO) ? txnId : promised).node;
        if (notify.equals(node.id()))
        {
            active.remove(txnId);
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("{}: executing backlog via {} with ballot {}", txnId, path, acceptedOrCommitted);
        node.withEpochAtLeast(executeAt.epoch(), null, (ignore, fail) -> {
            active.remove(txnId);
            node.agent().onException(fail);
        }, () -> {
            node.agent().coordinatorEvents().onRecoveryStarted(txnId, promised);
            Adapters.standard().execute(node, node.someExclusiveExecutor(), null, route, acceptedOrCommitted, path, CoordinationFlags.none(), txnId, txn, executeAt, deps, deps, (result, fail) -> {
                active.remove(txnId);
                if (logger.isTraceEnabled())
                    logger.trace("{}: executed backlog with outcome {}", txnId, fail == null ? "success" : fail.getClass().getSimpleName());
                if (fail == null) node.reportLocalExecution(txnId, route, promised, null, null, result);
                else if (!(fail instanceof NotReady)) node.agent().onException(fail);
            });
        });
    }

    static class Gathered
    {
        static final Gathered INELIGIBLE = new Gathered(null, null, Ballot.ZERO, Ballot.ZERO);

        final @Nullable PartialTxn txn;
        final @Nullable PartialDeps deps;
        final Ballot promised;
        final Ballot acceptedOrCommitted;

        Gathered(@Nullable PartialTxn txn, @Nullable PartialDeps deps, Ballot promised, Ballot acceptedOrCommitted)
        {
            this.txn = txn;
            this.deps = deps;
            this.promised = promised;
            this.acceptedOrCommitted = acceptedOrCommitted;
        }

        Gathered merge(Gathered that)
        {
            if (this == INELIGIBLE || that == INELIGIBLE)
                return INELIGIBLE;

            return new Gathered(PartialTxn.merge(this.txn, that.txn),
                                this.deps == null ? that.deps : that.deps == null ? this.deps : this.deps.with(that.deps),
                                Ballot.max(this.promised, that.promised),
                                Ballot.max(this.acceptedOrCommitted, that.acceptedOrCommitted));
        }
    }

    /**
     * Collect the definition and dependencies of a multi-key transaction from every local command store,
     * so that we may coordinate its execution.
     */
    class Gather extends MapReduceConsumeCommandStores<FullRoute<?>, Gathered>
    {
        final TxnId txnId;
        final Timestamp executeAt;

        Gather(TxnId txnId, FullRoute<?> route, Timestamp executeAt)
        {
            super(route);
            this.txnId = txnId;
            this.executeAt = executeAt;
        }

        void begin()
        {
            node.commandStores().mapReduceConsume(txnId.epoch(), executeAt.epoch(), this);
        }

        @Override
        protected Gathered applyInternal(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.ifInitialised(txnId);
            if (safeCommand == null)
                return new Gathered(null, null, Ballot.ZERO, Ballot.ZERO);

            // every store that executes the transaction must be ready to execute it;
            // a store that does not execute it (e.g. it participates only in an earlier epoch) need only be stable
            Command command = safeCommand.current();
            SaveStatus saveStatus = command.saveStatus();
            if (saveStatus != ReadyToExecute && (saveStatus != Stable || executesLocally(command)))
                return Gathered.INELIGIBLE;

            return new Gathered(command.partialTxn(), command.partialDeps(), command.promised(), command.acceptedOrCommitted());
        }

        @Override
        protected Gathered refuseInternal(SafeCommandStore safeStore)
        {
            return Gathered.INELIGIBLE;
        }

        @Override
        public Gathered reduce(Gathered o1, Gathered o2)
        {
            return o1.merge(o2);
        }

        @Override
        public void accept(Gathered result, Throwable failure)
        {
            if (failure != null)
            {
                active.remove(txnId);
                node.agent().onException(failure);
                return;
            }

            FullRoute<?> route = scope;
            if (result == null || result == Gathered.INELIGIBLE
                || result.txn == null || !result.txn.covers(route)
                || result.deps == null || !result.deps.covers(route))
            {
                if (logger.isTraceEnabled())
                    logger.trace("{}: not executing backlog, as {}", txnId, result == Gathered.INELIGIBLE ? "not ready locally" : "local definition incomplete");
                active.remove(txnId);
                return;
            }

            start(txnId, route, result.txn.reconstitute(route), executeAt, result.deps.reconstitute(route), result.promised, result.acceptedOrCommitted);
        }

        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public String reason()
        {
            return "Gather for ExecuteBacklog";
        }
    }
}
