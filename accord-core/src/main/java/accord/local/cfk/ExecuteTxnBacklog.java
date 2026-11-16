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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.LocalListeners;
import accord.api.ProtocolModifiers;
import accord.api.ProtocolModifiers.CoordinatorBacklogExecution;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Timeouts;
import accord.coordinate.Coordination.CoordinationKind;
import accord.coordinate.ExecuteBacklogTxn;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.TinyEnumSet;
import accord.coordinate.Exhausted;
import accord.coordinate.ExecuteTxn;
import accord.coordinate.NotReady;
import accord.coordinate.Redundant;
import accord.coordinate.Timeout;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.messages.BacklogClaim;
import accord.messages.ReplyContext;
import accord.messages.SimpleReply;
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
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.Topology;

import static accord.api.ProtocolModifiers.coordinatorBacklogExecution;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionClaims;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionMultiKey;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionYieldToNearby;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionLinger;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionWait;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionDesignated;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionGrace;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionPerDc;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionPersistLocally;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static accord.coordinate.Coordination.CoordinationKind.COORDINATES_STATE_MACHINE;
import static accord.coordinate.Coordination.CoordinationKind.MaybeRecover;
import static accord.local.CommandSummaries.SummaryStatus.STABLE;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static accord.messages.MessageType.StandardMessage.READ_REQ;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link NotifySink} ({@link KeySink}) installed on a {@link CommandsForKey} (see {@link SafeCommandsForKey#overrideSink(NotifySink)})
 * when this node is coordinating a transaction that has a backlog of STABLE transactions queued behind it.
 * As each transaction in the backlog becomes unblocked we coordinate its execution ourselves, rather than waiting
 * for its (possibly distant) original coordinator to do so. Having executed a transaction as part of a backlog, we
 * also install our sink on each of its other keys that we replicate, as we are then best placed to continue the
 * backlog on those keys too (since the transaction's successors there will become ready here first).
 * <p>
 * The sink remains installed until we catch up with the backlog (see {@link #notWaiting}), i.e. find no eligible STABLE
 * transaction queued behind the one we are notified of, or another local coordinator decides there is no eligible
 * backlog (see {@link #maybeInstall}). Under steady contention we typically catch up, as we execute transactions
 * faster than they arrive, so we may continue to listen for some time after we last found a backlog (see
 * {@link ProtocolModifiers#coordinatorBacklogExecutionLinger}). Transactions we decline to execute
 * (e.g. our own, or multi-key transactions we cannot coordinate) do not terminate the backlog: once they apply
 * we will be notified of the next transaction.
 * <p>
 * We only start to execute transactions that are ready to execute in the store that notifies us
 * ({@link SaveStatus#ReadyToExecute}). By default, {@link ExecuteBacklogTxn} then abandons the attempt if any replica
 * it reads from is not ready: if a transaction is still waiting on some other key or dependency, whoever unblocks it
 * may be a better candidate to execute it. But since we are typically caught up with the backlog, a replica that is not
 * ready is typically about to be, so we may instead wait (see {@link ProtocolModifiers#coordinatorBacklogExecutionWait}).
 * <p>
 * If the notifying store holds the full definition and deps (e.g. a single key transaction) we provide them,
 * otherwise (if enabled by {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionMultiKey()})
 * {@link ExecuteBacklogTxn} collects them from the replicas it reads from.
 * <p>
 * <p>
 * If enabled ({@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionClaims()}) we tell a transaction's
 * original coordinator that we intend to execute it ({@link BacklogClaim}), so that it may pause its own attempt,
 * awaiting our {@link accord.messages.RemoteSuccess} (and resuming if none arrives promptly). Having claimed a
 * transaction we are obligated to execute it, i.e. we wait for it to become ready rather than abandon our attempt.
 * We claim:
 * <ul>
 *   <li>a multi-key transaction once every replica we read from has reported it is ready (see {@link ExecuteBacklogTxn});</li>
 *   <li>any other transaction once we have received all of our reads, before we persist it;</li>
 *   <li>the next single key transaction queued behind the one we are executing, before we persist the latter
 *       (as it will be ready to execute once we have done so), so that its coordinator learns of our claim
 *       before its own reads complete.</li>
 * </ul>
 * The original coordinator side of this protocol is also implemented here: received claims are recorded
 * (for coordinators that have not yet reached execution), executions paused, and results retained (for coordinators
 * that receive our RemoteSuccess before reaching execution).
 * <p>
 * <p>
 * We yield to a transaction's original coordinator if it is nearby (see {@link Node#isNearby}) and plausibly still
 * coordinating the transaction, as it will receive its replies about as promptly as we would. If it is expected to
 * continue the backlog behind the transaction itself, we also stop listening to the key.
 * <p>
 * With {@link ProtocolModifiers#coordinatorBacklogExecutionPersistLocally()}, a designated replica persists the
 * transactions it executes only to the replicas in its DC, and reports to the original coordinator which have applied
 * them ({@link accord.messages.BacklogApplied}). The original coordinator remains responsible for durability: it
 * executes the transaction itself, and defers its Apply to remote replicas
 * (see {@link ProtocolModifiers#coordinatorBacklogExecutionRemoteApplyDelay}), sending it only to those not reported
 * to have applied the transaction. The original coordinator side of this protocol is also implemented here.
 */
public class ExecuteTxnBacklog
{
    // TODO (desired): configurable
    private static final long ACTIVE_EXPIRES_AFTER_NANOS = MINUTES.toNanos(1);
    private static final int PRUNE_ACTIVE_AT_SIZE = 1 << 10;

    private static final Logger logger = LoggerFactory.getLogger(ExecuteTxnBacklog.class);
    private static final TinyEnumSet<CoordinationKind> COORDINATES_STATE_MACHINE_EXCEPT_MAYBE_RECOVER = COORDINATES_STATE_MACHINE.not(TinyEnumSet.of(MaybeRecover));

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
        return nextBacklog(cfk, executeAt) != null;
    }

    /**
     * @return the next transaction to execute after {@code executeAt} if it is STABLE and eligible for backlog execution
     */
    private static @Nullable TxnInfo nextBacklog(CommandsForKey cfk, Timestamp executeAt)
    {
        int i = cfk.unappliedCommittedIndexOf(executeAt);
        if (i < 0) i = -1 - i;
        else ++i;

        if (i >= cfk.committedSize())
            return null;

        TxnInfo next = cfk.committedByExecuteAt(i);
        return next.is(STABLE) && mayExecute(next) ? next : null;
    }

    /**
     * @return why there is no eligible backlog after {@code executeAt}, for logging
     */
    private static String describeNoBacklog(CommandsForKey cfk, Timestamp executeAt)
    {
        int i = cfk.unappliedCommittedIndexOf(executeAt);
        if (i < 0) i = -1 - i;
        else ++i;

        if (i >= cfk.committedSize())
            return "no committed successor";

        TxnInfo next = cfk.committedByExecuteAt(i);
        if (!next.is(STABLE)) return "successor " + next.plainTxnId() + " not stable: " + next.status();
        return "successor " + next.plainTxnId() + " not eligible";
    }

    /**
     * @return true iff we should tell the original coordinator of {@code txnId} that we intend to execute it
     */
    private boolean mayClaim(TxnId txnId, Ballot promised)
    {
        return coordinatorBacklogExecutionClaims() && !txnId.node.equals(node.id()) && promised.equals(Ballot.ZERO);
    }

    /**
     * Our sink on a particular key, which records when we last found a backlog there, so that we may continue to
     * listen for a while once we have caught up (see {@link ProtocolModifiers#coordinatorBacklogExecutionLinger})
     */
    static final class KeySink implements NotifySink
    {
        final ExecuteTxnBacklog backlog;
        // per Node#elapsed(MICROSECONDS); only accessed by the owning command store
        long foundBacklogAt;

        KeySink(ExecuteTxnBacklog backlog, long foundBacklogAt)
        {
            this.backlog = backlog;
            this.foundBacklogAt = foundBacklogAt;
        }

        @Override
        public void notWaiting(SafeCommandStore safeStore, TxnId txnId, RoutingKey key, long uniqueHlc)
        {
            DefaultNotifySink.INSTANCE.notWaiting(safeStore, txnId, key, uniqueHlc);
            backlog.notWaiting(this, safeStore, txnId, key);
        }

        @Override
        public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
        {
            DefaultNotifySink.INSTANCE.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
        }

        @Override
        public String toString()
        {
            return "ExecuteTxnBacklog.KeySink{" + backlog.node.id() + '}';
        }
    }

    private @Nullable KeySink sink(SafeCommandsForKey safeCfk)
    {
        NotifySink sink = safeCfk.overrideSink();
        return sink instanceof KeySink && ((KeySink) sink).backlog == this ? (KeySink) sink : null;
    }

    /**
     * Install our sink on {@code safeCfk}, or refresh it if already installed
     */
    private void install(SafeCommandsForKey safeCfk)
    {
        long now = node.elapsed(MICROSECONDS);
        KeySink sink = sink(safeCfk);
        if (sink != null) sink.foundBacklogAt = now;
        else safeCfk.overrideSink(new KeySink(this, now));
    }

    /**
     * Invoked by a local coordinator that is about to execute {@code executeAt} on each of {@code keys};
     * install this node's backlog sink if there is an eligible backlog queued behind it. Otherwise, if we do not
     * linger (see {@link ProtocolModifiers#coordinatorBacklogExecutionLinger}), remove any sink; if we do, any
     * sink we have installed will expire by itself.
     */
    public static void maybeInstall(Node node, SafeCommandStore safeStore, Participants<?> keys, Timestamp executeAt)
    {
        ExecuteTxnBacklog backlog = node.executeBacklogSink();
        boolean linger = coordinatorBacklogExecutionLinger(MICROSECONDS) > 0;
        for (int i = 0, size = keys.size() ; i < size ; ++i)
        {
            SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(keys.get(i).asRoutingKey());
            if (safeCfk == null)
                continue;

            if (hasBacklogAfter(safeCfk.current(), executeAt)) backlog.install(safeCfk);
            else if (!linger) safeCfk.overrideSink(null);
        }
    }

    /**
     * Having executed {@code txnId} as part of a backlog, we are best placed to continue the backlog on each of its keys,
     * so install our sink on every key of {@code route} we replicate (not only the key that notified us). We do so
     * before we persist (so typically before we apply the transaction locally, and so are notified of its successors).
     */
    private void installOnEveryKey(TxnId txnId, FullRoute<?> route, Timestamp executeAt)
    {
        Participants<?> participants = route.participants();
        long epoch = executeAt.epoch();
        node.commandStores().forEach("Install ExecuteBacklog", txnId, participants, epoch, epoch, safeStore -> {
            Participants<?> owned = participants.slice(safeStore.ranges().allAt(epoch), Minimal);
            for (int i = 0, size = owned.size() ; i < size ; ++i)
            {
                SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(owned.get(i).asRoutingKey());
                if (safeCfk != null)
                    install(safeCfk);
            }
        }).begin(node.agent());
    }

    private void notWaiting(KeySink sink, SafeCommandStore safeStore, TxnId txnId, RoutingKey key)
    {
        if (!mayExecute(txnId) || txnId.node.equals(node.id()))
            return;

        boolean yield = shouldYieldTo(txnId);
        TxnId claimAhead = null;
        SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(key);
        if (safeCfk != null && safeCfk.overrideSink() == sink)
        {
            CommandsForKey cfk = safeCfk.current();
            TxnInfo info = cfk.get(txnId);
            if (info != null)
            {
                TxnInfo next = nextBacklog(cfk, info.executeAt);
                long now = node.elapsed(MICROSECONDS);
                if (next != null)
                    sink.foundBacklogAt = now;

                // if nothing eligible is queued behind this transaction then we have caught up with the backlog; unless
                // we linger, stop listening on this key (a later coordinator may decide to install us again)
                boolean caughtUp = next == null && now - sink.foundBacklogAt >= coordinatorBacklogExecutionLinger(MICROSECONDS);
                if (caughtUp || (yield && mayContinueBacklog(txnId, key)))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("{}: releasing backlog sink on {} ({})", txnId, key, caughtUp ? describeNoBacklog(cfk, info.executeAt) : "yield");
                    safeCfk.overrideSink(null);
                }
                // if a single key transaction is queued behind this one then once we have executed this transaction it
                // will be ready to execute, so we may claim it (see ExecuteBacklogTxn)
                else if (!yield && next != null && next.is(SingleKey) && mayClaim(next, Ballot.ZERO) && !shouldYieldTo(next.plainTxnId())) claimAhead = next.plainTxnId();
            }
        }

        if (yield)
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: yielding backlog execution to nearby coordinator", txnId);
            return;
        }

        execute(safeStore.commandStore(), txnId, claimAhead);
    }

    /*
     * Designated execution (see CoordinatorBacklogExecution#DESIGNATED)
     */

    /**
     * @return the replica designated to execute {@code txnId} (with home key {@code homeKey}) on behalf of its original
     * coordinator, from the perspective of {@code node}: a deterministic choice amongst the replicas of the home key's
     * shard (only those nearby {@code node} if {@link ProtocolModifiers#coordinatorBacklogExecutionPerDc}), so that every
     * node computes the same choice (given the same topology), and the choice is spread across replicas by key.
     * Choosing by key (rather than e.g. by transaction) means the same replica executes consecutive transactions with
     * the same home key.
     * TODO (desired): consider replica health; consider other choices (see EXECUTE_BACKLOG_PLAN)
     */
    public static @Nullable Node.Id designatedExecutor(Node node, RoutingKey homeKey)
    {
        Topology topology = node.topology().current();
        int shardIndex = topology.indexForKey(homeKey);
        if (shardIndex < 0)
            return null;

        accord.topology.Shard shard = topology.get(shardIndex);
        int hash = mix(homeKey.hashCode());
        if (!coordinatorBacklogExecutionPerDc())
            return shard.nodes.get(Math.floorMod(hash, shard.nodes.size()));

        int nearby = 0;
        for (int i = 0, size = shard.nodes.size() ; i < size ; ++i)
        {
            if (node.isNearby(shard.nodes.get(i)))
                ++nearby;
        }
        if (nearby == 0)
        {
            if (logger.isTraceEnabled())
                logger.trace("no replica of {} near {}", homeKey, node.id());
            return null;
        }

        int pick = Math.floorMod(hash, nearby);
        for (int i = 0, size = shard.nodes.size() ; i < size ; ++i)
        {
            Node.Id id = shard.nodes.get(i);
            if (node.isNearby(id) && pick-- == 0)
                return id;
        }
        throw new IllegalStateException();
    }

    private static int mix(int hash)
    {
        hash *= 0x9E3779B9;
        return hash ^ (hash >>> 16);
    }

    /**
     * @return true iff the original coordinator of {@code txnId} should defer to a designated replica in another DC,
     * as {@code txnId} is contended (it had to wait to execute); see {@link CoordinatorBacklogExecution#DESIGNATED}
     */
    public boolean shouldDeferToDesignated(TxnId txnId, RoutingKey homeKey)
    {
        if (!coordinatorBacklogExecutionDesignated() || coordinatorBacklogExecutionPerDc() || !mayExecute(txnId))
            return false;
        Node.Id designated = designatedExecutor(node, homeKey);
        return designated != null && !node.isNearby(designated);
    }

    /**
     * A command has become ready to execute in {@code safeStore}; if we are its designated executor and its coordinator
     * is not nearby, execute it on its coordinator's behalf. If it was ready as soon as it became Stable it is probably
     * uncontended, so its coordinator will probably execute it promptly: wait a grace period first, so that we may
     * receive its Apply (see {@link ProtocolModifiers#coordinatorBacklogExecutionGrace}).
     */
    public void onReadyToExecute(SafeCommandStore safeStore, Command command, boolean onStable)
    {
        TxnId txnId = command.txnId();
        if (!mayExecute(txnId) || txnId.node.equals(node.id()) || node.isNearby(txnId.node) || !command.promised().equals(Ballot.ZERO))
            return;

        Route<?> route = command.route();
        if (route == null)
            return;

        Node.Id designated = designatedExecutor(node, route.homeKey());
        if (!node.id().equals(designated))
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: ready on {} but designated is {}", txnId, node.id(), designated);
            return;
        }

        if (!Route.isFullRoute(route))
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: designated to execute on {} but do not know full route: {}", txnId, node.id(), route);
            return;
        }

        CommandStore commandStore = safeStore.commandStore();
        // if we persist locally, we are responsible for applying the transaction in our DC, so do not wait for its coordinator
        long grace = onStable && !coordinatorBacklogExecutionPersistLocally() ? coordinatorBacklogExecutionGrace(MICROSECONDS) : 0;
        if (logger.isTraceEnabled())
            logger.trace("{}: designated to execute on {}{}", txnId, node.id(), grace > 0 ? " after grace" : "");
        if (grace <= 0) execute(commandStore, txnId, null);
        else node.scheduler().once(() -> execute(commandStore, txnId, null), grace, MICROSECONDS);
    }

    /**
     * @return true iff the original coordinator of {@code txnId} is nearby (e.g. in the same DC) and plausibly still
     * coordinating it, in which case it will receive its replies about as promptly as we would, so we should leave
     * the transaction to it rather than duplicate its work
     * TODO (desired): consider the coordinator's liveness, not only the transaction's age
     */
    private boolean shouldYieldTo(TxnId txnId)
    {
        Node.Id coordinator = txnId.node;
        if (!coordinatorBacklogExecutionYieldToNearby() || coordinator.equals(node.id()) || !node.isNearby(coordinator) || isObligated(txnId))
            return false;

        long age = Math.max(0, node.now() - txnId.hlc());
        // attempt 1: has the coordinator taken longer than we would wait before first trying to recover it
        // (attempt 0 would typically consider any coordinator slow, e.g. with Cassandra's "5s*attempts")
        return !node.agent().isSlowCoordinator(age, node.time().nowUnits(), txnId, 1);
    }

    /**
     * @return true iff the original coordinator of {@code txnId} is expected to continue executing the backlog on
     * {@code key} behind it, i.e. it will execute the transaction locally and install its own backlog sink
     */
    private boolean mayContinueBacklog(TxnId txnId, RoutingKey key)
    {
        if (!coordinatorBacklogExecution(Ballot.ZERO) || !txnId.is(SingleKey))
            return false;

        Topology current = node.topology().current();
        Topology.NodeInfo info = current.nodeInfo(txnId.node);
        return info != null && info.ranges.contains(key);
    }

    private void execute(CommandStore commandStore, TxnId txnId, @Nullable TxnId claimAhead)
    {
        if (tryMarkActive(txnId)) executeActive(commandStore, txnId, claimAhead);
        else if (logger.isTraceEnabled())
        {
            StringBuilder kinds = new StringBuilder();
            node.coordinations().forEach(txnId, c -> kinds.append(c.kind()).append(' '));
            logger.trace("{}: not executing backlog as already active or coordinating on {} ({})", txnId, node.id(), kinds);
        }
    }

    private void executeActive(CommandStore commandStore, TxnId txnId, @Nullable TxnId claimAhead)
    {
        commandStore.execute(ExecutionContext.unsequenced(txnId, "Load for ExecuteBacklog"), safeStore -> {
            tryExecuteOrRelease(safeStore, txnId, claimAhead);
        }, (success, fail) -> {
            if (fail != null)
            {
                release(txnId);
                node.agent().onException(fail);
            }
        });
    }

    private void tryExecuteOrRelease(SafeCommandStore safeStore, TxnId txnId, @Nullable TxnId claimAhead)
    {
        if (!tryExecute(safeStore, txnId, claimAhead))
        {
            designatedRetries.remove(txnId);
            release(txnId);
        }
    }

    private void release(TxnId txnId)
    {
        active.remove(txnId);
        obligations.remove(txnId);
    }

    /**
     * @return true if we have started execution (or, if we are obligated, are waiting to)
     */
    private boolean tryExecute(SafeCommandStore safeStore, TxnId txnId, @Nullable TxnId claimAhead)
    {
        SafeCommand safeCommand = safeStore.ifInitialised(txnId);
        if (safeCommand == null)
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: not executing backlog as not initialised in {}", txnId, safeStore.commandStore());
            return false;
        }

        Command command = safeCommand.current();
        boolean obligated = isObligated(txnId);
        if (obligated && command.saveStatus().compareTo(ReadyToExecute) < 0)
        {
            // we have claimed this transaction, so must wait for it to be ready rather than abandon it
            safeStore.register(txnId, new ReadyListener(txnId));
            return true;
        }

        if (!isCandidate(command))
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: not executing backlog as not a candidate ({})", txnId, command.saveStatus());
            return false;
        }

        Ballot promised = command.promised();
        Node.Id recovering = promised.equals(Ballot.ZERO) ? null : promised.node;
        if (node.id().equals(recovering))
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: not executing backlog as we are recovering it", txnId);
            return false; // we are already recovering the transaction
        }

        FullRoute<?> route = (FullRoute<?>) command.route();
        Timestamp executeAt = command.executeAt();
        Ballot ballot = command.acceptedOrCommitted();
        Txn txn = null;
        Deps deps = null;
        if (coversRoute(command))
        {
            // this store holds the full definition and deps (e.g. a single key transaction), so we need not collect them
            txn = command.partialTxn().reconstitute(route);
            deps = command.partialDeps().reconstitute(route);
        }
        Runnable beforePersist = null;
        if (claimAhead != null)
        {
            CommandStore commandStore = safeStore.commandStore();
            beforePersist = () -> claimAhead(claimAhead, commandStore);
        }
        start(safeStore.commandStore(), txnId, route, txn, executeAt, deps, ballot, promised, obligated, beforePersist);
        return true;
    }

    private static boolean isCandidate(Command command)
    {
        Participants<?> stillExecutes = command.participants().stillExecutes();
        return command.saveStatus() == ReadyToExecute
               && stillExecutes != null && !stillExecutes.isEmpty()
               && Route.isFullRoute(command.route());
    }

    private static boolean coversRoute(Command command)
    {
        FullRoute<?> route = (FullRoute<?>) command.route();
        PartialTxn txn = command.partialTxn();
        PartialDeps deps = command.partialDeps();
        return txn != null && deps != null && txn.covers(route) && deps.covers(route);
    }

    private boolean tryMarkActive(TxnId txnId)
    {
        // a MaybeRecover only checks the transaction's status (e.g. because it is blocking others, perhaps because its
        // coordinator has deferred to us), so should not stop us executing it; but a recovery should
        if (node.coordinations().anyMatch(txnId, COORDINATES_STATE_MACHINE_EXCEPT_MAYBE_RECOVER))
            return false;

        long now = node.elapsed(NANOSECONDS);
        if (active.size() >= PRUNE_ACTIVE_AT_SIZE)
            active.values().removeIf(at -> now - at > ACTIVE_EXPIRES_AFTER_NANOS);

        Long prev = active.putIfAbsent(txnId, now);
        return prev == null || (now - prev > ACTIVE_EXPIRES_AFTER_NANOS && active.replace(txnId, prev, now));
    }

    private void start(CommandStore commandStore, TxnId txnId, FullRoute<?> route, @Nullable Txn txn, Timestamp executeAt, @Nullable Deps deps, Ballot ballot, Ballot promised, boolean obligated, @Nullable Runnable beforePersist)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: executing backlog{}{}", txnId, txn == null ? ", collecting definition" : "", obligated ? ", as claimed" : "");

        boolean mayClaim = !obligated && mayClaim(txnId, promised);
        Runnable claimBeforePersist = beforePersist;
        Runnable installAndClaimBeforePersist = () -> {
            if (!coordinatorBacklogExecutionDesignated())
                installOnEveryKey(txnId, route, executeAt);
            if (claimBeforePersist != null)
                claimBeforePersist.run();
        };
        node.withEpochAtLeast(executeAt.epoch(), null, (ignore, fail) -> {
            release(txnId);
            node.agent().onException(fail);
        }, () -> {
            ExecuteBacklogTxn.execute(node, route, ballot, txnId, txn, executeAt, deps, obligated, mayClaim, installAndClaimBeforePersist, (result, fail) -> {
                release(txnId);
                if (logger.isTraceEnabled())
                    logger.trace("{}: executed backlog with outcome {} on {}", txnId, fail == null ? "success" : fail.getClass().getSimpleName(), node.id());

                if (fail == null) designatedRetries.remove(txnId);
                if (fail == null) node.reportLocalExecution(txnId, route, promised, null, null, result);
                else if (!isExpectedFailure(fail)) node.agent().onException(fail);
                else if (coordinatorBacklogExecutionDesignated() && !(fail instanceof Redundant)) retryDesignated(commandStore, txnId);
            });
        });
    }

    // the number of times we have retried executing a transaction we are designated to execute
    private final ConcurrentHashMap<TxnId, Integer> designatedRetries = new ConcurrentHashMap<>();
    // TODO (desired): configurable
    private static final long DESIGNATED_RETRY_MICROS = MILLISECONDS.toMicros(5), MAX_DESIGNATED_RETRY_MICROS = MILLISECONDS.toMicros(500);
    private static final int MAX_DESIGNATED_RETRIES = 20;

    /**
     * We are designated to execute {@code txnId}, but our attempt failed without anybody else having executed it (e.g.
     * because some replica we read from had not yet received its Stable message); since its original coordinator may
     * have deferred to us we should not give up, so we retry with backoff. We stop once it is no longer a candidate
     * (e.g. it has been executed) or after a number of attempts (after which the original coordinator will resume).
     */
    private void retryDesignated(CommandStore commandStore, TxnId txnId)
    {
        int attempt = designatedRetries.merge(txnId, 1, Integer::sum);
        if (attempt > MAX_DESIGNATED_RETRIES)
        {
            designatedRetries.remove(txnId);
            return;
        }
        if (designatedRetries.size() >= PRUNE_ACTIVE_AT_SIZE)
            designatedRetries.keySet().removeIf(id -> !active.containsKey(id) && !id.equals(txnId));

        long delay = Math.min(MAX_DESIGNATED_RETRY_MICROS, DESIGNATED_RETRY_MICROS << Math.min(attempt - 1, 16));
        if (logger.isTraceEnabled())
            logger.trace("{}: retrying designated execution on {} (attempt {})", txnId, node.id(), attempt);
        node.scheduler().once(() -> execute(commandStore, txnId, null), delay, MICROSECONDS);
    }

    /**
     * @return true iff {@code fail} is an expected outcome of an attempt to execute a backlog, that need not be reported
     */
    private static boolean isExpectedFailure(Throwable fail)
    {
        if (fail instanceof NotReady || fail instanceof Redundant)
            return true;
        // if we wait for replicas to become ready, we may time out (e.g. if they are waiting on a slow predecessor)
        // or run out of replicas to try (e.g. if none has yet recorded the transaction as Stable)
        return (coordinatorBacklogExecutionWait() || coordinatorBacklogExecutionDesignated() || coordinatorBacklogExecutionClaims()) && (fail instanceof Timeout || fail instanceof Exhausted);
    }

    /*
     * Persisting locally (see ProtocolModifiers#coordinatorBacklogExecutionPersistLocally)
     */

    /**
     * @return true iff we (as a designated replica) should persist {@code txnId} only to replicas in our DC, reporting to
     * its original coordinator which have applied it, as the original coordinator is responsible for its durability
     * (see {@link #coordinatorOwnsDurability})
     */
    public static boolean persistsLocally(Node node, TxnId txnId, Ballot ballot, Topologies topologies, Timestamp executeAt, Route<?> route)
    {
        return coordinatorBacklogExecutionPersistLocally() && ballot.equals(Ballot.ZERO) && mayExecute(txnId) && !node.isNearby(txnId.node)
               && nearbyReplicatesAll(node, txnId.node, executesIn(topologies, executeAt), route);
    }

    /**
     * @return true iff we (as original coordinator of {@code txnId}) are responsible for its durability, even if a
     * designated replica in another DC executes it first (see {@link ProtocolModifiers#coordinatorBacklogExecutionPersistLocally()}),
     * so must not stop executing the transaction on {@link accord.messages.RemoteSuccess}. This requires that our DC
     * replicates every shard of the transaction, so that we may read everything we need nearby (if we read from a
     * replica in another DC, it may already have applied the transaction, and so be unable to serve our read).
     * Designated replicas make the same decision, so if we are not responsible for durability they persist globally.
     */
    public static boolean coordinatorOwnsDurability(Node node, TxnId txnId, Ballot ballot, Topologies topologies, Timestamp executeAt, Route<?> route)
    {
        return coordinatorBacklogExecutionPersistLocally() && ballot.equals(Ballot.ZERO) && mayExecute(txnId) && txnId.node.equals(node.id())
               && nearbyReplicatesAll(node, node.id(), executesIn(topologies, executeAt), route);
    }

    /**
     * @return true iff (as original coordinator) we may defer sending Apply to {@code to}, i.e. it is in another DC
     * that replicates every shard of the transaction, so that a designated replica there will execute the transaction
     * and persist it locally
     */
    public static boolean mayDeferApplyTo(Node node, Node.Id to, Topologies topologies, Timestamp executeAt, Route<?> route)
    {
        return !node.isNearby(to) && nearbyReplicatesAll(node, to, executesIn(topologies, executeAt), route);
    }

    private static Topology executesIn(Topologies topologies, Timestamp executeAt)
    {
        long epoch = executeAt.epoch();
        return topologies.containsEpoch(epoch) ? topologies.getEpoch(epoch) : topologies.current();
    }

    /**
     * @return true iff the nodes near {@code of} (e.g. in its DC) replicate every shard of {@code topology} that intersects {@code route}
     */
    public static boolean nearbyReplicatesAll(Node node, Node.Id of, Topology topology, Route<?> route)
    {
        for (int i = 0, size = topology.size() ; i < size ; ++i)
        {
            accord.topology.Shard shard = topology.get(i);
            if (!route.intersects(shard.range))
                continue;

            boolean found = false;
            for (int j = 0, jsize = shard.nodes.size() ; !found && j < jsize ; ++j)
                found = node.areNearby(of, shard.nodes.get(j));
            if (!found)
                return false;
        }
        return true;
    }

    /**
     * Notified (as original coordinator) of replicas that a designated replica reports have applied a transaction
     */
    public interface AppliedListener
    {
        void onReportedApplied(SortedArrayList<Node.Id> appliedAt);
    }

    /**
     * Reports received before any listener registered (i.e. before we began to persist the transaction), together
     * with the acknowledgements we owe their senders once we know we are responsible for the transaction's durability
     */
    private static class BufferedApplied
    {
        final SortedArrayList<Node.Id> appliedAt;
        final List<PendingAck> acks;
        final long expiresAt;

        BufferedApplied(SortedArrayList<Node.Id> appliedAt, List<PendingAck> acks, long expiresAt)
        {
            this.appliedAt = appliedAt;
            this.acks = acks;
            this.expiresAt = expiresAt;
        }
    }

    private static class PendingAck
    {
        final Node.Id from;
        final ReplyContext replyContext;

        PendingAck(Node.Id from, ReplyContext replyContext)
        {
            this.from = from;
            this.replyContext = replyContext;
        }
    }

    /**
     * We have finished deferring Apply for a transaction (so need no further reports), but should still acknowledge
     * any reports we receive, as we remain responsible for its durability
     */
    private static class Resolved
    {
        final long expiresAt;

        Resolved(long expiresAt)
        {
            this.expiresAt = expiresAt;
        }
    }

    // for each transaction we coordinate (and are responsible for the durability of), either: the AppliedListener
    // awaiting reports that replicas have applied it; the BufferedApplied reports we have received before any listener
    // registered; or Resolved once we no longer need reports
    private final ConcurrentHashMap<TxnId, Object> appliedReports = new ConcurrentHashMap<>();

    private static boolean isExpired(Object entry, long now)
    {
        if (entry instanceof BufferedApplied) return ((BufferedApplied) entry).expiresAt <= now;
        if (entry instanceof Resolved) return ((Resolved) entry).expiresAt <= now;
        return false;
    }

    private void ack(Node.Id to, ReplyContext replyContext)
    {
        node.reply(to, replyContext, SimpleReply.Ok, null, null);
    }

    /**
     * A designated replica {@code from} reports (to us, as original coordinator) that {@code appliedAt} have applied
     * {@code txnId}. We acknowledge the report once we know we are responsible for the transaction's durability, i.e.
     * once we have begun to persist it; if we never do (e.g. because our own execution failed), the designated replica
     * will persist the transaction more widely itself.
     */
    public void onApplied(TxnId txnId, Node.Id from, SortedArrayList<Node.Id> appliedAt, ReplyContext replyContext)
    {
        long now = node.elapsed(MICROSECONDS);
        if (appliedReports.size() >= PRUNE_ACTIVE_AT_SIZE)
            appliedReports.values().removeIf(v -> isExpired(v, now));

        Object[] found = new Object[1];
        appliedReports.compute(txnId, (id, cur) -> {
            if (cur instanceof AppliedListener || (cur instanceof Resolved && !isExpired(cur, now)))
            {
                found[0] = cur;
                return cur;
            }
            List<PendingAck> acks = new ArrayList<>(2);
            SortedArrayList<Node.Id> prev = null;
            if (cur instanceof BufferedApplied && !isExpired(cur, now))
            {
                prev = ((BufferedApplied) cur).appliedAt;
                acks.addAll(((BufferedApplied) cur).acks);
            }
            acks.add(new PendingAck(from, replyContext));
            return new BufferedApplied(SortedArrayList.union(prev, appliedAt), acks, claimExpiresAt(txnId));
        });

        if (logger.isTraceEnabled())
            logger.trace("{}: {} reports applied at {}{}", txnId, from, appliedAt, found[0] == null ? " (buffered)" : found[0] instanceof Resolved ? " (resolved)" : "");
        if (found[0] instanceof AppliedListener)
            ((AppliedListener) found[0]).onReportedApplied(appliedAt);
        if (found[0] != null)
            ack(from, replyContext);
    }

    /**
     * Register to receive reports that replicas have applied {@code txnId}, including any we have already received,
     * as we are responsible for its durability and are deferring Apply to remote replicas
     */
    public void registerAppliedListener(TxnId txnId, AppliedListener listener)
    {
        Object prev = appliedReports.put(txnId, listener);
        if (prev instanceof BufferedApplied && !isExpired(prev, node.elapsed(MICROSECONDS)))
        {
            BufferedApplied buffered = (BufferedApplied) prev;
            listener.onReportedApplied(buffered.appliedAt);
            for (PendingAck ack : buffered.acks)
                ack(ack.from, ack.replyContext);
        }
    }

    /**
     * We are responsible for the durability of {@code txnId}, and have contacted (or will contact) every replica not
     * reported to have applied it; acknowledge any further reports
     * @param listener the listener we registered, if any
     */
    public void resolveApplied(TxnId txnId, @Nullable AppliedListener listener)
    {
        Resolved resolved = new Resolved(claimExpiresAt(txnId));
        Object prev;
        if (listener != null)
        {
            if (!appliedReports.replace(txnId, listener, resolved))
                return;
            prev = listener;
        }
        else
        {
            prev = appliedReports.put(txnId, resolved);
        }

        if (prev instanceof BufferedApplied)
        {
            for (PendingAck ack : ((BufferedApplied) prev).acks)
                ack(ack.from, ack.replyContext);
        }
    }

    /*
     * BacklogClaim handling
     */

    /**
     * An execution paused because a backlog executor claimed it (see {@code ExecuteTxn.onClaimed})
     */
    public interface Paused
    {
        TxnId txnId();
        void onRemoteSuccess(Result result);
        void onLocalExecution(Timestamp applyAt, Writes writes, Result result);
        void resume();
    }

    private class PausedEntry implements Timeouts.Timeout
    {
        final Paused paused;
        Timeouts.RegisteredTimeout timeout;

        PausedEntry(Paused paused)
        {
            this.paused = paused;
        }

        @Override
        public void timeout()
        {
            if (ExecuteTxnBacklog.this.paused.remove(paused.txnId(), this))
            {
                if (logger.isTraceEnabled())
                    logger.trace("{}: resuming execution paused for BacklogClaim on {}", paused.txnId(), node.id());
                paused.resume();
            }
        }

        @Override
        public int stripe()
        {
            return paused.txnId().hashCode();
        }
    }

    // claims we have received but have not (yet) found an execution to pause, mapped to their expiry (per Node#elapsed)
    private final ConcurrentHashMap<TxnId, Long> receivedClaims = new ConcurrentHashMap<>();
    // executions we have paused in response to a claim
    private final ConcurrentHashMap<TxnId, PausedEntry> paused = new ConcurrentHashMap<>();
    // transactions we have claimed, and so are obligated to execute, mapped to their expiry (per Node#elapsed)
    private final ConcurrentHashMap<TxnId, Long> obligations = new ConcurrentHashMap<>();
    // results reported to us (as original coordinator) before we began (or paused) execution
    private final ConcurrentHashMap<TxnId, RemoteSuccessEntry> remoteSuccesses = new ConcurrentHashMap<>();

    private static class RemoteSuccessEntry
    {
        final Result result;
        final long expiresAt;

        RemoteSuccessEntry(Result result, long expiresAt)
        {
            this.result = result;
            this.expiresAt = expiresAt;
        }
    }

    private long claimExpiresAt(TxnId txnId)
    {
        return node.agent().selfExpiresAt(txnId, READ_REQ, MICROSECONDS);
    }

    private static void prune(ConcurrentHashMap<TxnId, Long> expiries, long now)
    {
        if (expiries.size() >= PRUNE_ACTIVE_AT_SIZE)
            expiries.values().removeIf(expiresAt -> expiresAt <= now);
    }

    /**
     * We (as a replica) have executed {@code txnId} ourselves (see {@link ProtocolModifiers#executeAtReplica}),
     * so if we have paused our execution (as original coordinator) for a BacklogClaim, we may complete it
     */
    public void onLocalExecution(TxnId txnId, Timestamp applyAt, Writes writes, Result result)
    {
        if (paused.isEmpty())
            return;

        PausedEntry entry = paused.remove(txnId);
        if (entry == null)
            return;

        if (entry.timeout != null)
            entry.timeout.cancel();
        if (logger.isTraceEnabled())
            logger.trace("{}: completing execution paused for BacklogClaim on local execution", txnId);
        entry.paused.onLocalExecution(applyAt, writes, result);
    }

    /**
     * @return the result of a RemoteSuccess received for {@code txnId} before we began (or paused) execution, if any
     */
    public @Nullable Result takeRemoteSuccess(TxnId txnId)
    {
        if (remoteSuccesses.isEmpty())
            return null;
        RemoteSuccessEntry entry = remoteSuccesses.remove(txnId);
        return entry == null || entry.expiresAt <= node.elapsed(MICROSECONDS) ? null : entry.result;
    }

    private boolean isObligated(TxnId txnId)
    {
        Long expiresAt = obligations.get(txnId);
        return expiresAt != null && expiresAt > node.elapsed(MICROSECONDS);
    }

    /**
     * Tell the original coordinator of {@code txnId} that we intend to execute it; we are then obligated to do so,
     * i.e. we will wait for it to become ready rather than abandon our attempt.
     */
    public void claim(TxnId txnId, String reason)
    {
        prune(obligations, node.elapsed(MICROSECONDS));
        obligations.put(txnId, claimExpiresAt(txnId));
        if (logger.isTraceEnabled())
            logger.trace("{}: claiming for backlog execution ({}) on {}", txnId, reason, node.id());
        node.send(txnId.node, new BacklogClaim(txnId), null);
    }

    /**
     * Claim a transaction that will be ready to execute once the transaction we are executing has applied,
     * and prepare to execute it once it is ready
     */
    private void claimAhead(TxnId txnId, CommandStore commandStore)
    {
        // only claim the transaction if we will try to execute it (i.e. nobody else on this node is)
        if (!tryMarkActive(txnId))
            return;

        claim(txnId, "ahead");
        executeActive(commandStore, txnId, null);
    }

    /**
     * A backlog executor has told us (as original coordinator) that it intends to execute {@code txnId}
     */
    public void onClaimed(TxnId txnId, Node.Id from)
    {
        boolean[] found = new boolean[1];
        node.coordinations().forEach(txnId, coordination -> {
            if (coordination.kind() == Execute && coordination instanceof ExecuteTxn)
            {
                found[0] = true;
                ((ExecuteTxn) coordination).onClaimed();
            }
        });

        if (logger.isTraceEnabled())
            logger.trace("{}: received BacklogClaim from {} {}", txnId, from, found[0] ? "while executing" : "while not executing");
        if (!found[0])
        {
            prune(receivedClaims, node.elapsed(MICROSECONDS));
            receivedClaims.put(txnId, claimExpiresAt(txnId));
        }
    }

    /**
     * @return true iff we have received a (live) claim for {@code txnId} that has not yet been acted upon
     */
    public boolean consumeClaim(TxnId txnId)
    {
        Long expiresAt = receivedClaims.remove(txnId);
        return expiresAt != null && expiresAt > node.elapsed(MICROSECONDS);
    }

    public void pause(TxnId txnId, Paused pause)
    {
        Result result = takeRemoteSuccess(txnId);
        if (result != null)
        {
            pause.onRemoteSuccess(result);
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("{}: pausing execution for BacklogClaim on {}", txnId, node.id());
        PausedEntry entry = new PausedEntry(pause);
        PausedEntry prev = paused.put(txnId, entry);
        if (prev != null && prev.timeout != null)
            prev.timeout.cancel();
        entry.timeout = node.timeouts().registerAt(entry, claimExpiresAt(txnId), MICROSECONDS);
    }

    /**
     * @param reported whether some coordination was waiting for this result
     */
    public void onRemoteSuccess(TxnId txnId, Result result, boolean reported)
    {
        PausedEntry entry = paused.remove(txnId);
        if (entry == null)
        {
            // when using claims, a backlog executor may complete a transaction before we (as original coordinator)
            // reach the execution phase, so retain the result in case we do so
            if (!reported && coordinatorBacklogExecutionClaims())
            {
                long now = node.elapsed(MICROSECONDS);
                if (remoteSuccesses.size() >= PRUNE_ACTIVE_AT_SIZE)
                    remoteSuccesses.values().removeIf(e -> e.expiresAt <= now);
                remoteSuccesses.put(txnId, new RemoteSuccessEntry(result, claimExpiresAt(txnId)));
            }
            return;
        }

        if (entry.timeout != null)
            entry.timeout.cancel();
        if (logger.isTraceEnabled())
            logger.trace("{}: completing execution paused for BacklogClaim", txnId);
        entry.paused.onRemoteSuccess(result);
    }

    /**
     * Wait for a transaction we have claimed to become ReadyToExecute before trying to execute it
     */
    private class ReadyListener implements LocalListeners.ComplexListener
    {
        final TxnId txnId;

        ReadyListener(TxnId txnId)
        {
            this.txnId = txnId;
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            SaveStatus saveStatus = safeCommand.current().saveStatus();
            if (saveStatus.compareTo(ReadyToExecute) < 0)
                return true;

            if (saveStatus != ReadyToExecute)
            {
                // executed by somebody else
                release(txnId);
            }
            else
            {
                // re-evaluate in a fresh task rather than starting a coordination while the command is mid-update
                safeStore.commandStore().execute(ExecutionContext.unsequenced(txnId, "ExecuteBacklog once Ready"), safeStore0 -> {
                    tryExecuteOrRelease(safeStore0, txnId, null);
                }, (success, fail) -> {
                    if (fail != null)
                    {
                        release(txnId);
                        node.agent().onException(fail);
                    }
                });
            }
            return false;
        }

        @Override
        public String toString()
        {
            return "ExecuteBacklog.ReadyListener{" + txnId + '}';
        }
    }
}
