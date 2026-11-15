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

import accord.api.LocalListeners;
import accord.api.ProtocolModifiers;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Timeouts;
import accord.coordinate.ExecuteBacklogTxn;
import accord.coordinate.ExecuteTxn;
import accord.coordinate.NotReady;
import accord.coordinate.Redundant;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.messages.BacklogClaim;
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
import accord.topology.Topology;

import static accord.api.ProtocolModifiers.coordinatorBacklogExecution;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionClaims;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionMultiKey;
import static accord.api.ProtocolModifiers.coordinatorBacklogExecutionYieldToNearby;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static accord.coordinate.Coordination.CoordinationKind.COORDINATES_STATE_MACHINE;
import static accord.local.CommandSummaries.SummaryStatus.STABLE;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static accord.messages.MessageType.StandardMessage.READ_REQ;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
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
 * waiting to do so in any case). So, the transaction must be {@link SaveStatus#ReadyToExecute} in the store that
 * notifies us, and {@link ExecuteBacklogTxn} abandons the attempt if any replica it reads from is not ready.
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

        boolean yield = shouldYieldTo(txnId);
        TxnId claimAhead = null;
        SafeCommandsForKey safeCfk = safeStore.ifLoadedAndInitialised(key);
        if (safeCfk != null && safeCfk.overrideSink() == this)
        {
            CommandsForKey cfk = safeCfk.current();
            TxnInfo info = cfk.get(txnId);
            if (info != null)
            {
                TxnInfo next = nextBacklog(cfk, info.executeAt);
                // if nothing eligible is queued behind this transaction then the backlog is exhausted once it is executed,
                // so stop listening on this key; a later coordinator may decide to install us again
                if (next == null || (yield && mayContinueBacklog(txnId, key)))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("{}: releasing backlog sink on {} ({})", txnId, key, next == null ? describeNoBacklog(cfk, info.executeAt) : "yield");
                    safeCfk.overrideSink(null);
                }
                // if a single key transaction is queued behind this one then once we have executed this transaction it
                // will be ready to execute, so we may claim it (see ExecuteBacklogTxn)
                else if (!yield && next.is(SingleKey) && mayClaim(next, Ballot.ZERO) && !shouldYieldTo(next.plainTxnId())) claimAhead = next.plainTxnId();
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

    @Override
    public void waitingOn(SafeCommandStore safeStore, TxnInfo txn, RoutingKey key, SaveStatus waitingOnStatus, BlockedUntil blockedUntil, boolean notifyCfk)
    {
        DefaultNotifySink.INSTANCE.waitingOn(safeStore, txn, key, waitingOnStatus, blockedUntil, notifyCfk);
    }

    private void execute(CommandStore commandStore, TxnId txnId, @Nullable TxnId claimAhead)
    {
        if (tryMarkActive(txnId))
            executeActive(commandStore, txnId, claimAhead);
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
            release(txnId);
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
            return false;

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
            return false; // we are already recovering the transaction

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
        start(txnId, route, txn, executeAt, deps, ballot, promised, obligated, beforePersist);
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
        if (node.coordinations().anyMatch(txnId, COORDINATES_STATE_MACHINE))
            return false;

        long now = node.elapsed(NANOSECONDS);
        if (active.size() >= PRUNE_ACTIVE_AT_SIZE)
            active.values().removeIf(at -> now - at > ACTIVE_EXPIRES_AFTER_NANOS);

        Long prev = active.putIfAbsent(txnId, now);
        return prev == null || (now - prev > ACTIVE_EXPIRES_AFTER_NANOS && active.replace(txnId, prev, now));
    }

    private void start(TxnId txnId, FullRoute<?> route, @Nullable Txn txn, Timestamp executeAt, @Nullable Deps deps, Ballot ballot, Ballot promised, boolean obligated, @Nullable Runnable beforePersist)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: executing backlog{}{}", txnId, txn == null ? ", collecting definition" : "", obligated ? ", as claimed" : "");

        boolean mayClaim = !obligated && mayClaim(txnId, promised);
        node.withEpochAtLeast(executeAt.epoch(), null, (ignore, fail) -> {
            release(txnId);
            node.agent().onException(fail);
        }, () -> {
            ExecuteBacklogTxn.execute(node, route, ballot, txnId, txn, executeAt, deps, obligated, mayClaim, beforePersist, (result, fail) -> {
                release(txnId);
                if (logger.isTraceEnabled())
                    logger.trace("{}: executed backlog with outcome {} on {}", txnId, fail == null ? "success" : fail.getClass().getSimpleName(), node.id());

                if (fail == null) node.reportLocalExecution(txnId, route, promised, null, null, result);
                else if (!(fail instanceof NotReady) && !(fail instanceof Redundant)) node.agent().onException(fail);
            });
        });
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
