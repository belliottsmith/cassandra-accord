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

package accord.impl.progresslog;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Ballot;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;
import accord.utils.LogGroupTimers;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.NotBlocked;
import static accord.impl.progresslog.CoordinatePhase.Decided;
import static accord.impl.progresslog.CoordinatePhase.ReadyToExecute;
import static accord.impl.progresslog.CoordinatePhase.Undecided;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.impl.progresslog.TxnStateKind.Waiting;
import static accord.local.Command.NotDefined.uninitialised;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.btree.UpdateFunction.noOpReplace;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

// TODO (expected): for transactions that span multiple progress logs (notably: sync points) we need to coordinate *fetching* to avoid redundant work
// TODO (expected): report transactions not making progress
// TODO (desired): evict to disk
public class DefaultProgressLog implements ProgressLog, Consumer<SafeCommandStore>
{
    static abstract class PendingTask
    {
        final DefaultProgressLog owner;

        PendingTask(DefaultProgressLog owner)
        {
            this.owner = owner;
        }

        void postRun(SafeCommandStore safeStore)
        {
            owner.acceptIfNonEmptyRunBuffer(safeStore);
        }
    }

    public static class Config
    {
        public int concurrency = 8;
        public Duration maxActiveRunTime = Duration.ofMinutes(1);
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultProgressLog.class);

    final Node node;
    final CommandStore commandStore;

    private Object[] stateMap = BTree.empty();
    private Object[] progressTokenMap = BTree.empty();

    final LogGroupTimers<TxnState> timers = new LogGroupTimers<>(MICROSECONDS);

    /**
     * A collection of active callbacks (waiting remote replies) or submitted run invocations
     * (perhaps waiting load from disk, or for the CommandStore thread to be available).
     *
     * These callbacks are required to have hashCode() == txnId.hashCode() and equals(txnId) == true,
     * so that we can manage overriding callbacks on the relevant TxnState.
     */
    // TODO (desired): replace this with a set that can lookup the matching item
    private final Object2ObjectHashMap<TxnId, PendingTask> pendingWaiting = new Object2ObjectHashMap<>();
    private final Object2ObjectHashMap<TxnId, PendingTask> pendingHome = new Object2ObjectHashMap<>();

    private final Long2ObjectHashMap<Object> active = new Long2ObjectHashMap<>();
    private final Map<TxnId, StackTraceElement[]> debugDeleted = Invariants.debug() ? new Object2ObjectHashMap<>() : null;

    private static final Object[] EMPTY_RUN_BUFFER = new Object[0];
    private static final RunInvoker[] EMPTY_AWAITING_EPOCH_BUFFER = new RunInvoker[0];

    // The tasks whose timers have elapsed and are going to be run
    // The queue is drained here first before processing tasks so that tasks can modify the queue
    private Object[] runBuffer = EMPTY_RUN_BUFFER;
    private int runBufferIndex, runBufferCount;
    private RunInvoker[] awaitingEpochBuffer = EMPTY_AWAITING_EPOCH_BUFFER;
    private int awaitingEpochBufferCount;
    private boolean isAwaitingEpoch;
    private boolean processing;

    private volatile boolean stopped;
    private Config config = new Config();

    private long nextCallbackId;

    protected DefaultProgressLog(Node node, CommandStore commandStore)
    {
        this.node = node;
        this.commandStore = commandStore;
    }

    Node node()
    {
        return node;
    }

    void update(long deadline, TxnState timer)
    {
        timers.update(deadline, timer);
    }

    void add(long deadline, TxnState timer)
    {
        timers.add(deadline, timer);
    }

    @Nullable TxnState get(TxnId txnId)
    {
        Invariants.require(txnId.isVisible());
        return BTree.<TxnId, TxnState>find(stateMap, (id, state) -> id.compareTo(state.txnId), txnId);
    }

    TxnState ensure(TxnId txnId)
    {
        Invariants.require(txnId.isVisible());
        TxnState result = BTree.<TxnId, TxnState>find(stateMap, (id, state) -> id.compareTo(state.txnId), txnId);
        if (result == null)
        {
            Invariants.require(debugDeleted == null || !debugDeleted.containsKey(txnId));
            stateMap = BTree.update(stateMap, BTree.singleton(result = new TxnState(txnId)), TxnState::compareTo);
        }
        return result;
    }

    private TxnState insert(TxnId txnId)
    {
        Invariants.require(debugDeleted == null || !debugDeleted.containsKey(txnId));
        TxnState result = new TxnState(txnId);
        stateMap = BTree.update(stateMap, BTree.singleton(result), TxnState::compareTo);
        return result;
    }

    ProgressToken savedProgressToken(TxnId txnId)
    {
        ProgressToken saved = BTree.<TxnId, SavedProgressToken>find(progressTokenMap, (id, e) -> id.compareTo(e.txnId), txnId);
        if (saved == null)
            return ProgressToken.NONE;

        progressTokenMap = BTreeRemoval.<TxnId, SavedProgressToken>remove(progressTokenMap, (id, e) -> id.compareTo(e.txnId), txnId);
        return saved;
    }

    void saveProgressToken(TxnId txnId, ProgressToken token)
    {
        SavedProgressToken save = new SavedProgressToken(txnId, token);
        // we could save memory by setting ballot to ZERO when we have the same ballot in command (and can restore isAccepted)
        // but this isn't likely to offer dramatic savings very often
        progressTokenMap = BTree.update(progressTokenMap, BTree.singleton(save), SavedProgressToken::compare, noOpReplace());
    }

    void clearProgressToken(TxnId txnId)
    {
        progressTokenMap = BTreeRemoval.<TxnId, SavedProgressToken>remove(progressTokenMap, (id, e) -> id.compareTo(e.txnId), txnId);
    }

    @Override
    public void update(SafeCommandStore safeStore, TxnId txnId, Command before, Command after, boolean force)
    {
        if (!txnId.isVisible())
            return;

        TxnState state = null;
        Route<?> beforeRoute = before.route();
        Route<?> afterRoute = after.route();
        if (force || (afterRoute != null && beforeRoute == null) || (after.durability().isDurableOrInvalidated() && !before.durability().isDurableOrInvalidated()))
            state = updateOrInitialiseHomeState(safeStore, after, get(txnId));

        SaveStatus beforeSaveStatus = before.saveStatus();
        SaveStatus afterSaveStatus = after.saveStatus();
        if (beforeSaveStatus == afterSaveStatus && !force)
            return;

        if (state == null)
            state = get(txnId);

        if (state == null)
            return;

        state.waiting().record(this, afterSaveStatus);
        if (state.isHomeInitialised())
            updateHomeState(safeStore, state, before, after);
    }

    @Override
    public void decided(SafeCommandStore safeStore, TxnId txnId)
    {
        TxnState state = get(txnId);
        if (state != null && state.isHomeInitialised())
            state.home().atLeast(safeStore, this, Decided, NoneExpected);
    }

    private void updateHomeState(SafeCommandStore safeStore, TxnState state, Command before, Command after)
    {
        switch (after.saveStatus())
        {
            case Stable:
                if (!after.acceptedOrCommitted().equals(Ballot.ZERO) || (before != null && before.saveStatus() == SaveStatus.Committed))
                    state.home().atLeast(safeStore, this, Decided, NoneExpected);
            default:
                // fall-through to default handler, which simply postpones any scheduled coordination attempt if we witness another coordination attempt in the meantime
                if (state.homeProgress() == Queued && (before == null ? after.promised().compareTo(Ballot.ZERO) > 0 : (after.promised().compareTo(before.promised()) > 0) || after.acceptedOrCommitted().compareTo(before.acceptedOrCommitted()) > 0))
                {
                    clearPendingAndActive(Home, state.txnId);
                    state.home().set(safeStore, this, state.phase(), Queued);
                }
                break;
            case ReadyToExecute:
            case PreApplied:
                state.home().atLeast(safeStore, this, ReadyToExecute, Queued);
                break;
        }
    }

    private TxnState updateOrInitialiseHomeState(SafeCommandStore safeStore, Command after, @Nullable TxnState state)
    {
        Route<?> route = after.route();
        if (after.durability().isDurableOrInvalidated())
        {
            // command is durable, so we don't need to coordinate it - whether we're the home shard or not
            if (state != null)
                state.setHomeDone(this);

            // ... and we should be able to fetch its outcome if we need it
            state = maybeFetch(safeStore, after, state);

            if (state != null && state.maybeRemove(this))
                state = null;

            return state;
        }

        if (route == null)
            return state; // we don't know if we're the home shard

        TxnId txnId = after.txnId();
        RoutingKey homeKey = route.homeKey();
        Ranges coordinateRanges = safeStore.coordinateRanges(txnId);
        boolean isHome = coordinateRanges.contains(homeKey);
        state = get(txnId);
        if (isHome)
        {
            if (state == null)
                state = insert(txnId);

            Invariants.require(!after.durability().isDurableOrInvalidated());
            if (!state.isHomeInitialised())
                state.set(safeStore, this, Undecided, Queued); // initialise
        }
        else if (state != null)
        {
            // not home shard
            state.setHomeDone(this);
        }

        return state;
    }

    private TxnState maybeFetch(SafeCommandStore safeStore, Command after, TxnState state)
    {
        if (after.hasBeen(PreApplied))
            return state;

        // TODO (required): (LHF) this does not appear to correctly compute for an ExclusiveSyncPoint, but equally
        //   executes() may not be set for only PreCommitted transactions.
        Ranges executeRanges = after.hasBeen(PreCommitted) ? safeStore.ranges().allAt(after.executeAt())
                                                           : safeStore.ranges().allSince(after.txnId().epoch());
        if (executeRanges.intersects(after.participants().owns()))
        {
            // this command should be ready to apply locally, so fetch it
            if (state == null)
                state = insert(after.txnId());
            state.waiting().setBlockedUntil(safeStore, this, CanApply);
        }
        return state;
    }

    @Override
    public void clear(TxnId txnId)
    {
        if (!txnId.isVisible())
            return;

        TxnState state = get(txnId);
        if (state != null)
            clear(state);
    }

    public List<TxnId> activeBefore(TxnId before)
    {
        List<TxnId> result = new ArrayList<>();
        for (TxnState state : BTree.<TxnState>iterable(stateMap))
        {
            if (state.txnId.compareTo(before) >= 0)
                break;

            result.add(state.txnId);
        }
        return result;
    }

    @Override
    public void clearBefore(SafeCommandStore safeStore, TxnId clearWaitingBefore, TxnId clearAllBefore)
    {
        Invariants.require(clearAllBefore.compareTo(clearWaitingBefore) <= 0);

        int index = 0;
        while (index < BTree.size(stateMap))
        {
            TxnState state = BTree.findByIndex(stateMap, index);
            if (state.txnId.compareTo(clearWaitingBefore) >= 0)
                return;

            boolean notify = state.waiting().blockedUntil()  != NotBlocked
                          && state.waiting().homeSatisfies() == NotBlocked;

            if (notify)
            {
                // the command might be invalidated, which should be established on load, so simply load the command
                TxnId txnId = state.txnId;
                safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, "Clear Progress"), safeStore0 -> {
                    safeStore0.unsafeGet(txnId);
                }, node.agent());
            }

            if (state.txnId.compareTo(clearAllBefore) < 0)
            {
                clear(state);
            }
            else
            {
                state.setWaitingDone(this);
                if (!state.maybeRemove(this))
                    ++index;
            }
        }
    }

    @Override
    public void start()
    {
        commandStore.maybeExecuteImmediately(() -> {
            stopped = false;
            accept(null);
        });
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    @Override
    public void clear()
    {
        timers.clear();

        stateMap = BTree.empty();
        progressTokenMap = BTree.empty();

        pendingWaiting.clear();
        pendingHome.clear();
        if (debugDeleted != null)
            debugDeleted.clear();

        runBuffer = EMPTY_RUN_BUFFER;
        runBufferIndex = runBufferCount = 0;
    }

    private void clear(TxnState state)
    {
        state.setHomeDone(this);
        state.setWaitingDone(this);
        Invariants.require(!state.isScheduled());
        remove(state.txnId);
    }

    void remove(TxnId txnId)
    {
        stateMap = BTreeRemoval.<TxnId, TxnState>remove(stateMap, (id, s) -> id.compareTo(s.txnId), txnId);
        if (debugDeleted != null)
            debugDeleted.put(txnId, Thread.currentThread().getStackTrace());
    }

    @Override
    public void remoteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, SaveStatus remoteStatus, int callbackId, Node.Id from)
    {
        TxnState state = get(safeCommand.txnId());
        if (state != null)
            state.asynchronousAwaitCallback(this, safeStore, remoteStatus, from, callbackId);
    }

    @Override
    public void waiting(BlockedUntil blockedUntil, SafeCommandStore safeStore, SafeCommand blockedBy, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants, StoreParticipants blockedOnStoreParticipants)
    {
        if (!blockedBy.txnId().isVisible())
            return;


        Command command = blockedBy.current();
        if (command == null) command = uninitialised(blockedBy.txnId());
        SaveStatus saveStatus = command.saveStatus();
        Invariants.require(saveStatus.compareTo(blockedUntil.unblockedFrom) < 0);

        StoreParticipants blockedOnStoreParticipants2 = null;
        if (blockedOnParticipants != null || blockedOnRoute != null)
        {
            Participants<?> owns, touches;
            Ranges coordinateRanges = safeStore.ranges().allAt(blockedBy.txnId().epoch());
            if (blockedOnRoute == null)
            {
                touches = blockedOnParticipants;
                owns = blockedOnParticipants.slice(coordinateRanges, Minimal);
            }
            else
            {
                owns = blockedOnRoute.slice(coordinateRanges, Minimal);
                touches = owns;
            }
            blockedOnStoreParticipants2 = StoreParticipants.create(blockedOnRoute, owns, null, null, touches, touches);
        }

        // first save the route/participant info into the Command if it isn't already there

        Command update = blockedBy.current();
        StoreParticipants participants = update.participants();
        StoreParticipants updatedParticipants = participants;
        if (blockedOnStoreParticipants != null) updatedParticipants = updatedParticipants.supplementOrMerge(saveStatus, blockedOnStoreParticipants);
        if (blockedOnStoreParticipants2 != null) updatedParticipants = updatedParticipants.supplementOrMerge(saveStatus, blockedOnStoreParticipants2);
        if (participants != updatedParticipants)
            update = command.updateParticipants(updatedParticipants);

        if (update != command)
            command = blockedBy.incidentalUpdate(update);

        // TODO (required): tighten up ExclusiveSyncPoint range bounds
        Invariants.require((command.txnId().is(ExclusiveSyncPoint) ? safeStore.ranges().all()
                                                                   : safeStore.ranges().allSince(command.txnId().epoch())
                              ).intersects(command.participants().hasTouched()));

        // TODO (desired):  consider triggering a preemption of existing coordinator (if any) in some circumstances;
        //                  today, an LWT can pre-empt more efficiently (i.e. instantly) a failed operation whereas Accord will
        //                  wait for some progress interval before taking over; there is probably some middle ground where we trigger
        //                  faster preemption once we're blocked on a transaction, while still offering some amount of time to complete.
        // TODO (desired, efficiency): forward to local progress shard for processing (if known)
        // TODO (desired, efficiency): if we are co-located with the home shard, don't need to do anything unless we're in a
        //                             later topology that wasn't covered by its coordination
        TxnState state = ensure(blockedBy.txnId());
        state.waiting().setBlockedUntil(safeStore, this, blockedUntil);
        // in case progress log hasn't been updated (e.g. bug on replay), force an update to the command's state since we're about to wait on it
        if (!state.isHomeInitialised() && command.route() != null)
            updateOrInitialiseHomeState(safeStore, command, state);
    }

    @Override
    public void invalidIfUncommitted(TxnId txnId)
    {
        TxnState state = get(txnId);
        if (state != null)
            state.setInvalidIfUncommitted();
    }

    void acceptIfNonEmptyRunBuffer(SafeCommandStore safeStore)
    {
        if (runBufferIndex < runBufferCount)
            accept(safeStore);
    }

    @Override
    public void accept(@Nullable SafeCommandStore safeStore)
    {
        if (stopped || processing)
            return;

        long nowMicros = node.elapsed(TimeUnit.MICROSECONDS);
        processing = true;
        try
        {
            processAwaitingEpoch();
            try (BufferList<TxnState> preRunBuffer = new BufferList<>())
            {
                // drain to a buffer to avoid reentrancy in timers
                timers.advance(nowMicros, preRunBuffer, BufferList::add);
                updateRunBuffer(nowMicros, preRunBuffer);
            }
            processRunBuffer(safeStore);

            if (awaitingEpochBufferCount > 0)
                rerunWithPendingEpoch();
        }
        catch (Throwable t)
        {
            node.agent().onUncaughtException(t);
        }
        finally
        {
            processing = false;
        }
    }

    private void addToRunBuffer(RunInvoker readyToRun)
    {
        if (runBufferCount == runBuffer.length)
        {
            int newCount = runBufferCount - runBufferIndex;
            Object[] newBuffer = cachedAny().get(Math.max(8, newCount * 2));
            replaceRunBuffer(newBuffer);
        }
        runBuffer[runBufferCount++] = readyToRun;
    }

    private void replaceRunBuffer(Object[] newBuffer)
    {
        Object[] prevBuffer = runBuffer;
        int prevCount = runBufferCount;
        int newCount = prevCount - runBufferIndex;
        System.arraycopy(prevBuffer, runBufferIndex, newBuffer, 0, newCount);
        runBuffer = newBuffer;
        runBufferIndex = 0;
        runBufferCount = newCount;
        if (prevBuffer.length >= ArrayBuffers.MIN_BUFFER_SIZE)
            cachedAny().forceDiscard(prevBuffer, prevCount);
    }

    private void maybeShrinkRunBuffer()
    {
        if (runBuffer.length <= (runBufferCount - runBufferIndex)/2)
        {
            int newCount = runBufferCount - runBufferIndex;
            Object[] newBuffer = new Object[newCount + (newCount/2)];
            replaceRunBuffer(newBuffer);
        }
    }

    private void rerunWithPendingEpoch()
    {
        if (isAwaitingEpoch)
            return;
        
        long minEpoch = Long.MAX_VALUE;
        for (int i = 0 ; i < awaitingEpochBufferCount ; ++i)
            minEpoch = Math.min(awaitingEpochBuffer[i].run.txnId.epoch(), minEpoch);
        Invariants.requireArgument(minEpoch != Long.MAX_VALUE);
        isAwaitingEpoch = true;
        node.withEpochAtLeast(minEpoch, commandStore, (success, fail) -> commandStore.execute((PreLoadContext.Empty) () -> "Run ProgressLog", ss -> {
            isAwaitingEpoch = false;
            accept(ss);
        }, node.agent()));
    }

    private void updateRunBuffer(long nowMicros, List<TxnState> preRunBuffer)
    {
        long hasEpoch = node.topology().epoch();

        for (TxnState run : preRunBuffer)
        {
            Invariants.require(!run.isScheduled());
            TxnStateKind runKind = run.wasScheduledTimer();
            validatePreRunState(run, runKind);
            long pendingTimerDeadline = run.pendingTimerDeadline();
            boolean invokeBoth = false;
            if (pendingTimerDeadline > 0)
            {
                run.clearPendingTimerDelay();
                if (pendingTimerDeadline <= nowMicros)
                {
                    validatePreRunState(run, runKind.other());
                    invokeBoth = true;
                }
                else
                {
                    run.setScheduledTimer(runKind.other());
                    timers.add(pendingTimerDeadline, run);
                }
            }

            long epoch = run.txnId.epoch();
            if (epoch <= hasEpoch)
            {
                addToRunBuffer(invoker(run, runKind));
                if (invokeBoth) addToRunBuffer(invoker(run, runKind.other()));
            }
            else
            {
                int count = invokeBoth ? 2 : 1;
                if (awaitingEpochBufferCount + count >= awaitingEpochBuffer.length)
                    awaitingEpochBuffer = Arrays.copyOf(awaitingEpochBuffer, Math.max(8, awaitingEpochBufferCount * 2));
                awaitingEpochBuffer[awaitingEpochBufferCount++] = invoker(run, runKind);
                if (invokeBoth) awaitingEpochBuffer[awaitingEpochBufferCount++] = invoker(run, runKind.other());
            }
        }
    }

    private void processAwaitingEpoch()
    {
        if (awaitingEpochBufferCount == 0)
            return;

        long hasEpoch = node.topology().epoch();
        int retainCount = 0;
        for (int i = 0 ; i < awaitingEpochBufferCount ; ++i)
        {
            RunInvoker awaiting = awaitingEpochBuffer[i];
            if (awaiting.run.txnId.epoch() <= hasEpoch) addToRunBuffer(awaiting);
            else awaitingEpochBuffer[retainCount++] = awaiting;
        }
        awaitingEpochBufferCount = retainCount;
        if (retainCount == 0) awaitingEpochBuffer = EMPTY_AWAITING_EPOCH_BUFFER;
        else if (retainCount < awaitingEpochBuffer.length / 2) awaitingEpochBuffer = Arrays.copyOf(awaitingEpochBuffer, retainCount);
    }

    private void processRunBuffer(SafeCommandStore safeStore)
    {
        while (runBufferIndex < runBufferCount)
        {
            if (active.size() >= config.concurrency)
            {
                maybeShrinkRunBuffer();
                return;
            }

            RunInvoker run = (RunInvoker) runBuffer[runBufferIndex];
            if (safeStore == null || !safeStore.canExecuteWith(run))
            {
                maybeShrinkRunBuffer();
                commandStore.execute(run, this);
                return;
            }

            ++runBufferIndex;
            try { run.accept(safeStore); }
            catch (Throwable t) { node.agent().onUncaughtException(t); }
        }

        if (runBuffer.length > ArrayBuffers.MIN_BUFFER_SIZE)
            cachedAny().forceDiscard(runBuffer, runBufferCount);
        runBufferIndex = runBufferCount = 0;
        runBuffer = EMPTY_RUN_BUFFER;
    }

    private void validatePreRunState(TxnState run, TxnStateKind kind)
    {
        Progress progress = kind == Waiting ? run.waiting().waitingProgress() : run.home().homeProgress();
        Invariants.require(progress != NoneExpected);
        if (progress == Querying)
        {
            // TODO (expected): add debug information about the active task
            logger.warn("Interrupting query for {} ({}) as fallback timeout exceeded", run.txnId, kind);
            clearPendingAndActive(kind, run.txnId);
        }
    }

    RunInvoker invoker(TxnState run, TxnStateKind runKind)
    {
        RunInvoker invoker = new RunInvoker(this, run, runKind);
        registerPending(runKind, run.txnId, invoker);
        return invoker;
    }

    static final class RunInvoker extends PendingTask implements PreLoadContext, Consumer<SafeCommandStore>
    {
        final DefaultProgressLog owner;
        final TxnState run;
        final TxnStateKind runKind;

        RunInvoker(DefaultProgressLog owner, TxnState run, TxnStateKind runKind)
        {
            super(owner);
            this.owner = owner;
            this.run = run;
            this.runKind = runKind;
        }

        private boolean complete()
        {
            return owner.complete(runKind, run.txnId, this);
        }

        private void acceptInternal(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            owner.run(runKind, run, safeStore, safeCommand);
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            try
            {
                // we load safeCommand first so that if it clears the progress log we abandon the callback
                SafeCommand safeCommand = safeStore.ifInitialised(run.txnId);
                if (complete() && safeCommand != null)
                    acceptInternal(safeStore, safeCommand);
            }
            finally
            {
                postRun(safeStore);
            }
        }

        @Override
        public TxnId primaryTxnId()
        {
            return run.txnId;
        }

        @Override
        public String reason()
        {
            return "Invoke " + runKind + " Progress Log";
        }
    }

    protected void run(TxnStateKind runKind, TxnState run, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        // check this after fetching SafeCommand, as doing so can erase the command (and invalidate our state)
        if (run.isDone(runKind))
            return;

        Invariants.require(get(run.txnId) == run, "Transaction state for %s does not match expected one %s", run.txnId, run);
        Invariants.require(run.scheduledTimer() != runKind, "We are actively executing %s, but we are also scheduled to run this same TxnState later. This should not happen.", runKind);
        Invariants.require(run.pendingTimer() != runKind, "We are actively executing %s, but we also have a pending scheduled task to run this same TxnState later. This should not happen.", runKind);

        validatePreRunState(run, runKind);
        if (runKind == Home) run.runHome(DefaultProgressLog.this, safeStore, safeCommand);
        else run.runWaiting(DefaultProgressLog.this, safeStore, safeCommand);
    }

    long nextCallbackId()
    {
        return ++nextCallbackId;
    }

    Object2ObjectHashMap<TxnId, PendingTask> pending(TxnStateKind kind)
    {
        return kind == Waiting ? pendingWaiting : pendingHome;
    }

    void registerPending(TxnStateKind kind, TxnId txnId, PendingTask register)
    {
        Object2ObjectHashMap<TxnId, PendingTask> collection = pending(kind);
        PendingTask existing = collection.putIfAbsent(txnId, register);
        Invariants.require(existing == null);
    }

    boolean hasPending(TxnStateKind kind, TxnId txnId)
    {
        return pending(kind).containsKey(txnId);
    }

    void start(CallbackInvoker<?, ?> invoker, Object debug)
    {
        // task is an arbitrary object to help debug, but must be non-null
        // TODO (expected): make active debuggable via virtual table or other mechanism
        if (debug == null)
            debug = invoker;
        active.put(invoker.id, debug);
    }

    boolean complete(TxnStateKind kind, long id, TxnId txnId, PendingTask completing)
    {
        boolean stillActive = active.remove(id) != null;
        return complete(kind, txnId, completing) && stillActive;
    }

    boolean complete(TxnStateKind kind, TxnId txnId, PendingTask completing)
    {
        return pending(kind).remove(txnId, completing);
    }

    void clearPendingAndActive(TxnStateKind kind, TxnId txnId)
    {
        PendingTask pending = pending(kind).remove(txnId);
        if (pending instanceof CallbackInvoker<?,?>)
            active.remove(((CallbackInvoker<?, ?>) pending).id);
    }

    public void requeue(SafeCommandStore safeStore, TxnStateKind kind, TxnId txnId)
    {
        clearPendingAndActive(kind, txnId);
        TxnState state = get(txnId);
        if (state != null && (kind == Home ? state.isHomeInitialised() : !state.isWaitingDone()))
            state.updateScheduling(safeStore, this, kind, null, Queued);
    }

    void unschedule(TxnState state)
    {
        timers.remove(state);
    }

    public boolean isHomeStateActive(TxnId txnId)
    {
        TxnState state = get(txnId);
        return state != null && !state.isHomeDone();
    }

    public boolean isWaitingStateActive(TxnId txnId)
    {
        TxnState state = get(txnId);
        return state != null && !state.isWaitingDone();
    }

    public void maybeNotify()
    {
        if (stopped)
            return;

        if (commandStore.inStore())
        {
            accept(null);
        }
        else
        {
            long now = node.elapsed(MICROSECONDS);
            if (timers.shouldWake(now))
                commandStore.execute((PreLoadContext.Empty) () -> "Run ProgressLog", this, node.agent());
        }
    }

    public Config config()
    {
        return config;
    }

    public void setConfig(SafeCommandStore safeStore, Config config)
    {
        Invariants.require(commandStore.inStore());
        this.config = config;
    }

    public void unsafeSetConfig(Config config)
    {
        this.config = config;
    }

    public int size()
    {
        return BTree.size(stateMap);
    }

    public int pendingHome()
    {
        return pendingHome.size();
    }

    public int pendingWaiting()
    {
        return pendingWaiting.size();
    }

    public ImmutableView immutableView()
    {
        return new ImmutableView(commandStore.id(), stateMap);
    }

    public static class ImmutableView
    {
        private final int commandStoreId;
        private final Object[] snapshot;

        ImmutableView(int commandStoreId, Object[] snapshot)
        {
            this.commandStoreId = commandStoreId;
            this.snapshot = snapshot;
        }

        public boolean isEmpty()
        {
            return BTree.isEmpty(snapshot);
        }

        private Iterator<TxnState> iterator = null;
        private TxnState current = null;

        public boolean advance()
        {
            if (iterator == null)
                iterator = BTree.iterator(snapshot);

            if (!iterator.hasNext())
            {
                current = null;
                return false;
            }

            current = iterator.next();
            return true;
        }

        public int commandStoreId()
        {
            return commandStoreId;
        }

        @Nonnull
        public TxnId txnId()
        {
            return current.txnId;
        }

        @Nullable
        public Long timerScheduledAt(TxnStateKind kind)
        {
            // TODO (expected): global constant declaring granularity of these timer deadlines
            if (current.scheduledTimer() == kind)
                return current.scheduledTimerDeadline();
            if (current.pendingTimer() == kind)
                return current.pendingTimerDeadline();
            return null;
        }

        public boolean contactEveryone()
        {
            return current.contactEveryone();
        }

        public boolean isWaitingUninitialised()
        {
            return current.isUninitialised();
        }

        @Nonnull
        public BlockedUntil waitingIsBlockedUntil()
        {
            return current.blockedUntil();
        }

        @Nonnull
        public BlockedUntil waitingHomeSatisfies()
        {
            return current.homeSatisfies();
        }

        @Nonnull
        public Progress waitingProgress()
        {
            return current.waitingProgress();
        }

        @Nonnull
        public long waitingPackedKeyTrackerBits()
        {
            return current.waitingKeyTrackerBits();
        }

        @Nonnull
        public int waitingRetryCounter()
        {
            return current.waitingRunCounter();
        }

        @Nonnull
        public CoordinatePhase homePhase()
        {
            return current.phase();
        }

        @Nonnull
        public Progress homeProgress()
        {
            return current.homeProgress();
        }

        public int homeRetryCounter()
        {
            return current.homeRunCounter();
        }
    }
}
