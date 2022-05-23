package accord.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Invalidate;
import accord.impl.SimpleProgressLog.HomeState.LocalStatus;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.primitives.KeyRange;
import accord.primitives.PartialRoute;
import accord.primitives.RoutingKeys;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topology;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.coordinate.CheckOnCommitted.checkOnCommitted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.impl.SimpleProgressLog.CoordinateApplyAndCheck.applyAndCheck;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.Durable;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.RemotelyDurable;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.Committed;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.NonHomeState.Safe;
import static accord.impl.SimpleProgressLog.NonHomeState.StillUnsafe;
import static accord.impl.SimpleProgressLog.NonHomeState.Unsafe;
import static accord.impl.SimpleProgressLog.Progress.Done;
import static accord.impl.SimpleProgressLog.Progress.Expected;
import static accord.impl.SimpleProgressLog.Progress.Investigating;
import static accord.impl.SimpleProgressLog.Progress.NoProgress;
import static accord.impl.SimpleProgressLog.Progress.NoneExpected;
import static accord.impl.SimpleProgressLog.Progress.advance;
import static accord.local.Status.Executed;
import static accord.utils.Functions.notNullOrMerge;

public class SimpleProgressLog implements Runnable, ProgressLog.Factory
{
    enum Progress
    {
        NoneExpected, Expected, NoProgress, Investigating, Done; // TODO: done is a redundant state (vs NoneExpected)

        static Progress advance(Progress current)
        {
            switch (current)
            {
                default: throw new IllegalStateException();
                case NoneExpected:
                case Investigating:
                case Done:
                    return current;
                case Expected:
                case NoProgress:
                    return NoProgress;
            }
        }
    }

    static class HomeState
    {
        enum LocalStatus
        {
            NotWitnessed, Uncommitted, Committed, ReadyToExecute, Done;
            boolean isAtMost(LocalStatus equalOrLessThan)
            {
                return compareTo(equalOrLessThan) <= 0;
            }
            boolean isAtLeast(LocalStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
        }

        LocalStatus status = LocalStatus.NotWitnessed;
        Progress progress = NoneExpected;
        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        Object debugInvestigating;

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Command command)
        {
            if (newStatus == Committed && command.isGloballyPersistent() && !command.executes())
            {
                status = LocalStatus.Done;
                progress = Done;
            }
            else if (newStatus.compareTo(status) > 0)
            {
                status = newStatus;
                progress = newProgress;
            }
            updateMax(command);
        }

        void updateMax(Command command)
        {
            if (maxStatus == null || maxStatus.compareTo(command.status()) < 0)
                maxStatus = command.status();
            if (maxPromised == null || maxPromised.compareTo(command.promised()) < 0)
                maxPromised = command.promised();
            maxPromiseHasBeenAccepted |= command.accepted().equals(maxPromised);
        }

        void updateMax(CheckStatusOk ok)
        {
            // TODO: perhaps set localProgress back to Waiting if Investigating and we update anything?
            if (ok.status.compareTo(maxStatus) > 0) maxStatus = ok.status;
            if (ok.promised.compareTo(maxPromised) > 0)
            {
                maxPromised = ok.promised;
                maxPromiseHasBeenAccepted = ok.accepted.equals(ok.promised);
            }
            else if (ok.promised.equals(maxPromised))
            {
                maxPromiseHasBeenAccepted |= ok.accepted.equals(ok.promised);
            }
        }

        void executed()
        {
            switch (status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    status = LocalStatus.Done;
                    progress = NoneExpected;
                case Done:
            }
        }

        void update(Node node, TxnId txnId, Command command)
        {
            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            switch (status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Committed:
                case Done:
                    throw new IllegalStateException(); // NoProgressExpected

                case Uncommitted:
                case ReadyToExecute:
                {
                    if (status.isAtLeast(Committed) && command.isGloballyPersistent())
                    {
                        // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                        // records to the home shard, so we only know the executeAt shards will have witnessed this
                        // if the home shard is at an earlier phase, it must run recovery
                        long epoch = command.executeAt().epoch;
                        node.withEpoch(epoch, () -> {
                            // TODO (now): slice the route to only those owned locally
                            debugInvestigating = checkOnCommitted(node, txnId, command.route(), command.executeAt().epoch, (success, fail) -> {
                                // should have found enough information to apply the result, but in case we did not reset progress
                                if (progress == Investigating)
                                    progress = Expected;
                            });
                        });
                    }
                    else
                    {
                        RoutingKey homeKey = command.homeKey();
                        long homeEpoch = (status.isAtMost(Uncommitted) ? txnId : command.executeAt()).epoch;

                        node.withEpoch(homeEpoch, () -> {

                            Future<CheckStatusOk> recover = node.maybeRecover(txnId, homeKey, homeEpoch,
                                                                              maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                            recover.addCallback((success, fail) -> {
                                if (status.isAtMost(ReadyToExecute) && progress == Investigating)
                                {
                                    progress = Expected;
                                    if (fail != null)
                                        return;

                                    // TODO (now): ensure hasExecutedOnAllShards is propagated by CheckShards
                                    if (success == null || success.hasExecutedOnAllShards)
                                        global.executedOnAllShards(node, command, null);
                                    else
                                        updateMax(success);
                                }
                            });

                            debugInvestigating = recover;
                        });
                    }
                }
            }
        }

        @Override
        public String toString()
        {
            return "{" + status + ',' + progress + '}';
        }
    }

    static class GlobalState
    {
        enum GlobalStatus
        {
            NotExecuted, RemotelyDurable, LocallyDurable, Durable, Done;
            boolean isAtLeast(GlobalStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
        }

        static class PendingDurable
        {
            final Set<Id> fullyPersistedOn;

            PendingDurable(Set<Id> fullyPersistedOn)
            {
                this.fullyPersistedOn = fullyPersistedOn;
            }
        }

        boolean isAppliedLocally;
        GlobalStatus status = NotExecuted;
        Progress progress = NoneExpected;
        Map<KeyRange, Set<Id>> notPersisted;
        PendingDurable pendingDurable;
        Timestamp executeAt;
        PartialRoute partialRoute;

        Object debugInvestigating;

        private boolean refresh(@Nullable Node node, @Nullable Command command, @Nullable Id fullyPersistedOn,
                                @Nullable Set<Id> fullyPersistedOns, @Nullable Map<KeyRange, Set<Id>> notPersistedOns)
        {
            if (status == NotExecuted)
                return false;

            if (pendingDurable != null)
            {
                if (node == null || command == null || command.is(Status.NotWitnessed))
                    return false;

                if (fullyPersistedOns == null) fullyPersistedOns = pendingDurable.fullyPersistedOn;
                else fullyPersistedOns.addAll(pendingDurable.fullyPersistedOn);

                status = Durable;
                progress = Expected;
            }

            if (notPersisted == null)
            {
                assert node != null && command != null;
                if (!node.topology().hasEpoch(command.executeAt().epoch))
                    return false;

                Topology topology = node.topology()
                                        .globalForEpoch(command.executeAt().epoch)
                                        .forKeys(command.routeForKeysOwnedAtExecution());

                notPersisted = Maps.newHashMapWithExpectedSize(topology.size());
                for (Shard shard : topology)
                {
                    Set<Id> remaining = new HashSet<>(shard.nodes);
                    if (isAppliedLocally)
                        remaining.remove(node.id());
                    notPersisted.put(shard.range, remaining);
                }
            }
            if (notPersisted != null)
            {
                if (fullyPersistedOn != null)
                {
                    notPersisted.values().forEach(remaining -> remaining.remove(fullyPersistedOn));
                }
                if (fullyPersistedOns != null)
                {
                    Set<Id> remove = fullyPersistedOns;
                    notPersisted.values().forEach(remaining -> remaining.removeAll(remove));
                }
                if (notPersistedOns != null)
                {
                    retainAllNotPersistedOn(notPersistedOns);
                }
                if (notPersisted.isEmpty())
                {
                    status = GlobalStatus.Done;
                    progress = Done;
                }
            }

            return true;
        }

        private void retainAllNotPersistedOn(Map<KeyRange, Set<Id>> notPersistedOn)
        {
            notPersistedOn.forEach((k, s) -> {
                Set<Id> remaining = notPersisted.get(k);
                if (remaining == null)
                    return;
                remaining.retainAll(s);
            });
        }

        void executedOnAllShards(Node node, Command command, Set<Id> persistedOn)
        {
            if (status == NotExecuted)
            {
                status = RemotelyDurable;
                progress = NoneExpected;
                pendingDurable = new PendingDurable(persistedOn);
            }
            else if (status != GlobalStatus.Done)
            {
                status = Durable;
                progress = Expected;
                refresh(node, command, null, persistedOn, null);
            }
        }

        void update(Node node, TxnId txnId, Command command)
        {
            if (!refresh(node, command, null, null, null))
                return;

            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            debugInvestigating = applyAndCheck(node, txnId, this).addCallback((success, fail) -> {
                if (progress != Done)
                    progress = Expected;
            });
        }

        @Override
        public String toString()
        {
            return "{" + status + ',' + progress + '}';
        }
    }

    static class BlockingState
    {
        Status blockedOn = Status.NotWitnessed;
        Progress progress = NoneExpected;
        RoutingKeys someKeys;
        Command blocking;

        Object debugInvestigating;

        void recordBlocking(Command blocking, RoutingKeys someKeys)
        {
            this.blocking = blocking;
            switch (blocking.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case AcceptedInvalidate:
                    this.someKeys = someKeys;
                case PreAccepted:
                case Accepted:
                    blockedOn = Status.Committed;
                    progress = Expected;
                    break;
                case Committed:
                case ReadyToExecute:
                    blockedOn = Executed;
                    progress = Expected;
                    break;
                case Executed:
                case Applied:
                case Invalidated:
                    throw new IllegalStateException("Should not be recorded as blocked if result already recorded locally");
            }
        }

        void recordCommit()
        {
            if (blockedOn == Status.Committed)
                progress = NoneExpected;
        }

        void recordExecute()
        {
            progress = Done;
        }

        void update(Node node, TxnId txnId, Command command)
        {
            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            // check status with the only keys we know, if any, then:
            // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
            // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
            long onEpoch = (command.hasBeen(Status.Committed) ? command.executeAt() : txnId).epoch;
            node.withEpoch(onEpoch, () -> {
                RoutingKey someKey; RoutingKeys someKeys; {
                    RoutingKeys tmpKeys = notNullOrMerge(this.someKeys, command.someRoutingKeys(), RoutingKeys::union);
                    someKey = command.homeKey() == null ? tmpKeys.get(0) : command.homeKey();
                    someKeys = tmpKeys.with(someKey);
                }

                BiConsumer<CheckStatusOkFull, Throwable> callback = (success, fail) -> {
                    if (progress != Investigating)
                        return;

                    progress = Expected;
                    if (fail != null)
                        return;

                    switch (success.status)
                    {
                        default: throw new IllegalStateException();
                        case AcceptedInvalidate:
                            // we may or may not know the homeShard at this point; if the response doesn't know
                            // then assume we potentially need to pick up the invalidation
                            if (success.homeKey != null)
                                break;
                            // TODO (now): probably don't want to immediately go to Invalidate,
                            //             instead first give in-flight one a chance to complete
                        case NotWitnessed:
                            progress = Investigating;
                            // TODO: this should instead invalidate the transaction on this shard, which invalidates it for all shards,
                            //       but we need to first support invalidation
                            debugInvestigating = Invalidate.invalidate(node, txnId, someKeys, someKey)
                                                           .addCallback((success2, fail2) -> {
                                                               if (progress != Investigating) return;
                                                               if (fail2 != null) progress = Expected;
                                                               else switch (success2)
                                                               {
                                                                   default: throw new IllegalStateException();
                                                                   case Preempted:
                                                                       progress = Expected;
                                                                       break;
                                                                   case Executed:
                                                                   case Invalidated:
                                                                       progress = Done;
                                                               }
                                                           });
                            break;
                        case PreAccepted:
                        case Accepted:
                            // either it's the home shard and it's managing progress,
                            // or we now know the home shard and will contact it next time
                            break;
                        case Committed:
                        case ReadyToExecute:
                            Preconditions.checkState(command.hasBeen(Status.Committed) || !command.commandStore.ranges().intersects(txnId.epoch, someKeys));
                            if (blockedOn == Status.Committed)
                                progress = NoneExpected;
                            break;
                        case Executed:
                        case Applied:
                        case Invalidated:
                            progress = Done;
                    }
                };
                debugInvestigating = blockedOn == Executed ? checkOnCommitted(node, txnId, someKeys, onEpoch, callback)
                                                           : checkOnUncommitted(node, txnId, someKeys, onEpoch, callback);
            });
        }

        public String toString()
        {
            return progress.toString();
        }
    }

    enum NonHomeState
    {
        Unsafe, StillUnsafe, Investigating, Safe
    }

    static class State
    {
        final TxnId txnId;
        final Command command;

        HomeState homeState;
        GlobalState globalState;
        NonHomeState nonHomeState;
        BlockingState blockingState;

        State(TxnId txnId, Command command)
        {
            this.txnId = txnId;
            this.command = command;
        }

        void recordBlocking(Command blockedByCommand, RoutingKeys someKeys)
        {
            Preconditions.checkArgument(blockedByCommand.txnId().equals(txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.recordBlocking(blockedByCommand, someKeys);
        }

        void ensureAtLeast(NonHomeState ensureAtLeast)
        {
            if (nonHomeState == null || nonHomeState.compareTo(ensureAtLeast) < 0)
                nonHomeState = ensureAtLeast;
        }

        HomeState home()
        {
            if (homeState == null)
                homeState = new HomeState();
            return homeState;
        }

        GlobalState global()
        {
            if (globalState == null)
                globalState = new GlobalState();
            return globalState;
        }

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress)
        {
            home().ensureAtLeast(newStatus, newProgress, command);
        }

        void updateNonHome(Node node)
        {
            switch (nonHomeState)
            {
                default: throw new IllegalStateException();
                case Safe:
                case Investigating:
                    break;
                case Unsafe:
                    nonHomeState = StillUnsafe;
                    break;
                case StillUnsafe:
                    // make sure a quorum of the home shard is aware of the transaction, so we can rely on it to ensure progress
                    Future<Void> inform = inform(node, txnId, command.homeKey());
                    inform.addCallback((success, fail) -> {
                        if (nonHomeState == Safe)
                            return;

                        if (fail != null) nonHomeState = Unsafe;
                        else nonHomeState = Safe;
                    });
                    break;
            }
        }

        void update(Node node)
        {
            if (blockingState != null)
                blockingState.update(node, txnId, command);

            if (homeState != null)
                homeState.update(node, txnId, command);

            if (globalState != null)
                globalState.update(node, txnId, command);

            if (nonHomeState != null)
                updateNonHome(node);
        }

        @Override
        public String toString()
        {
            return homeState != null ? homeState.toString()
                                     : nonHomeState != null
                                       ? nonHomeState.toString()
                                       : blockingState.toString();
        }
    }

    final Node node;
    final List<Instance> instances = new CopyOnWriteArrayList<>();

    public SimpleProgressLog(Node node)
    {
        this.node = node;
        node.scheduler().recurring(this, 200L, TimeUnit.MILLISECONDS);
    }

    class Instance implements ProgressLog
    {
        final CommandStore commandStore;
        final Map<TxnId, State> stateMap = new HashMap<>();

        Instance(CommandStore commandStore)
        {
            this.commandStore = commandStore;
            instances.add(this);
        }

        State ensure(TxnId txnId)
        {
            return stateMap.computeIfAbsent(txnId, id -> new State(id, commandStore.command(id)));
        }

        State ensure(TxnId txnId, State state)
        {
            return state != null ? state : ensure(txnId);
        }

        @Override
        public void unwitnessed(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            if (isHomeShard)
                ensure(txnId).ensureAtLeast(Uncommitted, Expected);
        }

        @Override
        public void preaccept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            if (isProgressShard)
            {
                State state = ensure(txnId);
                if (isHomeShard) state.ensureAtLeast(Uncommitted, Expected);
                else state.ensureAtLeast(NonHomeState.Unsafe);
            }
        }

        State recordCommit(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.recordCommit();
            return state;
        }

        State recordExecute(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.blockingState != null)
                state.blockingState.recordExecute();
            return state;
        }

        private void ensureSafeOrAtLeast(TxnId txnId, boolean isProgressShard, boolean isHomeShard, LocalStatus newStatus, Progress newProgress)
        {
            State state = null;
            assert newStatus.isAtMost(ReadyToExecute);
            if (newStatus.isAtLeast(LocalStatus.Committed))
                state = recordCommit(txnId);

            if (isProgressShard)
            {
                state = ensure(txnId, state);

                if (isHomeShard) state.ensureAtLeast(newStatus, newProgress);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void accept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            ensureSafeOrAtLeast(txnId, isProgressShard, isHomeShard, Uncommitted, Expected);
        }

        @Override
        public void commit(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            ensureSafeOrAtLeast(txnId, isProgressShard, isHomeShard, LocalStatus.Committed, NoneExpected);
        }

        @Override
        public void readyToExecute(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            ensureSafeOrAtLeast(txnId, isProgressShard, isHomeShard, LocalStatus.ReadyToExecute, Expected);
        }

        @Override
        public void execute(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            State state = recordExecute(txnId);

            if (isProgressShard)
            {
                state = ensure(txnId, state);

                if (isHomeShard) state.home().executed();
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void invalidate(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            State state = recordExecute(txnId);

            if (isProgressShard)
            {
                state = ensure(txnId, state);

                if (isHomeShard) state.ensureAtLeast(LocalStatus.Done, Done);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void executedOnAllShards(TxnId txnId, Set<Id> persistedOn)
        {
            State state = ensure(txnId);
            state.global().executedOnAllShards(node, state.command, persistedOn);
        }

        @Override
        public void waiting(TxnId blockedBy, RoutingKeys someKeys)
        {
            Command blockedByCommand = commandStore.command(blockedBy);
            if (!blockedByCommand.hasBeen(Executed))
                ensure(blockedBy).recordBlocking(blockedByCommand, someKeys);
        }
    }

    @Override
    public void run()
    {
        for (Instance instance : instances)
        {
            // TODO: we want to be able to poll others about pending dependencies to check forward progress,
            //       as we don't know all dependencies locally (or perhaps any, at execution time) so we may
            //       begin expecting forward progress too early
            instance.stateMap.values().forEach(state -> {
                try
                {
                    state.update(node);
                }
                catch (Throwable t)
                {
                    node.agent().onUncaughtException(t);
                }
            });
        }
    }

    static class CoordinateApplyAndCheck extends AsyncFuture<Void> implements Callback<ApplyAndCheckOk>
    {
        final TxnId txnId;
        final GlobalState state;
        final Set<Id> waitingOnResponses;

        static Future<Void> applyAndCheck(Node node, TxnId txnId, GlobalState state, CommandStore commandStore)
        {

            node.mapReduceLocal(state.partialRoute, )
            CoordinateApplyAndCheck coordinate = new CoordinateApplyAndCheck(txnId, command, state);
            PartialRoute route = command.routeForKeysOwnedAtExecution();
            Topologies topologies = node.topology().preciseEpochs(route, command.executeAt().epoch);
            node.send(topologies.nodes(), id -> new ApplyAndCheck(id, topologies, command.txnId(), route,
                                                                        command.savedPartialDeps(), command.executeAt(),
                                                                        command.writes(), command.result(),
                                                                        state.notPersisted),
                      coordinate);
            return coordinate;
        }

        CoordinateApplyAndCheck(TxnId txnId, GlobalState state)
        {
            this.txnId = txnId;
            this.state = state;
            this.waitingOnResponses = state.notPersisted.values().stream().flatMap(Set::stream)
                                                        .collect(Collectors.toSet());
        }

        @Override
        public void onSuccess(Id from, ApplyAndCheckOk response)
        {
            state.refresh(null, null, null, null, response.notPersisted);
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            if (waitingOnResponses.remove(from) && waitingOnResponses.isEmpty())
                trySuccess(null);
        }

        @Override
        public void onCallbackFailure(Throwable failure)
        {
            tryFailure(failure);
        }
    }

    // TODO (now): pull, don't push, to simplify so we never perform partial application
    static class ApplyAndCheck extends Apply
    {
        final Map<KeyRange, Set<Id>> notPersistedOn;
        ApplyAndCheck(Id id, Topologies topologies, TxnId txnId, PartialRoute route, Deps deps, Timestamp executeAt, Writes writes, Result result, Map<KeyRange, Set<Id>> notPersistedOn)
        {
            super(id, topologies, txnId, route, executeAt, deps, writes, result);
            this.notPersistedOn = notPersistedOn;
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {
            // TODO (now): this progressKey is potentially invalid, as the scope may not be complete
            RoutingKey progressKey = node.trySelectProgressKey(txnId, scope);
            node.forEachLocalSince(scope(), executeAt, instance -> {
                Command command = instance.command(txnId);
                switch (command.apply(scope.homeKey, progressKey, executeAt, deps, writes, result))
                {
                    default:
                    case Insufficient:
                        throw new IllegalStateException();

                    case Partial:
                    case Success:
                    case Redundant:
                }
            });

            node.reply(from, replyContext, node.ifLocal(progressKey, txnId.epoch, instance -> {
                SimpleProgressLog.Instance log = ((SimpleProgressLog.Instance)instance.progressLog());
                State state = log.stateMap.get(txnId);
                if (state.globalState.notPersisted != null)
                {
                    state.globalState.retainAllNotPersistedOn(notPersistedOn);
                    notPersistedOn.forEach((k, s) -> {
                        Set<Id> retain = state.globalState.notPersisted.get(k);
                        if (retain != null)
                            s.retainAll(retain);
                    });
                }
                notPersistedOn.values().forEach(set -> set.remove(node.id()));
                return new ApplyAndCheckOk(notPersistedOn);
            }));
        }

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_AND_CHECK_REQ;
        }

        @Override
        public String toString()
        {
            return "SendAndCheck{" +
                   "txnId:" + txnId +
                   ", deps:" + deps +
                   ", executeAt:" + executeAt +
                   ", writes:" + writes +
                   ", result:" + result +
                   ", waitingOn:" + notPersistedOn +
                   '}';
        }
    }

    static class ApplyAndCheckOk implements Reply
    {
        final Map<KeyRange, Set<Id>> notPersisted;

        ApplyAndCheckOk(Map<KeyRange, Set<Id>> notPersisted)
        {
            this.notPersisted = notPersisted;
        }

        @Override
        public String toString()
        {
            return "SendAndCheckOk{" +
                   "waitingOn:" + notPersisted +
                   '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_AND_CHECK_RSP;
        }
    }

    @Override
    public ProgressLog create(CommandStore commandStore)
    {
        return new Instance(commandStore);
    }
}
