package accord.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.coordinate.FindHomeKey;
import accord.coordinate.FindRoute;
import accord.coordinate.Invalidate;
import accord.impl.SimpleProgressLog.HomeState.LocalStatus;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Callback;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.InformDurable;
import accord.messages.SimpleReply;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.coordinate.CheckOnCommitted.checkOnCommitted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.Durable;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.LocallyDurableOnly;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.GlobalState.GlobalStatus.RemotelyDurableOnly;
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

public class SimpleProgressLog implements Runnable, ProgressLog.Factory
{
    enum Progress
    {
        NoneExpected, Expected, NoProgress, Investigating, Done;

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
            Preconditions.checkState(command.owns(command.txnId().epoch, command.homeKey()));
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
                        if (!command.executes())
                        {
                            status = LocalStatus.Done;
                            progress = Done;
                            break;
                        }
                        // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                        // records to the home shard, so we only know the executeAt shards will have witnessed this
                        // if the home shard is at an earlier phase, it must run recovery
                        long epoch = command.executeAt().epoch;
                        node.withEpoch(epoch, () -> {
                            // TODO (now): slice the route to only those owned locally
                            debugInvestigating = checkOnCommitted(node, txnId, command.route(), epoch, epoch, (success, fail) -> {
                                // should have found enough information to apply the result, but in case we did not reset progress
                                if (progress == Investigating)
                                    progress = Expected;
                            });
                        });
                    }
                    else
                    {
                        RoutingKey homeKey = command.homeKey();
                        node.withEpoch(txnId.epoch, () -> {

                            Future<CheckStatusOk> recover = node.maybeRecover(txnId, command.homeKey(), command.route(), maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                            recover.addCallback((success, fail) -> {
                                if (status.isAtMost(ReadyToExecute) && progress == Investigating)
                                {
                                    progress = Expected;
                                    if (fail != null)
                                        return;

                                    // TODO: avoid returning null (need to change semantics here in this case, though, as Recover doesn't return CheckStatusOk)
                                    if (success == null || success.hasExecutedOnAllShards)
                                        command.setGloballyPersistent(homeKey, null);

                                    if (success != null)
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
        enum GlobalStatus { NotExecuted, RemotelyDurableOnly, LocallyDurableOnly, Durable, Done }

        static class PendingDurable
        {
            final Set<Id> persistedOn;

            PendingDurable(Set<Id> persistedOn)
            {
                this.persistedOn = persistedOn;
            }
        }

        // TODO: thread safety (schedule on progress log executor)
        class CoordinateAwareness implements Callback<SimpleReply>
        {
            @Override
            public void onSuccess(Id from, SimpleReply reply)
            {
                notAwareOfDurability.remove(from);
            }

            @Override
            public void onFailure(Id from, Throwable failure)
            {
            }

            @Override
            public void onCallbackFailure(Throwable failure)
            {
            }
        }

        GlobalStatus status = NotExecuted;
        Progress progress = NoneExpected;
        Set<Id> notAwareOfDurability;
        Set<Id> notPersisted;
        PendingDurable pendingDurable;

        CoordinateAwareness investigating;

        private boolean refresh(@Nullable Node node, @Nullable Command command, @Nullable Id persistedOn,
                                @Nullable Set<Id> persistedOns, @Nullable Set<Id> notPersistedOns)
        {
            if (notPersisted == null)
            {
                assert node != null && command != null;
                if (!node.topology().hasEpoch(command.executeAt().epoch))
                    return false;

                Topology topology = node.topology()
                                        .globalForEpoch(command.executeAt().epoch)
                                        .forKeys(command.route());

                if (pendingDurable != null)
                {
                    if (persistedOns == null) persistedOns = pendingDurable.persistedOn;
                    else persistedOns.addAll(pendingDurable.persistedOn);
                    pendingDurable = null;
                }

                notAwareOfDurability = new HashSet<>(topology.nodes());
                notPersisted = new HashSet<>(topology.nodes());
                if (status.compareTo(LocallyDurableOnly) >= 0)
                {
                    notAwareOfDurability.remove(node.id());
                    notPersisted.remove(node.id());
                }
            }

            if (persistedOn != null)
            {
                notPersisted.remove(persistedOn);
                notAwareOfDurability.remove(persistedOn);
            }
            if (persistedOns != null)
            {
                notPersisted.removeAll(persistedOns);
                notAwareOfDurability.removeAll(persistedOns);
            }
            if (notPersistedOns != null)
            {
                notPersisted.retainAll(notPersistedOns);
                notAwareOfDurability.retainAll(notPersistedOns);
            }

            return true;
        }

        void durable(Node node, Command command, Set<Id> persistedOn)
        {
            switch (status)
            {
                default: throw new IllegalStateException();
                case Done:
                case RemotelyDurableOnly:
                    if (persistedOn != null)
                    {
                        if (pendingDurable == null)
                            pendingDurable = new PendingDurable(persistedOn);
                        else
                            pendingDurable.persistedOn.addAll(persistedOn);
                    }
                    break;
                case NotExecuted:
                    status = RemotelyDurableOnly;
                    progress = NoneExpected;
                    if (persistedOn != null)
                        pendingDurable = new PendingDurable(persistedOn);
                    break;
                case LocallyDurableOnly:
                    status = Durable;
                    progress = Expected;
                    break;
                case Durable:
                    refresh(node, command, null, persistedOn, null);
            }
        }

        void durableLocal(Node node, Command command)
        {
            switch (status)
            {
                default: throw new IllegalStateException();
                case Done:
                case LocallyDurableOnly:
                    break;
                case NotExecuted:
                    status = LocallyDurableOnly;
                    break;
                case RemotelyDurableOnly:
                    status = Durable;
                    progress = Expected;
                    refresh(node, command, node.id(), null, null);
                case Durable:
            }
        }

        void update(Node node, TxnId txnId, Command command)
        {
            switch (status)
            {
                default: throw new IllegalStateException();
                case LocallyDurableOnly:
                case NotExecuted:
                case Done:
                    return;
                case Durable:
                case RemotelyDurableOnly:
            }

            if (progress != NoProgress)
            {
                progress = advance(progress);
                return;
            }

            progress = Investigating;
            if (notAwareOfDurability.isEmpty())
            {
                // TODO: also track actual durability
                status = GlobalStatus.Done;
                progress = Done;
                return;
            }

            Route route = (Route) command.route();
            Timestamp executeAt = command.executeAt();
            investigating = new CoordinateAwareness();
            Topologies topologies = node.topology().forEpoch(route, executeAt.epoch);
            node.send(notAwareOfDurability, to -> new InformDurable(to, topologies, route, txnId, executeAt), investigating);
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

        Object debugInvestigating;

        void recordBlocking(Command blocking, RoutingKeys someKeys)
        {
            Preconditions.checkState(!someKeys.isEmpty());
            switch (blocking.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case AcceptedInvalidate:
                    this.someKeys = someKeys;
                case PreAccepted:
                case Accepted:
                    if (blockedOn.compareTo(Status.Committed) < 0)
                    {
                        blockedOn = Status.Committed;
                        progress = Expected;
                    }
                    break;
                case Committed:
                case ReadyToExecute:
                    if (blockedOn.compareTo(Executed) < 0)
                    {
                        blockedOn = Executed;
                        progress = Expected;
                    }
                    break;
                case Executed:
                case Applied:
                case Invalidated:
                    throw new IllegalStateException("Should not be recorded as blocked if result already recorded locally");
            }
        }

        void recordBlocking(Status blockedOn, RoutingKeys someKeys)
        {
            Preconditions.checkState(!someKeys.isEmpty());
            this.someKeys = someKeys;
            if (blockedOn.compareTo(this.blockedOn) > 0)
            {
                this.blockedOn = blockedOn;
                progress = Expected;
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

            // first make sure we have enough information to obtain the command locally
            long srcEpoch = (command.hasBeen(Status.Committed) ? command.executeAt() : txnId).epoch;
            // TODO: compute fromEpoch, the epoch we already have this txn replicated until
            long toEpoch = Math.max(srcEpoch, node.topology().epoch());
            node.withEpoch(srcEpoch, () -> {

                // first check we have enough routing information for the ranges we own; if not, fetch it
                AbstractRoute route = route(command);
                KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, toEpoch);
                if (route == null || !route.covers(ranges))
                {
                    Status blockedOn = this.blockedOn;
                    BiConsumer<FindRoute.Result, Throwable> foundRoute = (findRoute, fail) -> {
                        if (progress == Investigating && blockedOn == this.blockedOn)
                        {
                            progress = Expected;
                            if (findRoute != null && !(someKeys instanceof Route))
                                someKeys = findRoute.route;
                            if (findRoute == null && fail == null)
                                invalidate(node, command);
                        }
                    };

                    if (command.homeKey() != null)
                    {
                        debugInvestigating = FindRoute.findRoute(node, txnId, command.homeKey(), foundRoute);
                    }
                    else
                    {
                        RoutingKeys someKeys = this.someKeys;
                        if (someKeys == null) someKeys = route;
                        debugInvestigating = FindHomeKey.findHomeKey(node, txnId, someKeys, (homeKey, fail) -> {
                            if (progress == Investigating && blockedOn == this.blockedOn)
                            {
                                if (fail != null) progress = Expected;
                                else if (homeKey != null) FindRoute.findRoute(node, txnId, homeKey, foundRoute);
                                else invalidate(node, command);
                            }
                        });
                    }
                    return;
                }

                // check status with the only keys we know, if any, then:
                // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
                // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
                BiConsumer<CheckStatusOkFull, Throwable> callback = (success, fail) -> {
                    if (progress != Investigating)
                        return;

                    progress = Expected;
                    if (fail != null)
                        return;

                    switch (success.fullStatus)
                    {
                        default:
                            throw new IllegalStateException();
                        case NotWitnessed:
                        case PreAccepted:
                        case Accepted:
                        case AcceptedInvalidate:
                            // the home shard is managing progress, give it time
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

                debugInvestigating = blockedOn == Executed ? checkOnCommitted(node, txnId, route, srcEpoch, toEpoch, callback)
                                                           : checkOnUncommitted(node, txnId, route, srcEpoch, toEpoch, callback);
            });
        }

        private AbstractRoute route(Command command)
        {
            return AbstractRoute.merge(command.route(), someKeys instanceof AbstractRoute ? (AbstractRoute) someKeys : null);
        }

        private void invalidate(Node node, Command command)
        {
            progress = Investigating;
            RoutingKey someKey = command.homeKey();
            if (someKey == null) someKey = someKeys.get(0);

            RoutingKeys someKeys = route(command);
            if (someKeys == null) someKeys = this.someKeys;
            if (someKeys == null || !someKeys.contains(someKey))
                someKeys = RoutingKeys.of(someKey);
            debugInvestigating = Invalidate.invalidate(node, command.txnId(), someKeys, someKey, (success, fail) -> {
                if (progress != Investigating) return;
                if (fail != null) progress = Expected;
                else switch (success)
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

        void recordBlocking(TxnId txnId, Status waitingFor, RoutingKeys someKeys)
        {
            Preconditions.checkArgument(txnId.equals(this.txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.recordBlocking(waitingFor, someKeys);
        }

        void recordBlocking(Command waitingFor, RoutingKeys someKeys)
        {
            Preconditions.checkArgument(waitingFor.txnId().equals(this.txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.recordBlocking(waitingFor, someKeys);
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
        public void unwitnessed(TxnId txnId, ProgressShard shard)
        {
            if (shard.isHome())
                ensure(txnId).ensureAtLeast(Uncommitted, Expected);
        }

        @Override
        public void preaccept(TxnId txnId, ProgressShard shard)
        {
            Preconditions.checkState(shard != Unsure);

            if (shard.isProgress())
            {
                State state = ensure(txnId);
                if (shard.isHome()) state.ensureAtLeast(Uncommitted, Expected);
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

        private void ensureSafeOrAtLeast(TxnId txnId, ProgressShard shard, LocalStatus newStatus, Progress newProgress)
        {
            Preconditions.checkState(shard != Unsure);

            State state = null;
            assert newStatus.isAtMost(ReadyToExecute);
            if (newStatus.isAtLeast(LocalStatus.Committed))
                state = recordCommit(txnId);

            if (shard.isProgress())
            {
                state = ensure(txnId, state);

                if (shard.isHome()) state.ensureAtLeast(newStatus, newProgress);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void accept(TxnId txnId, ProgressShard shard)
        {
            ensureSafeOrAtLeast(txnId, shard, Uncommitted, Expected);
        }

        @Override
        public void commit(TxnId txnId, ProgressShard shard)
        {
            ensureSafeOrAtLeast(txnId, shard, LocalStatus.Committed, NoneExpected);
        }

        @Override
        public void readyToExecute(TxnId txnId, ProgressShard shard)
        {
            ensureSafeOrAtLeast(txnId, shard, LocalStatus.ReadyToExecute, Expected);
        }

        @Override
        public void execute(TxnId txnId, ProgressShard shard)
        {
            Preconditions.checkState(shard != Unsure);
            State state = recordExecute(txnId);

            if (shard.isProgress())
            {
                state = ensure(txnId, state);

                if (shard.isHome()) state.home().executed();
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void invalidate(TxnId txnId, ProgressShard shard)
        {
            State state = recordExecute(txnId);

            Preconditions.checkState(shard == Home || state == null || state.homeState == null);

            // note: we permit Unsure here, so we check if we have any local home state
            if (shard.isProgress())
            {
                state = ensure(txnId, state);

                if (shard.isHome()) state.ensureAtLeast(LocalStatus.Done, Done);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void durableLocal(TxnId txnId)
        {
            State state = ensure(txnId);
            ensure(txnId).global().durableLocal(node, state.command);
        }

        @Override
        public void durable(TxnId txnId, Set<Id> persistedOn)
        {
            State state = ensure(txnId);
            state.global().durable(node, state.command, persistedOn);
        }

        @Override
        public void durable(TxnId txnId, AbstractRoute route, ProgressShard shard)
        {
            ensure(txnId).recordBlocking(txnId, Executed, null);
        }

        @Override
        public void waiting(TxnId blockedBy, RoutingKeys someKeys)
        {
            // TODO (now): propagate to progress shard, if known
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

    @Override
    public ProgressLog create(CommandStore commandStore)
    {
        return new Instance(commandStore);
    }
}
