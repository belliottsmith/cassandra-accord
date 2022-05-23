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
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.Durable;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.Disseminating;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.PendingDurable;
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

    static class GlobalPendingDurable
    {
        final Set<Id> fullyPersistedOn;

        GlobalPendingDurable(Set<Id> fullyPersistedOn)
        {
            this.fullyPersistedOn = fullyPersistedOn;
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

        enum GlobalStatus
        {
            NotExecuted, Disseminating, PendingDurable, Durable, Done; // TODO: manage propagating from Durable to everyone
            boolean isAtLeast(GlobalStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
        }

        LocalStatus local = LocalStatus.NotWitnessed;
        Progress localProgress = NoneExpected;
        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        GlobalStatus global = NotExecuted;
        Progress globalProgress = NoneExpected;
        Map<KeyRange, Set<Id>> globalNotPersisted;
        GlobalPendingDurable globalPendingDurable;

        Object debugInvestigating;

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Node node, Command command)
        {
            if (newStatus == Committed && global.isAtLeast(Durable) && !command.executes())
            {
                local = LocalStatus.Done;
                localProgress = Done;
            }
            else if (newStatus.compareTo(local) > 0)
            {
                local = newStatus;
                localProgress = newProgress;
            }
            refreshGlobal(node, command, null, null, null);
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

        private boolean refreshGlobal(@Nullable Node node, @Nullable Command command, @Nullable Id fullyPersistedOn,
                                      @Nullable Set<Id> fullyPersistedOns, @Nullable Map<KeyRange, Set<Id>> notPersistedOns)
        {
            if (global == NotExecuted)
                return false;

            if (globalPendingDurable != null)
            {
                if (node == null || command == null || command.is(Status.NotWitnessed))
                    return false;

                if (fullyPersistedOns == null) fullyPersistedOns = globalPendingDurable.fullyPersistedOn;
                else fullyPersistedOns.addAll(globalPendingDurable.fullyPersistedOn);

                global = Durable;
                globalProgress = Expected;
            }

            if (globalNotPersisted == null)
            {
                assert node != null && command != null;
                if (!node.topology().hasEpoch(command.executeAt().epoch))
                    return false;

                Topology topology = node.topology()
                                        .globalForEpoch(command.executeAt().epoch)
                                        .forKeys(command.routeForKeysOwnedAtExecution());

                globalNotPersisted = Maps.newHashMapWithExpectedSize(topology.size());
                for (Shard shard : topology)
                {
                    Set<Id> remaining = new HashSet<>(shard.nodes);
                    if (local == LocalStatus.Done)
                        remaining.remove(node.id());
                    globalNotPersisted.put(shard.range, remaining);
                }
            }
            if (globalNotPersisted != null)
            {
                if (fullyPersistedOn != null)
                {
                    globalNotPersisted.values().forEach(remaining -> remaining.remove(fullyPersistedOn));
                }
                if (fullyPersistedOns != null)
                {
                    Set<Id> remove = fullyPersistedOns;
                    globalNotPersisted.values().forEach(remaining -> remaining.removeAll(remove));
                }
                if (notPersistedOns != null)
                {
                    retainAllNotPersistedOn(notPersistedOns);
                }
                if (globalNotPersisted.isEmpty())
                {
                    global = GlobalStatus.Done;
                    globalProgress = Done;
                }
            }

            return true;
        }

        private void retainAllNotPersistedOn(Map<KeyRange, Set<Id>> notPersistedOn)
        {
            notPersistedOn.forEach((k, s) -> {
                Set<Id> remaining = globalNotPersisted.get(k);
                if (remaining == null)
                    return;
                remaining.retainAll(s);
            });
        }

        void executedOnAllShards(Node node, Command command, Set<Id> persistedOn)
        {
            if (local == LocalStatus.NotWitnessed)
            {
                global = PendingDurable;
                globalProgress = NoneExpected;
                globalPendingDurable = new GlobalPendingDurable(persistedOn);
            }
            else if (global != GlobalStatus.Done)
            {
                global = Durable;
                globalProgress = Expected;
                refreshGlobal(node, command, null, persistedOn, null);
                if (local.isAtLeast(Committed) && !command.executes())
                {
                    local = LocalStatus.Done;
                    localProgress = Done;
                }
            }
        }

        void executed(Node node, Command command)
        {
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    local = LocalStatus.Done;
                    localProgress = NoneExpected;
                    if (global == NotExecuted)
                    {
                        global = Disseminating;
                        globalProgress = Expected;
                    }
                    refreshGlobal(node, command, node.id(), null, null);
                case Done:
            }
        }

        void updateLocal(Node node, TxnId txnId, Command command)
        {
            if (localProgress != NoProgress)
            {
                localProgress = advance(localProgress);
                return;
            }

            localProgress = Investigating;
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Committed:
                    throw new IllegalStateException(); // NoProgressExpected

                case Uncommitted:
                case ReadyToExecute:
                {
                    if (local.isAtLeast(Committed) && global.isAtLeast(PendingDurable))
                    {
                        // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                        // records to the home shard, so we only know the executeAt shards will have witnessed this
                        // if the home shard is at an earlier phase, it must run recovery
                        long epoch = command.executeAt().epoch;
                        node.withEpoch(epoch, () -> {
                            debugInvestigating = checkOnCommitted(node, txnId, command.routingKeys(), command.executeAt().epoch, (success, fail) -> {
                                // should have found enough information to apply the result, but in case we did not reset progress
                                if (localProgress == Investigating)
                                    localProgress = Expected;
                            });
                        });
                    }
                    else
                    {
                        RoutingKey homeKey = command.homeKey();
                        long homeEpoch = (local.isAtMost(Uncommitted) ? txnId : command.executeAt()).epoch;

                        node.withEpoch(homeEpoch, () -> {

                            Future<CheckStatusOk> recover = node.maybeRecover(txnId, homeKey, homeEpoch,
                                                                              maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                            recover.addCallback((success, fail) -> {
                                if (local.isAtMost(ReadyToExecute) && localProgress == Investigating)
                                {
                                    localProgress = Expected;
                                    if (fail != null)
                                        return;

                                    if (success == null || success.hasExecutedOnAllShards)
                                        executedOnAllShards(node, command, null);
                                    else
                                        updateMax(success);
                                }
                            });

                            debugInvestigating = recover;
                        });
                    }
                }
                case Done:
            }
        }

        void updateGlobal(Node node, TxnId txnId, Command command)
        {
            if (!refreshGlobal(node, command, null, null, null))
                return;

            if (global != Disseminating)
                return;

            if (!command.hasBeen(Executed))
                return;

            if (globalProgress != NoProgress)
            {
                globalProgress = advance(globalProgress);
                return;
            }

            globalProgress = Investigating;
            applyAndCheck(node, txnId, command, this).addCallback((success, fail) -> {
                if (globalProgress != Done)
                    globalProgress = Expected;
            });
        }

        void update(Node node, TxnId txnId, Command command)
        {
            updateLocal(node, txnId, command);
            updateGlobal(node, txnId, command);
        }

        @Override
        public String toString()
        {
            return "{" + local + ',' + localProgress + ',' + global + ',' + globalProgress + '}';
        }
    }

    static class GlobalState
    {
        enum GlobalStatus
        {
            NotExecuted, Disseminating, PendingDurable, Durable, Done; // TODO: manage propagating from Durable to everyone
            boolean isAtLeast(GlobalStatus equalOrGreaterThan)
            {
                return compareTo(equalOrGreaterThan) >= 0;
            }
        }

        GlobalStatus global = NotExecuted;
        Progress globalProgress = NoneExpected;
        Map<KeyRange, Set<Id>> globalNotPersisted;
        GlobalPendingDurable globalPendingDurable;

        Object debugInvestigating;

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Node node, Command command)
        {
            if (newStatus == Committed && global.isAtLeast(Durable) && !command.executes())
            {
                local = LocalStatus.Done;
                localProgress = Done;
            }
            else if (newStatus.compareTo(local) > 0)
            {
                local = newStatus;
                localProgress = newProgress;
            }
            refreshGlobal(node, command, null, null, null);
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

        private boolean refreshGlobal(@Nullable Node node, @Nullable Command command, @Nullable Id fullyPersistedOn,
                                      @Nullable Set<Id> fullyPersistedOns, @Nullable Map<KeyRange, Set<Id>> notPersistedOns)
        {
            if (global == NotExecuted)
                return false;

            if (globalPendingDurable != null)
            {
                if (node == null || command == null || command.is(Status.NotWitnessed))
                    return false;

                if (fullyPersistedOns == null) fullyPersistedOns = globalPendingDurable.fullyPersistedOn;
                else fullyPersistedOns.addAll(globalPendingDurable.fullyPersistedOn);

                global = Durable;
                globalProgress = Expected;
            }

            if (globalNotPersisted == null)
            {
                assert node != null && command != null;
                if (!node.topology().hasEpoch(command.executeAt().epoch))
                    return false;

                Topology topology = node.topology()
                                        .globalForEpoch(command.executeAt().epoch)
                                        .forKeys(command.routeForKeysOwnedAtExecution());

                globalNotPersisted = Maps.newHashMapWithExpectedSize(topology.size());
                for (Shard shard : topology)
                {
                    Set<Id> remaining = new HashSet<>(shard.nodes);
                    if (local == LocalStatus.Done)
                        remaining.remove(node.id());
                    globalNotPersisted.put(shard.range, remaining);
                }
            }
            if (globalNotPersisted != null)
            {
                if (fullyPersistedOn != null)
                {
                    globalNotPersisted.values().forEach(remaining -> remaining.remove(fullyPersistedOn));
                }
                if (fullyPersistedOns != null)
                {
                    Set<Id> remove = fullyPersistedOns;
                    globalNotPersisted.values().forEach(remaining -> remaining.removeAll(remove));
                }
                if (notPersistedOns != null)
                {
                    retainAllNotPersistedOn(notPersistedOns);
                }
                if (globalNotPersisted.isEmpty())
                {
                    global = GlobalStatus.Done;
                    globalProgress = Done;
                }
            }

            return true;
        }

        private void retainAllNotPersistedOn(Map<KeyRange, Set<Id>> notPersistedOn)
        {
            notPersistedOn.forEach((k, s) -> {
                Set<Id> remaining = globalNotPersisted.get(k);
                if (remaining == null)
                    return;
                remaining.retainAll(s);
            });
        }

        void executedOnAllShards(Node node, Command command, Set<Id> persistedOn)
        {
            if (local == LocalStatus.NotWitnessed)
            {
                global = PendingDurable;
                globalProgress = NoneExpected;
                globalPendingDurable = new GlobalPendingDurable(persistedOn);
            }
            else if (global != GlobalStatus.Done)
            {
                global = Durable;
                globalProgress = Expected;
                refreshGlobal(node, command, null, persistedOn, null);
                if (local.isAtLeast(Committed) && !command.executes())
                {
                    local = LocalStatus.Done;
                    localProgress = Done;
                }
            }
        }

        void executed(Node node, Command command)
        {
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    local = LocalStatus.Done;
                    localProgress = NoneExpected;
                    if (global == NotExecuted)
                    {
                        global = Disseminating;
                        globalProgress = Expected;
                    }
                    refreshGlobal(node, command, node.id(), null, null);
                case Done:
            }
        }

        void updateLocal(Node node, TxnId txnId, Command command)
        {
            if (localProgress != NoProgress)
            {
                localProgress = advance(localProgress);
                return;
            }

            localProgress = Investigating;
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Committed:
                    throw new IllegalStateException(); // NoProgressExpected

                case Uncommitted:
                case ReadyToExecute:
                {
                    if (local.isAtLeast(Committed) && global.isAtLeast(PendingDurable))
                    {
                        // must also be committed, as at the time of writing we do not guarantee dissemination of Commit
                        // records to the home shard, so we only know the executeAt shards will have witnessed this
                        // if the home shard is at an earlier phase, it must run recovery
                        long epoch = command.executeAt().epoch;
                        node.withEpoch(epoch, () -> {
                            debugInvestigating = checkOnCommitted(node, txnId, command.routingKeys(), command.executeAt().epoch, (success, fail) -> {
                                // should have found enough information to apply the result, but in case we did not reset progress
                                if (localProgress == Investigating)
                                    localProgress = Expected;
                            });
                        });
                    }
                    else
                    {
                        RoutingKey homeKey = command.homeKey();
                        long homeEpoch = (local.isAtMost(Uncommitted) ? txnId : command.executeAt()).epoch;

                        node.withEpoch(homeEpoch, () -> {

                            Future<CheckStatusOk> recover = node.maybeRecover(txnId, homeKey, homeEpoch,
                                                                              maxStatus, maxPromised, maxPromiseHasBeenAccepted);
                            recover.addCallback((success, fail) -> {
                                if (local.isAtMost(ReadyToExecute) && localProgress == Investigating)
                                {
                                    localProgress = Expected;
                                    if (fail != null)
                                        return;

                                    if (success == null || success.hasExecutedOnAllShards)
                                        executedOnAllShards(node, command, null);
                                    else
                                        updateMax(success);
                                }
                            });

                            debugInvestigating = recover;
                        });
                    }
                }
                case Done:
            }
        }

        void updateGlobal(Node node, TxnId txnId, Command command)
        {
            if (!refreshGlobal(node, command, null, null, null))
                return;

            if (global != Disseminating)
                return;

            if (!command.hasBeen(Executed))
                return;

            if (globalProgress != NoProgress)
            {
                globalProgress = advance(globalProgress);
                return;
            }

            globalProgress = Investigating;
            applyAndCheck(node, txnId, command, this).addCallback((success, fail) -> {
                if (globalProgress != Done)
                    globalProgress = Expected;
            });
        }

        void update(Node node, TxnId txnId, Command command)
        {
            updateLocal(node, txnId, command);
            updateGlobal(node, txnId, command);
        }

        @Override
        public String toString()
        {
            return "{" + local + ',' + localProgress + ',' + global + ',' + globalProgress + '}';
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

        void ensureAtLeast(LocalStatus newStatus, Progress newProgress, Node node)
        {
            home().ensureAtLeast(newStatus, newProgress, node, command);
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
                ensure(txnId).ensureAtLeast(Uncommitted, Expected, node);
        }

        @Override
        public void preaccept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
            if (isProgressShard)
            {
                State state = ensure(txnId);
                if (isHomeShard) state.ensureAtLeast(Uncommitted, Expected, node);
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

                if (isHomeShard) state.ensureAtLeast(newStatus, newProgress, node);
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

                if (isHomeShard) state.home().executed(node, state.command);
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

                if (isHomeShard) state.ensureAtLeast(LocalStatus.Done, Done, node);
                else ensure(txnId).ensureAtLeast(Safe);
            }
        }

        @Override
        public void executedOnAllShards(TxnId txnId, Set<Id> persistedOn)
        {
            State state = ensure(txnId);
            state.home().executedOnAllShards(node, state.command, persistedOn);
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
        final Command command;
        final HomeState state;
        final Set<Id> waitingOnResponses;

        static Future<Void> applyAndCheck(Node node, TxnId txnId, Command command, HomeState state)
        {
            CoordinateApplyAndCheck coordinate = new CoordinateApplyAndCheck(txnId, command, state);
            PartialRoute route = command.routeForKeysOwnedAtExecution();
            Topologies topologies = node.topology().preciseEpochs(route, command.executeAt().epoch);
            node.send(topologies.nodes(), id -> new ApplyAndCheck(id, topologies, command.txnId(), route,
                                                                        command.savedPartialDeps(), command.executeAt(),
                                                                        command.writes(), command.result(),
                                                                        state.globalNotPersisted),
                      coordinate);
            return coordinate;
        }

        CoordinateApplyAndCheck(TxnId txnId, Command command, HomeState state)
        {
            this.txnId = txnId;
            this.command = command;
            this.state = state;
            this.waitingOnResponses = state.globalNotPersisted.values().stream().flatMap(Set::stream)
                                                              .collect(Collectors.toSet());
        }

        @Override
        public void onSuccess(Id from, ApplyAndCheckOk response)
        {
            state.refreshGlobal(null, null, null, null, response.notPersisted);
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
                if (state.homeState.globalNotPersisted != null)
                {
                    state.homeState.retainAllNotPersistedOn(notPersistedOn);
                    notPersistedOn.forEach((k, s) -> {
                        Set<Id> retain = state.homeState.globalNotPersisted.get(k);
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
