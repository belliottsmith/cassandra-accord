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

package accord.local;

import accord.api.*;
import accord.primitives.*;
import accord.primitives.Writes;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.utils.Utils.listOf;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.ProgressLog.ProgressShard;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.api.ProgressLog.ProgressShard.No;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Command.EnsureAction.Add;
import static accord.local.Command.EnsureAction.Check;
import static accord.local.Command.EnsureAction.Ignore;
import static accord.local.Command.EnsureAction.Set;
import static accord.local.Command.EnsureAction.TrySet;
import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.Invalidated;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

public abstract class Command implements Listener, Consumer<Listener>, TxnOperation
{
    private static final Logger logger = LoggerFactory.getLogger(Command.class);

    public abstract TxnId txnId();
    public abstract CommandStore commandStore();

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    public abstract RoutingKey homeKey();
    protected abstract void setHomeKey(RoutingKey key);

    public abstract RoutingKey progressKey();
    protected abstract void setProgressKey(RoutingKey key);

    /**
     * If this is the home shard, we require that this is a Route for all states > NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     *
     * TODO: maybe set this for all local shards, but slice to only those participating keys
     * (would probably need to remove hashIntersects)
     */
    public abstract AbstractRoute route();
    protected abstract void setRoute(AbstractRoute route);

    public abstract PartialTxn partialTxn();
    protected abstract void setPartialTxn(PartialTxn txn);

    public abstract Ballot promised();
    protected abstract void setPromised(Ballot ballot);

    public abstract Ballot accepted();
    protected abstract void setAccepted(Ballot ballot);

    public void saveRoute(Route route)
    {
        setRoute(route);
        updateHomeKey(route.homeKey);
    }

    public abstract Timestamp executeAt();
    protected abstract void setExecuteAt(Timestamp timestamp);

    /**
     * While !hasBeen(Committed), used only as a register for Accept state, used by Recovery
     * If hasBeen(Committed), represents the full deps owned by this range for execution at both txnId.epoch
     * AND executeAt.epoch so that it may be used for Recovery (which contacts only txnId.epoch topology),
     * but also for execution.
     */
    public abstract PartialDeps partialDeps();
    protected abstract void setPartialDeps(PartialDeps deps);

    public abstract Writes writes();
    protected abstract void setWrites(Writes writes);

    public abstract Result result();
    protected abstract void setResult(Result result);

    public abstract Status status();
    protected abstract void setStatus(Status status);

    public abstract boolean isGloballyPersistent();
    public abstract void setGloballyPersistent(boolean v);

    public abstract Command addListener(Listener listener);
    public abstract void removeListener(Listener listener);
    protected abstract void notifyListeners();

    protected abstract void addWaitingOnCommit(Command command);
    protected abstract boolean isWaitingOnCommit();
    protected abstract void removeWaitingOnCommit(Command command);
    protected abstract Command firstWaitingOnCommit();

    protected abstract void addWaitingOnApplyIfAbsent(Command command);
    protected abstract boolean isWaitingOnApply();
    protected abstract void removeWaitingOn(Command command);
    protected abstract Command firstWaitingOnApply();

    protected boolean isUnableToApply()
    {
        return isWaitingOnCommit() || isWaitingOnApply();
    }

    public boolean hasBeenWitnessed()
    {
        return partialTxn() != null;
    }

    public boolean hasBeen(Status status)
    {
        return status().hasBeen(status);
    }

    public boolean is(Status status)
    {
        return status() == status;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId()), partialDeps().txnIds());
    }

    @Override
    public Iterable<? extends RoutingKey> keys()
    {
        return partialTxn().keys();
    }

    public void setGloballyPersistent(RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        updateHomeKey(homeKey);
        if (executeAt != null && hasBeen(Committed) && !this.executeAt().equals(executeAt))
            commandStore().agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        setGloballyPersistent(true);
    }

    public enum AcceptOutcome
    {
        Success, Insufficient, Redundant, RejectedBallot
    }

    public AcceptOutcome preaccept(PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        if (promised().compareTo(Ballot.ZERO) > 0)
            return AcceptOutcome.RejectedBallot;

        return preacceptInternal(partialTxn, route, progressKey);
    }

    private AcceptOutcome preacceptInternal(PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        if (executeAt() != null)
        {
            logger.trace("{}: skipping preaccept - already preaccepted ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        Status status = status();
        switch (status)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
            case AcceptedInvalidate:
            case Invalidated:
        }

        KeyRanges coordinateRanges = coordinateRanges();
        ProgressShard shard = progressShard(route, progressKey, coordinateRanges);
        if (!validate(KeyRanges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore))
            throw new IllegalStateException();

        Timestamp max = commandStore().maxConflict(partialTxn.keys());
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        TxnId txnId = txnId();
        setExecuteAt(txnId.compareTo(max) > 0 && txnId.epoch >= commandStore().latestEpoch()
                         ? txnId : commandStore().uniqueNow(max));

        set(KeyRanges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore);
        if (status == NotWitnessed)
            setStatus(PreAccepted);

        commandStore().progressLog().preaccept(txnId, shard);

        notifyListeners();
        return AcceptOutcome.Success;
    }

    public boolean preacceptInvalidate(Ballot ballot)
    {
        if (promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preacceptInvalidate - witnessed higher ballot ({})", txnId(), promised());
            return false;
        }
        setPromised(ballot);
        return true;
    }

    public AcceptOutcome accept(Ballot ballot, PartialRoute route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping accept - already committed ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        TxnId txnId = txnId();
        KeyRanges coordinateRanges = coordinateRanges();
        KeyRanges executeRanges = txnId.epoch == executeAt.epoch ? coordinateRanges : commandStore().ranges().at(executeAt.epoch);
        ProgressShard shard = progressShard(route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set))
        {
            logger.trace("{}: insufficient info to accept {}, ", txnId(), status());
            return AcceptOutcome.Insufficient;
        }

        setExecuteAt(executeAt);
        setPromised(ballot);
        setAccepted(ballot);
        set(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);
        setStatus(Accepted);

        this.commandStore().progressLog().accept(txnId, shard);
        notifyListeners();

        return AcceptOutcome.Success;
    }

    public AcceptOutcome acceptInvalidate(Ballot ballot)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping accept invalidated - already committed ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        setPromised(ballot);
        setAccepted(ballot);
        setStatus(AcceptedInvalidate);
        logger.trace("{}: accepted invalidated", txnId());

        notifyListeners();
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Redundant, Insufficient }

    // relies on mutual exclusion for each key
    public CommitOutcome commit(AbstractRoute route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping commit - already committed ({})", txnId(), status());
            if (executeAt.equals(executeAt()) && status() != Invalidated)
                return CommitOutcome.Redundant;

            commandStore().agent().onInconsistentTimestamp(this, (status() == Invalidated ? Timestamp.NONE : this.executeAt()), executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges();
        // TODO (now): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        KeyRanges executeRanges = executeRanges(executeAt);
        ProgressShard shard = progressShard(route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set))
            return CommitOutcome.Insufficient;

        setExecuteAt(executeAt);
        set(coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        setStatus(Committed);
        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId(), executeAt, partialDeps);
        populateWaitingOn();

        commandStore().progressLog().commit(txnId(), shard);

        // TODO (now): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
        maybeExecute(shard, true);
        return CommitOutcome.Success;
    }

    protected void populateWaitingOn()
    {
        KeyRanges ranges = commandStore().ranges().since(executeAt().epoch);
        if (ranges != null) {
            partialDeps().forEachOn(ranges, commandStore()::hashIntersects, txnId -> {
                Command command = commandStore().command(txnId);
                switch (command.status()) {
                    default:
                        throw new IllegalStateException();
                    case NotWitnessed:
                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                        // we don't know when these dependencies will execute, and cannot execute until we do
                        command.addListener(this);
                        addWaitingOnCommit(command);
                        break;
                    case Committed:
                        // TODO: split into ReadyToRead and ReadyToWrite;
                        //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                        //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                    case ReadyToExecute:
                    case Executed:
                    case Applied:
                        command.addListener(this);
                        updatePredecessor(command, true);
                    case Invalidated:
                        break;
                }
            });
        }
    }

    // TODO (now): commitInvalidate may need to update cfks _if_ possible
    public void commitInvalidate()
    {
        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping commit invalidated - already committed ({})", txnId(), status());
            if (!hasBeen(Invalidated))
                commandStore().agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt());

            return;
        }

        ProgressShard shard = progressShard();
        commandStore().progressLog().invalidate(txnId(), shard);
        setExecuteAt(txnId());
        if (partialDeps() == null)
            setPartialDeps(PartialDeps.NONE);
        setStatus(Invalidated);
        logger.trace("{}: committed invalidated", txnId());

        notifyListeners();
    }

    public enum ApplyOutcome { Success, Redundant, OutOfRange, Insufficient }

    public ApplyOutcome apply(long untilEpoch, AbstractRoute route, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", txnId(), status());
            return ApplyOutcome.Redundant;
        }
        else if (hasBeen(Committed) && !executeAt.equals(this.executeAt()))
        {
            commandStore().agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges();
        KeyRanges executeRanges = executeRanges(executeAt);
        if (untilEpoch < commandStore().latestEpoch())
        {
            // at present this should only happen if we receive an Apply message that was scoped to a node's earlier ownership
            // in which case this might be routed to a new shard on the replica; it shouldn't imply any
            KeyRanges expectedRanges = commandStore().ranges().between(executeAt.epoch, untilEpoch);
            if (!expectedRanges.contains(executeRanges))
                return ApplyOutcome.OutOfRange;
        }
        ProgressShard shard = progressShard(route, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet))
            return ApplyOutcome.Insufficient; // TODO: this should probably be an assertion failure if !TrySet

        setWrites(writes);
        setResult(result);
        setExecuteAt(executeAt);
        set(coordinateRanges, executeRanges, shard, route, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet);

        if (!hasBeen(Committed))
            populateWaitingOn();
        setStatus(Executed);
        logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", txnId(), executeAt, partialDeps);

        commandStore().progressLog().execute(txnId(), shard);

        maybeExecute(shard, true);
        return ApplyOutcome.Success;
    }

    public AcceptOutcome recover(PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        if (promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preaccept invalidate - higher ballot witnessed ({})", txnId(), promised());
            return AcceptOutcome.RejectedBallot;
        }

        if (executeAt() == null)
        {
            Preconditions.checkState(status() == NotWitnessed || status() == AcceptedInvalidate);
            switch (preacceptInternal(partialTxn, route, progressKey))
            {
                default:
                case RejectedBallot:
                case Insufficient:
                case Redundant:
                    throw new IllegalStateException();

                case Success:
            }
        }

        setPromised(ballot);
        return AcceptOutcome.Success;
    }

    @Override
    public TxnOperation listenerScope(TxnId caller)
    {
        return TxnOperation.scopeFor(listOf(txnId(), caller), Collections.emptyList());
    }

    @Override
    public void onChange(Command command)
    {
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     txnId(), command.txnId(), command.status(), command);
        switch (command.status())
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case Committed:
            case ReadyToExecute:
            case Executed:
            case Applied:
            case Invalidated:
                updatePredecessor(command, false);
                maybeExecute(progressShard(), false);
                break;
        }
    }

    protected void postApply()
    {
        logger.trace("{} applied, setting status to Applied and notifying listeners", txnId());
        setStatus(Applied);
        notifyListeners();
    }

    private static Function<CommandStore, Void> callPostApply(TxnId txnId)
    {
        return commandStore -> {
            commandStore.command(txnId).postApply();
            return null;
        };
    }

    protected Future<Void> apply()
    {
        // important: we can't include a reference to *this* in the lambda, since the C* implementation may evict
        // the command instance from memory between now and the write completing (and post apply being called)
        return writes().apply(commandStore()).flatMap(unused ->
            commandStore().process(this, callPostApply(txnId()))
        );
    }

    public void read(BiConsumer<Data, Throwable> callback)
    {
        partialTxn().read(this, callback);
    }

    private void maybeExecute(ProgressShard shard, boolean alwaysNotifyListeners)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}", txnId(), status(), alwaysNotifyListeners);

        if (status() != Committed && status() != Executed)
        {
            if (alwaysNotifyListeners)
                notifyListeners();
            return;
        }

        if (isUnableToApply())
        {
            BlockedBy blockedBy = blockedBy();
            if (blockedBy != null)
            {
                logger.trace("{}: not executing, blocked on {}", txnId(), blockedBy.txnId);
                commandStore().progressLog().waiting(blockedBy.txnId, blockedBy.until, blockedBy.someKeys);
                if (alwaysNotifyListeners) notifyListeners();
                return;
            }
            assert !isWaitingOnApply();
        }

        switch (status())
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                setStatus(ReadyToExecute);
                logger.trace("{}: set to ReadyToExecute", txnId());
                commandStore().progressLog().readyToExecute(txnId(), shard);
                notifyListeners();
                break;

            case Executed:
                if (executeRanges(executeAt()).intersects(writes().keys, commandStore()::hashIntersects))
                {
                    logger.trace("{}: applying", txnId());
                    apply();
                }
                else
                {
                    logger.trace("{}: applying no-op", txnId());
                    setStatus(Applied);
                    notifyListeners();
                }
        }
    }

    /**
     * @param dependency is either committed or invalidated
     * @param isInsert true iff this is initial {@code populateWaitingOn} call
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private boolean updatePredecessor(Command dependency, boolean isInsert)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", txnId(), dependency.txnId());
            dependency.removeListener(this);
            removeWaitingOnCommit(dependency); // TODO (now): this was missing in partial-replication; might be redundant?
            return true;
        }
        else if (dependency.executeAt().compareTo(executeAt()) > 0)
        {
            // cannot be a predecessor if we execute later
            logger.trace("{}: {} executes after us. Stop listening and removing from waiting on apply set.", txnId(), dependency.txnId());
            removeWaitingOn(dependency);
            dependency.removeListener(this);
            return true;
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Stop listening and removing from waiting on apply set.", txnId(), dependency.txnId());
            removeWaitingOn(dependency);
            dependency.removeListener(this);
            return true;
        }
        else if (isUnableToApply())
        {
            logger.trace("{}: adding {} to waiting on apply set.", txnId(), dependency.txnId());
            addWaitingOnApplyIfAbsent(dependency);
            removeWaitingOnCommit(dependency);
            return false;
        }
        else if (isInsert)
        {
            logger.trace("{}: adding {} to waiting on commit set.", txnId(), dependency.txnId());
            addWaitingOnCommit(dependency);
            return false;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    // TEMPORARY: once we can invalidate commands that have not been witnessed on any shard, we do not need to know the home shard
    static class BlockedBy
    {
        final TxnId txnId;
        final Status until;
        final RoutingKeys someKeys; // some keys we know to be associated with this txnId, unlikely to be exhaustive

        BlockedBy(TxnId txnId, Status until, RoutingKeys someKeys)
        {
            this.txnId = txnId;
            this.until = until;
            this.someKeys = someKeys;
        }
    }

    public BlockedBy blockedBy()
    {
        Command prev = this;
        Command cur = directlyBlockedBy();
        if (cur == null)
            return null;

        Command next;
        while (null != (next = cur.directlyBlockedBy()))
        {
            prev = cur;
            cur = next;
        }

        RoutingKeys someKeys = cur.someRoutingKeys();
        if (someKeys == null)
            someKeys = prev.partialDeps().someRoutingKeys(cur.txnId());
        return new BlockedBy(cur.txnId(), cur.hasBeen(Committed) ? Executed : Committed, someKeys);
    }

    /**
     * A key nominated to represent the "home" shard - only members of the home shard may be nominated to recover
     * a transaction, to reduce the cluster-wide overhead of ensuring progress. A transaction that has only been
     * witnessed at PreAccept may however trigger a process of ensuring the home shard is durably informed of
     * the transaction.
     *
     * Note that for ProgressLog purposes the "home shard" is the shard as of txnId.epoch.
     * For recovery purposes the "home shard" is as of txnId.epoch until Committed, and executeAt.epoch once Executed
     *
     * TODO: Markdown documentation explaining the home shard and local shard concepts
     */

    public final void homeKey(Key homeKey)
    {
        RoutingKey current = homeKey();
        if (current == null) setHomeKey(homeKey);
        else if (!current.equals(homeKey)) throw new AssertionError();
    }

    public void updateHomeKey(RoutingKey homeKey)
    {
        if (homeKey() == null)
        {
            setHomeKey(homeKey);
            if (progressKey() == null && owns(txnId().epoch, homeKey))
                progressKey(homeKey);
        }
        else if (!this.homeKey().equals(homeKey))
        {
            throw new IllegalStateException();
        }
    }

    private ProgressShard progressShard(AbstractRoute route, @Nullable RoutingKey progressKey, KeyRanges coordinateRanges)
    {
        updateHomeKey(route.homeKey);

        if (progressKey == null || progressKey == NO_PROGRESS_KEY)
        {
            if (this.progressKey() == null)
                setProgressKey(NO_PROGRESS_KEY);

            return No;
        }

        if (this.progressKey() == null) setProgressKey(progressKey);
        else if (!this.progressKey().equals(progressKey)) throw new AssertionError();

        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!commandStore().hashIntersects(progressKey))
            return No;

        return progressKey.equals(homeKey()) ? Home : Local;
    }

    /**
     * A key nominated to be the primary shard within this node for managing progress of the command.
     * It is nominated only as of txnId.epoch, and may be null (indicating that this node does not monitor
     * the progress of this command).
     *
     * Preferentially, this is homeKey on nodes that replicate it, and otherwise any key that is replicated, as of txnId.epoch
     */

    public final void progressKey(RoutingKey progressKey)
    {
        RoutingKey current = progressKey();
        if (current == null) setProgressKey(progressKey);
        else if (!current.equals(progressKey)) throw new AssertionError();
    }

    private ProgressShard progressShard(AbstractRoute route, KeyRanges coordinateRanges)
    {
        if (progressKey() == null)
            return Unsure;

        return progressShard(route, progressKey(), coordinateRanges);
    }

    private ProgressShard progressShard()
    {
        RoutingKey progressKey = progressKey();
        if (progressKey == null)
            return Unsure;

        if (progressKey == NO_PROGRESS_KEY)
            return No;

        KeyRanges coordinateRanges = commandStore().ranges().at(txnId().epoch);
        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!commandStore().hashIntersects(progressKey))
            return No;

        return progressKey.equals(homeKey()) ? Home : Local;
    }

    private KeyRanges coordinateRanges()
    {
        return commandStore().ranges().at(txnId().epoch);
    }

    public final void partialTxn(PartialTxn txn)
    {
        PartialTxn current = partialTxn();
        if (current == null) setPartialTxn(txn);
        else if (!current.equals(txn)) throw new AssertionError();
    }

    private KeyRanges executeRanges(Timestamp executeAt)
    {
        return commandStore().ranges().since(executeAt.epoch);
    }

    enum EnsureAction { Ignore, Check, Add, TrySet, Set }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private boolean validate(KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard, AbstractRoute route,
                             @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                             @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        if (shard == Unsure)
            return false;

        // first validate route
        if (shard.isProgress())
        {
            // validate route
            if (shard.isHome())
            {
                if (!(route() instanceof Route) && !(route instanceof Route))
                    return false;
            }
            else if (route() == null)
            {
                // failing any of these tests is always an illegal state
                if (!route.covers(existingRanges))
                    return false;

                if (existingRanges != additionalRanges && !route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else if (existingRanges != additionalRanges && !route().covers(additionalRanges))
            {
                if (!route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else
            {
                if (!route().covers(existingRanges))
                    throw new IllegalStateException();
            }
        }

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Preconditions.checkState(status() != Accepted && status() != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(partialTxn()), covers(partialTxn), "txn", partialTxn))
            return false;

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(partialTxn()) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(partialDeps()), covers(partialDeps), "deps", partialDeps);
    }

    private void set(KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard, AbstractRoute route,
                     @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                     @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Preconditions.checkState(progressKey() != null);
        KeyRanges allRanges = existingRanges.union(additionalRanges);

        if (shard.isProgress()) setRoute(AbstractRoute.merge(route(), route));
        else setRoute(AbstractRoute.merge(route(), route.slice(allRanges)));

        // TODO (soon): stop round-robin hashing; partition only on ranges
        switch (ensurePartialTxn)
        {
            case Add:
                if (partialTxn == null)
                    break;

                if (partialTxn() != null)
                {
                    partialTxn = partialTxn.slice(allRanges, shard.isHome());
                    partialTxn.keys().foldlDifference(partialTxn().keys(), (i, key, p, v) -> {
                        if (commandStore().hashIntersects(key))
                            commandStore().commandsForKey(key).register(this);
                        return v;
                    }, 0, 0, 1);
                    this.setPartialTxn(partialTxn().with(partialTxn));
                    break;
                }

            case Set:
            case TrySet:
                setPartialTxn(partialTxn = partialTxn.slice(allRanges, shard.isHome()));
                partialTxn.keys().forEach(key -> {
                    if (commandStore().hashIntersects(key))
                        commandStore().commandsForKey(key).register(this);
                });
                break;
        }

        switch (ensurePartialDeps)
        {
            case Add:
                if (partialDeps == null)
                    break;

                if (partialDeps() != null)
                {
                    setPartialDeps(partialDeps().with(partialDeps.slice(allRanges)));
                    break;
                }

            case Set:
            case TrySet:
                setPartialDeps(partialDeps.slice(allRanges));
                break;
        }
    }

    private static boolean validate(EnsureAction action, KeyRanges existingRanges, KeyRanges additionalRanges,
                                    KeyRanges existing, KeyRanges adding, String kind, Object obj)
    {
        switch (action)
        {
            default: throw new IllegalStateException();
            case Ignore:
                break;

            case TrySet:
                if (adding != null)
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                        return false;

                    break;
                }
            case Set:
                // failing any of these tests is always an illegal state
                Preconditions.checkState(adding != null);
                if (!adding.contains(existingRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + existingRanges);

                if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + additionalRanges);
                break;

            case Check:
                if (existing == null)
                    return false;

            case Add:
                if (adding == null)
                {
                    if (existing == null)
                        return false;

                    Preconditions.checkState(existing.contains(existingRanges));
                    if (existingRanges != additionalRanges && !existing.contains(additionalRanges))
                        throw new IllegalArgumentException("Missing additional " + kind + "; existing does not cover " + additionalRanges.difference(existingRanges));
                }
                else if (existing != null)
                {
                    KeyRanges covering = adding.union(existing);
                    Preconditions.checkState(covering.contains(existingRanges));
                    if (existingRanges != additionalRanges && !covering.contains(additionalRanges))
                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                }
                else
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (existingRanges != additionalRanges && !adding.contains(additionalRanges))
                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                }
                break;
        }

        return true;
    }

    // TODO: callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public AbstractRoute someRoute()
    {
        Preconditions.checkState(homeKey() != null);
        if (route() != null)
            return route();

        if (partialTxn() != null)
            return partialTxn().keys().toRoute(homeKey());

        if (partialDeps() != null)
            return partialDeps().keys().toRoute(homeKey());

        return null;
    }

    public RoutingKeys someRoutingKeys()
    {
        if (route() != null)
            return route() instanceof Route ? route() : route().with(route().homeKey);

        if (partialTxn() != null && !partialTxn().keys().isEmpty())
            return partialTxn().keys().toRoutingKeys();

        if (partialDeps() != null && !partialDeps().keys().isEmpty())
            return partialDeps().keys().toRoutingKeys();

        return null;
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public boolean owns(long epoch, RoutingKey someKey)
    {
        if (!commandStore().hashIntersects(someKey))
            return false;

        return commandStore().ranges().at(epoch).contains(someKey);
    }

    private Command directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (isWaitingOnCommit())
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = firstWaitingOnCommit();
            if (!waitingOn.hasBeen(Committed))
                return waitingOn;

            if (updatePredecessor(waitingOn, false))
                maybeExecute(progressShard(), false);
        }

        while (isWaitingOnApply())
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = firstWaitingOnApply();
            if (!waitingOn.hasBeen(Applied))
                return waitingOn;

            if (updatePredecessor(waitingOn, false))
                maybeExecute(progressShard(), false);
        }

        return null;
    }

    @Override
    public void accept(Listener listener)
    {
        listener.onChange(this);
    }

    @Override
    public String toString()
    {
        return "Command{" +
               "txnId=" + txnId() +
               ", status=" + status() +
               ", partialTxn=" + partialTxn() +
               ", executeAt=" + executeAt() +
               ", partialDeps=" + partialDeps() +
               '}';
    }

    private static KeyRanges covers(@Nullable PartialTxn txn)
    {
        return txn == null ? null : txn.covering();
    }

    private static KeyRanges covers(@Nullable PartialDeps deps)
    {
        return deps == null ? null : deps.covering;
    }

    private static boolean hasQuery(PartialTxn txn)
    {
        return txn != null && txn.query() != null;
    }

    // TODO: this is an ugly hack, need to encode progress/homeKey/Route state combinations much more clearly
    //  (perhaps introduce encapsulating class representing each possible arrangement)
    private static final RoutingKey NO_PROGRESS_KEY = new RoutingKey()
    {
        @Override
        public int routingHash()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(@Nonnull RoutingKey ignore)
        {
            throw new UnsupportedOperationException();
        }
    };
}
