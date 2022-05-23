package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.Invalidated;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

// TODO: this needs to be backed by persistent storage
public class Command implements Listener, Consumer<Listener>
{
    public final CommandStore commandStore;
    private final TxnId txnId;

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    private RoutingKey homeKey, progressKey;

    /**
     * If this is the home shard, we require that this is a Route for all states > NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     */
    private @Nullable AbstractRoute route;

    /**
     * While !hasBeen(Committed), contains the portion of a transaction that applies on this node as of txnId.epoch
     * If hasBeen(Committed) and !hasBeen(Executed), represents the portion that applies as of txnId.epoch AND executeAt.epoch
     * If hasBeen(Executed) MAY BE NULL, or may cover only txnId.epoch; no point requiring this be populated at execution
     */
    private PartialTxn partialTxn;

    /**
     * While !hasBeen(Committed), used only as a register for Accept state, used by Recovery
     * If hasBeen(Committed), represents the full deps owned by this range for execution at both txnId.epoch
     * AND executeAt.epoch so that it may be used for Recovery (which contacts only txnId.epoch topology),
     * but also for execution.
     *
     * At present, Deps contain every declare key in the range, however this should not be relied upon
     * TODO: routeForKeysOwnedAtExecution relies on this for correctness.
     */
    private PartialDeps partialDeps = PartialDeps.NONE;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;
    private boolean isGloballyPersistent;

    private NavigableMap<TxnId, Command> waitingOnCommit;
    private NavigableMap<Timestamp, Command> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public Command(CommandStore commandStore, TxnId id)
    {
        this.commandStore = commandStore;
        this.txnId = id;
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public PartialTxn partialTxn()
    {
        return partialTxn;
    }

    public Route fullRoute()
    {
        Preconditions.checkNotNull(homeKey);
        Preconditions.checkState(owns(txnId.epoch, homeKey));
        Preconditions.checkState(route instanceof Route);
        return (Route) route;
    }

    public AbstractRoute route()
    {
        return route;
    }

    public void saveRoute(Route route)
    {
        this.route = route;
    }

    public Ballot promised()
    {
        return promised;
    }

    public Ballot accepted()
    {
        return accepted;
    }

    public Timestamp executeAt()
    {
        return executeAt;
    }

    public PartialDeps savedPartialDeps()
    {
        return partialDeps;
    }

    public Writes writes()
    {
        return writes;
    }

    public Result result()
    {
        return result;
    }

    public Status status()
    {
        return status;
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public boolean is(Status status)
    {
        return this.status == status;
    }

    public boolean isGloballyPersistent()
    {
        return isGloballyPersistent;
    }

    public void setGloballyPersistent(RoutingKey homeKey, Timestamp executeAt)
    {
        updateHomeKey(homeKey);
        if (!hasBeen(Committed))
            this.executeAt = executeAt;
        else if (!this.executeAt.equals(executeAt))
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        isGloballyPersistent = true;
    }

    public enum AcceptOutcome
    {
        Success, Insufficient, Redundant, RejectedBallot
    }

    public AcceptOutcome preaccept(PartialTxn partialTxn, PartialRoute partialRoute, @Nullable RoutingKey progressKey, @Nullable Route route)
    {
        if (promised.compareTo(Ballot.ZERO) > 0)
            return AcceptOutcome.RejectedBallot;

        if (hasBeen(PreAccepted))
            return AcceptOutcome.Redundant;

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(partialRoute.homeKey);
        if (isHomeShard)
        {
            if (route == null)
                throw new IllegalArgumentException("routingKeys must be provided on preaccept to the home shard");
            this.route = route;
        }
        else if (isProgressShard)
        {
            this.route = partialRoute;
        }

        KeyRanges ranges = commandStore.ranges().at(txnId.epoch);
        if (!partialTxn.covers(ranges))
        {
            commandStore.progressLog().unwitnessed(txnId, isProgressShard, isHomeShard);
            throw new IllegalArgumentException("Incomplete partialTxn received for " + txnId + " covering " + partialTxn.covering + " which does not span " + ranges);
        }

        updateRoute(partialRoute);
        updateProgressKey(progressKey);
        updatePartialTxn(partialTxn, ranges, isHomeShard);

        Timestamp max = commandStore.maxConflict(partialTxn.keys);
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        this.executeAt = txnId.compareTo(max) > 0 && txnId.epoch >= commandStore.latestEpoch()
                         ? txnId : commandStore.uniqueNow(max);

        this.status = PreAccepted;
        this.commandStore.progressLog().preaccept(txnId, isProgressShard, isHomeShard);
        this.listeners.forEach(this);
        return AcceptOutcome.Success;
    }

    public boolean preacceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        this.promised = ballot;
        return true;
    }

    public AcceptOutcome accept(Ballot ballot, PartialRoute partialRoute, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised.compareTo(ballot) > 0)
            return AcceptOutcome.RejectedBallot;

        if (hasBeen(Committed))
            return AcceptOutcome.Redundant;

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(partialRoute.homeKey);

        if (isHomeShard && !(route instanceof Route))
            return AcceptOutcome.Insufficient;

        KeyRanges ranges = commandStore.ranges().at(txnId.epoch);
        if (!partialDeps.covers(ranges))
            throw new IllegalArgumentException("Incomplete partialDeps received for " + txnId + " covering " + partialDeps.covering + " which does not span " + ranges);

        maybeUpdateRoute(partialRoute);
        updateProgressKey(progressKey);
        updatePartialTxn(partialTxn, ranges, isHomeShard);
        setPartialDeps(partialDeps, ranges);
        this.executeAt = executeAt;
        this.promised = this.accepted = ballot;
        this.status = Accepted;

        this.commandStore.progressLog().accept(txnId, isProgressShard, isHomeShard);
        this.listeners.forEach(this);

        return AcceptOutcome.Success;
    }

    public AcceptOutcome acceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return AcceptOutcome.RejectedBallot;

        if (hasBeen(Committed))
            return AcceptOutcome.Redundant;

        promised = accepted = ballot;
        status = AcceptedInvalidate;

        listeners.forEach(this);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Redundant, Insufficient }

    // relies on mutual exclusion for each key
    public CommitOutcome commit(PartialRoute partialRoute, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps, @Nullable PartialTxn partialTxn)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(this.executeAt) && status != Invalidated)
                return CommitOutcome.Redundant;

            commandStore.agent().onInconsistentTimestamp(this, (status == Invalidated ? Timestamp.NONE : this.executeAt), executeAt);
        }

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(partialRoute.homeKey);

        // TODO: we want ideally to only require executeAt.epoch info here, but this might affect a CheckStatusOk response
        //       that expects anything >= PreAccept to know the PreAccept txn and deps
        KeyRanges ranges = commandStore.ranges().between(txnId.epoch, executeAt.epoch);
        if (!willCover(partialTxn, ranges))
            return CommitOutcome.Insufficient;

        if (isHomeShard && !(route instanceof Route))
            return CommitOutcome.Insufficient;

        if (!partialDeps.covers(ranges))
            throw new IllegalArgumentException("Incomplete partialDeps received for " + txnId + " covering " + partialDeps.covering + " which does not span " + ranges);

        updateRoute(partialRoute);
        updateProgressKey(progressKey);
        updatePartialTxn(partialTxn, ranges, isHomeShard);
        setPartialDeps(partialDeps, ranges);
        this.executeAt = executeAt;
        this.status = Committed;
        populateWaitingOn();

        commandStore.progressLog().commit(txnId, isProgressShard, isHomeShard);

        maybeExecute(false);
        listeners.forEach(this);
        return CommitOutcome.Success;
    }

    private void populateWaitingOn()
    {
        this.waitingOnCommit = new TreeMap<>();
        this.waitingOnApply = new TreeMap<>();
        partialDeps.forEachOn(commandStore, executeAt, txnId -> {
            Command command = commandStore.command(txnId);
            switch (command.status)
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    // we don't know when these dependencies will execute, and cannot execute until we do
                    command.addListener(this);
                    waitingOnCommit.put(txnId, command);
                    break;
                case Committed:
                    // TODO: split into ReadyToRead and ReadyToWrite;
                    //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                    //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                case ReadyToExecute:
                case Executed:
                case Applied:
                    command.addListener(this);
                    updatePredecessor(command);
                case Invalidated:
                    break;
            }
        });

        if (waitingOnCommit.isEmpty())
        {
            waitingOnCommit = null;
            if (waitingOnApply.isEmpty())
                waitingOnApply = null;
        }
    }

    public void commitInvalidate()
    {
        if (hasBeen(Committed))
        {
            if (!hasBeen(Invalidated))
                commandStore.agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt);

            return;
        }

        status = Invalidated;
        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        commandStore.progressLog().invalidate(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        listeners.forEach(this);
    }

    public enum ApplyOutcome { Success, Redundant, Insufficient, Partial }

    public ApplyOutcome apply(PartialRoute partialRoute, RoutingKey progressKey, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt))
        {
            return ApplyOutcome.Redundant;
        }
        else if (hasBeen(Committed) && !executeAt.equals(this.executeAt))
        {
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        }

        KeyRanges ranges = commandStore.ranges().between(txnId.epoch, executeAt.epoch);
        if (this.partialDeps == null || !this.partialDeps.covers(ranges))
        {
            if (partialDeps == null)
                return ApplyOutcome.Insufficient;

            updatePartialDeps(partialDeps, ranges);
            if (!partialDeps.covers(ranges))
            {
                // TODO: partial application of writes/result
                this.writes = writes;
                this.result = result;
                return ApplyOutcome.Partial;
            }
        }

        if (!hasBeen(Committed) && !(route instanceof Route) && owns(txnId.epoch, partialRoute.homeKey))
            return ApplyOutcome.Insufficient;

        updateRoute(partialRoute);
        updateProgressKey(progressKey);
        this.writes = writes;
        this.result = result;
        this.executeAt = executeAt;
        this.status = Executed;
        populateWaitingOn();

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(homeKey);
        commandStore.progressLog().execute(txnId, isProgressShard, isHomeShard);

        maybeExecute(false);
        this.listeners.forEach(this);
        return ApplyOutcome.Success;
    }

    public AcceptOutcome recover(PartialTxn txn, PartialRoute partialRoute, @Nullable RoutingKey progressKey, @Nullable Route route, Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return AcceptOutcome.RejectedBallot;

        if (status == NotWitnessed)
        {
            switch (preaccept(txn, partialRoute, progressKey, route))
            {
                default:
                case RejectedBallot:
                case Insufficient:
                    throw new IllegalStateException();

                case Redundant:
                case Success:
            }
        }

        this.promised = ballot;
        return AcceptOutcome.Success;
    }

    public void propagate(long epoch, Status status, PartialTxn partialTxn, PartialDeps partialDeps, RoutingKey homeKey, RoutingKey progressKey, Timestamp executeAt, Writes writes, Result result)
    {
        switch (status)
        {
            case AcceptedInvalidate:
                if (partialTxn == null)
                    break;
            case PreAccepted:
            case Accepted:
                commandStore.command(txnId).preaccept(partialTxn, homeKey, progressKey, );
                break;
            case Committed:
            case ReadyToExecute:
                commandStore.command(txnId).commit(partialTxn, homeKey, progressKey, executeAt, partialDeps);
                break;
            case Executed:
            case Applied:
                commandStore.command(txnId).apply(partialTxn, homeKey, progressKey, executeAt, partialDeps, writes, result);
                break;
            case Invalidated:
                commandStore.command(txnId).commitInvalidate();
            default:
                throw new IllegalStateException();
        }
        switch (status)
        {
            case Invalidated:
                this.status = Invalidated;
                return;
            case AcceptedInvalidate:
                if (partialTxn != null)
                    break;
                if (homeKey != null)
                    updateHomeKey(homeKey);
                return;
        }

        Status prev = this.status;
        try
        {
            KeyRanges ranges = commandStore.ranges().at(epoch);
            if (!hasBeen(PreAccepted))
            {
                // TODO: think about how this might work in a world where we have pre-accepted, but we adopt more ranges
                updateHomeKey(homeKey);
                updatePartialTxn(partialTxn, ranges, commandStore.ranges().owns(epoch, homeKey));

                if (!this.partialTxn.covers(ranges))
                    return INCOMPLETE;

                this.executeAt = executeAt;
                this.status = PreAccepted;
            }

            // Accept is a no-op for propagation purposes, as the accept was for the prior epoch
            if (status.compareTo(AcceptedInvalidate) <= 0)
                return SUCCESS;

            if (!hasBeen(Committed))
            {
                if (this.partialDeps == null) this.partialDeps = partialDeps;
                else if (partialDeps != null) this.partialDeps = this.partialDeps.with(partialDeps.slice(ranges));

                // TODO (now): this is probably broken, as we may have received pre-commit partialDeps
                if (!this.partialDeps.covers(ranges))
                    return INCOMPLETE;

                this.executeAt = executeAt;
                this.status = Committed;

                populateWaitingOn();
                maybeExecute(false);
            }
            else if (this.executeAt != null && !this.executeAt.equals(executeAt))
            {
                commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
            }

            if (status.compareTo(ReadyToExecute) <= 0)
                return SUCCESS;

            if (!hasBeen(Executed))
            {
                this.writes = writes;
                this.result = result;

                maybeExecute(false);
            }

            return SUCCESS;
        }
        finally
        {
            if (prev != this.status)
                this.listeners.forEach(this);
        }
    }

    public boolean addListener(Listener listener)
    {
        return listeners.add(listener);
    }

    public void removeListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void onChange(Command command)
    {
        switch (command.status)
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
                if (waitingOnApply != null)
                {
                    updatePredecessor(command);
                    if (waitingOnCommit != null)
                    {
                        if (waitingOnCommit.remove(command.txnId) != null && waitingOnCommit.isEmpty())
                            waitingOnCommit = null;
                    }
                    if (waitingOnCommit == null && waitingOnApply.isEmpty())
                        waitingOnApply = null;
                }
                else
                {
                    command.removeListener(this);
                }
                maybeExecute(true);
                break;
        }
    }

    private void maybeExecute(boolean notifyListeners)
    {
        if (status != Committed && status != Executed)
            return;

        if (waitingOnApply != null)
        {
            BlockedBy blockedBy = blockedBy();
            if (blockedBy != null)
            {
                commandStore.progressLog().waiting(blockedBy.txnId, blockedBy.someKeys);
                return;
            }
            assert waitingOnApply == null;
        }

        switch (status)
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status = ReadyToExecute;
                boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
                commandStore.progressLog().readyToExecute(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));
                if (notifyListeners)
                    listeners.forEach(this);
                break;
            case Executed:
                writes.apply(commandStore);
                status = Applied;
                if (notifyListeners)
                    listeners.forEach(this);
        }
    }

    /**
     * @param dependency is either committed or invalidated
     */
    private void updatePredecessor(Command dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            dependency.removeListener(this);
            if (waitingOnCommit.remove(dependency.txnId) != null && waitingOnCommit.isEmpty())
                waitingOnCommit = null;
        }
        else if (dependency.executeAt.compareTo(executeAt) > 0)
        {
            // cannot be a predecessor if we execute later
            dependency.removeListener(this);
        }
        else if (dependency.hasBeen(Applied))
        {
            waitingOnApply.remove(dependency.executeAt);
            dependency.removeListener(this);
        }
        else
        {
            waitingOnApply.putIfAbsent(dependency.executeAt, dependency);
        }
    }

    static class BlockedBy
    {
        final TxnId txnId;
        final RoutingKeys someKeys; // some keys we know to be associated with this txnId, unlikely to be exhaustive

        BlockedBy(TxnId txnId, RoutingKeys someKeys)
        {
            this.txnId = txnId;
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
            someKeys = prev.partialDeps.someRoutingKeys(cur.txnId);
        return new BlockedBy(cur.txnId, someKeys);
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
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    public void updateHomeKey(RoutingKey homeKey)
    {
        if (this.homeKey == null) this.homeKey = homeKey;
        else if (!this.homeKey.equals(homeKey)) throw new AssertionError();
    }

    public void updateProgressKey(RoutingKey progressKey)
    {
        if (progressKey == null)
            return;

        if (this.progressKey == null) this.progressKey = progressKey;
        else if (!this.progressKey.equals(progressKey)) throw new AssertionError();
    }

    public void maybeUpdateRoute(PartialRoute partialRoute)
    {
        updateHomeKey(partialRoute.homeKey);
        if (route == null)
            route = partialRoute;
    }

    public void updateRoute(PartialRoute partialRoute)
    {
        updateHomeKey(partialRoute.homeKey);
        this.route = partialRoute;
    }

    public RoutingKeys someRoutingKeys()
    {
        if (route != null)
            return route;

        if (partialTxn != null)
            return partialTxn.keys.toRoutingKeys();

        if (partialDeps != null)
            return partialDeps.keys().toRoutingKeys();

        return null;
    }

    public PartialRoute routeForKeysOwnedAtExecution()
    {
        if (!hasBeen(Committed))
            throw new IllegalStateException("May only be invoked on commands whose execution time is known");

        if (homeKey == null)
            throw new IllegalStateException();

        KeyRanges ranges = commandStore.ranges().at(executeAt.epoch);
        return route.sliceStrict(ranges);

        if (partialTxn != null && partialTxn.covers(ranges))
            return partialTxn.keys.toPartialRoute(partialTxn.covering, homeKey);

        if (partialDeps == null || !partialDeps.covers(ranges))
            throw new IllegalStateException();

        // TODO: we should not rely on partialDeps containing all keys, we should have a separate stash of routingKeys
        //       used only when execution does not receive the txn
        return partialDeps.keys().toPartialRoute(partialDeps.covering, homeKey);
    }

    /**
     * A key nominated to be the primary shard within this node for managing progress of the command.
     * It is nominated only as of txnId.epoch, and may be null (indicating that this node does not monitor
     * the progress of this command).
     *
     * Preferentially, this is homeKey on nodes that replicate it, and otherwise any key that is replicated, as of txnId.epoch
     */
    public RoutingKey progressKey()
    {
        return progressKey;
    }

    public void progressKey(RoutingKey progressKey)
    {
        if (this.progressKey == null) this.progressKey = progressKey;
        else if (!this.progressKey.equals(progressKey)) throw new AssertionError();
    }

    public void updatePartialTxn(PartialTxn partialTxn, KeyRanges ranges, boolean isHomeShard)
    {
        if (partialTxn == null)
            return;

        // TODO (now): slice needs to apply hashIntersects restriction
        if (this.partialTxn == null)
        {
            this.partialTxn = partialTxn.slice(ranges, isHomeShard);
            partialTxn.keys().forEach(key -> {
                if (commandStore.hashIntersects(key))
                    commandStore.commandsForKey(key).register(this);
            });
        }
        else if (!this.partialTxn.keys.containsAll(partialTxn.keys))
        {
            partialTxn.keys.foldlDifference(this.partialTxn.keys, (i, key, p, v) -> {
                if (commandStore.hashIntersects(key))
                    commandStore.commandsForKey(key).register(this);
                return v;
            }, 0, 0, 1);
            this.partialTxn = this.partialTxn.with(partialTxn.slice(ranges, isHomeShard));
        }
    }

    private void setPartialDeps(PartialDeps partialDeps, KeyRanges ranges)
    {
        if (partialDeps == null)
            return;

        // TODO (now): slice needs to apply hashIntersects restriction
        this.partialDeps = partialDeps.slice(ranges);
    }

    private void updatePartialDeps(PartialDeps partialDeps, KeyRanges ranges)
    {
        if (partialDeps == null)
            return;

        // TODO (now): slice needs to apply hashIntersects restriction
        if (this.partialDeps == null) this.partialDeps = partialDeps.slice(ranges);
        else this.partialDeps = this.partialDeps.with(partialDeps.slice(ranges));
    }

    private boolean willCover(PartialTxn partialTxn, KeyRanges ranges)
    {
        if (this.partialTxn == null && partialTxn == null) return ranges.isEmpty();
        else if (this.partialTxn == null) return partialTxn.covers(ranges);
        else if (partialTxn == null) return this.partialTxn.covers(ranges);
        else if (this.partialTxn.covers(ranges)) return true;
        else if (partialTxn.covers(ranges)) return true;
        else return partialTxn.covers(ranges.difference(this.partialTxn.covering));
    }

    // does this specific Command instance execute (i.e. does it lose ownership post Commit)
    public boolean executes()
    {
        KeyRanges ranges = commandStore.ranges().at(executeAt.epoch);
        // TODO: take care when merging commandStores, as partialTxn may be incomplete
        return partialTxn.keys.any(ranges, commandStore::hashIntersects);
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public boolean owns(long epoch, RoutingKey someKey)
    {
        if (!commandStore.hashIntersects(someKey))
            return false;

        return commandStore.ranges().at(epoch).contains(someKey);
    }

    private Id coordinator()
    {
        if (promised.equals(Ballot.ZERO))
            return txnId.node;
        return promised.node;
    }

    private Command directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (waitingOnCommit != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnCommit.firstEntry().getValue();
            if (!waitingOn.hasBeen(Committed)) return waitingOn;
            onChange(waitingOn);
        }

        while (waitingOnApply != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnApply.firstEntry().getValue();
            if (!waitingOn.hasBeen(Applied)) return waitingOn;
            onChange(waitingOn);
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
               "txnId=" + txnId +
               ", status=" + status +
               ", partOfTxn=" + partialTxn +
               ", executeAt=" + executeAt +
               ", partialDeps=" + partialDeps +
               '}';
    }
}
