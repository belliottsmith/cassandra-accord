package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.primitives.KeyRanges;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

import static accord.local.Command.Outcome.INCOMPLETE;
import static accord.local.Command.Outcome.REDUNDANT;
import static accord.local.Command.Outcome.REJECTED_BALLOT;
import static accord.local.Command.Outcome.SUCCESS;
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
    public enum Outcome
    {
        SUCCESS, REDUNDANT, INCOMPLETE, REJECTED_BALLOT
    }

    public final CommandStore commandStore;
    private final TxnId txnId;
    private RoutingKey homeKey, progressKey;
    private @Nullable RoutingKeys routingKeys; // usually set only on home shard
    private PartialTxn partialTxn; // WARNING: if ownership *expands* for execution, this may be incomplete
    private PartialDeps partialDeps = PartialDeps.NONE;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;
    private boolean isGloballyPersistent; // only set on home shard

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

    public RoutingKeys routingKeys()
    {
        return routingKeys;
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

    public Outcome preaccept(PartialTxn partialTxn, RoutingKey homeKey, @Nullable RoutingKey progressKey, @Nullable RoutingKeys routingKeys)
    {
        if (promised.compareTo(Ballot.ZERO) > 0)
            return Outcome.REJECTED_BALLOT;

        if (hasBeen(PreAccepted))
            return REDUNDANT;

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(homeKey);
        if (isHomeShard)
        {
            if (routingKeys == null)
                throw new IllegalArgumentException("routingKeys must be provided on preaccept to the home shard");
            this.routingKeys = routingKeys;
        }

        KeyRanges ranges = commandStore.ranges().at(txnId.epoch);
        if (!partialTxn.covers(ranges))
        {
            commandStore.progressLog().unwitnessed(txnId, isProgressShard, isHomeShard);
            throw new IllegalArgumentException("Incomplete partialTxn received for " + txnId + " covering " + partialTxn.covering + " which does not span " + ranges);
        }

        this.homeKey = homeKey;
        this.progressKey = progressKey;
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
        return SUCCESS;
    }

    public boolean preacceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        this.promised = ballot;
        return true;
    }

    public Outcome accept(Ballot ballot, RoutingKey homeKey, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised.compareTo(ballot) > 0)
            return Outcome.REJECTED_BALLOT;

        if (hasBeen(Committed))
            return REDUNDANT;

        KeyRanges ranges = commandStore.ranges().at(txnId.epoch);
        if (!partialDeps.covers(ranges))
            throw new IllegalArgumentException("Incomplete partialDeps received for " + txnId + " covering " + partialDeps.covering + " which does not span " + ranges);

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(homeKey);

        updatePartialTxn(partialTxn, ranges, isHomeShard);
        updateHomeKey(homeKey);
        this.partialDeps = partialDeps.slice(ranges);
        this.executeAt = executeAt;
        this.promised = this.accepted = ballot;
        this.status = Accepted;

        this.commandStore.progressLog().accept(txnId, isProgressShard, isHomeShard);
        this.listeners.forEach(this);

        return SUCCESS;
    }

    public Outcome acceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return REJECTED_BALLOT;

        if (hasBeen(Committed))
            return REDUNDANT;

        promised = accepted = ballot;
        status = AcceptedInvalidate;

        listeners.forEach(this);
        return SUCCESS;
    }

    // relies on mutual exclusion for each key
    public Outcome commit(RoutingKey homeKey, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps, @Nullable PartialTxn partialTxn)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(this.executeAt) && status != Invalidated)
                return REDUNDANT;

            commandStore.agent().onInconsistentTimestamp(this, (status == Invalidated ? Timestamp.NONE : this.executeAt), executeAt);
        }

        // TODO (now): we want ideally to only require executeAt.epoch info here, but this might affect a CheckStatusOk response
        //             that expects anything >= PreAccept to know the PreAccept txn and deps
        KeyRanges ranges = commandStore.ranges().between(txnId.epoch, executeAt.epoch);
        if (!willCover(partialTxn, ranges))
            return INCOMPLETE;

        if (!partialDeps.covers(ranges))
            throw new IllegalArgumentException("Incomplete partialDeps received for " + txnId + " covering " + partialDeps.covering + " which does not span " + ranges);

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(homeKey);

        updateHomeKey(homeKey);
        updatePartialTxn(partialTxn, ranges, isHomeShard);
        this.partialDeps = partialDeps.slice(ranges);
        this.executeAt = executeAt;
        this.status = Committed;
        populateWaitingOn();

        commandStore.progressLog().commit(txnId, isProgressShard, isHomeShard);

        maybeExecute(false);
        listeners.forEach(this);
        return SUCCESS;
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

    public Outcome commitInvalidate()
    {
        if (hasBeen(Committed))
        {
            if (!hasBeen(Invalidated))
                commandStore.agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt);

            return REDUNDANT;
        }

        status = Invalidated;
        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        commandStore.progressLog().invalidate(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        listeners.forEach(this);
        return SUCCESS;
    }

    public Outcome apply(RoutingKey homeKey, RoutingKey progressKey, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt))
        {
            return REDUNDANT;
        }
        else if (!hasBeen(Committed))
        {
            if (partialDeps == null)
                return INCOMPLETE;

            if (owns(txnId.epoch, homeKey) && routingKeys == null)
                return INCOMPLETE;
        }
        else if (!executeAt.equals(this.executeAt))
        {
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        }

        KeyRanges ranges = commandStore.ranges().between(txnId.epoch, executeAt.epoch);
        if (this.partialDeps == null || !this.partialDeps.covers(ranges))
        {
            if (this.partialDeps == null && partialDeps == null)
                throw new IllegalStateException("Invalid status with missing partialDeps: " + status);

            if (partialDeps == null)
                return INCOMPLETE;

            if (!partialDeps.covers(ranges))
                throw new IllegalArgumentException("Incomplete partialDeps received for " + txnId + " covering " + this.partialDeps.covering + " which does not span " + ranges);

            this.partialDeps = partialDeps.slice(ranges);
        }

        updateHomeKey(homeKey);
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.status = Executed;
        populateWaitingOn();

        boolean isProgressShard = progressKey != null && owns(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(homeKey);
        commandStore.progressLog().execute(txnId, isProgressShard, isHomeShard);

        maybeExecute(false);
        this.listeners.forEach(this);
        return SUCCESS;
    }

    public Outcome recover(PartialTxn txn, RoutingKey homeKey, @Nullable RoutingKey progressKey, @Nullable RoutingKeys routingKeys, Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return REJECTED_BALLOT;

        if (status == NotWitnessed)
        {
            Outcome outcome = preaccept(txn, homeKey, progressKey, routingKeys);
            if (outcome != SUCCESS)
                return outcome;
        }

        this.promised = ballot;
        return SUCCESS;
    }

    public Outcome propagate(long epoch, Status status, PartialTxn partialTxn, PartialDeps partialDeps, RoutingKey homeKey, Timestamp executeAt, Writes writes, Result result)
    {
        switch (status)
        {
            case Invalidated:
                this.status = Invalidated;
                return SUCCESS;
            case AcceptedInvalidate:
                if (partialTxn != null)
                    break;
                if (homeKey != null)
                    updateHomeKey(homeKey);
                return SUCCESS;
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
                commandStore.progressLog().waiting(blockedBy.txnId, blockedBy.someRoutingKeys);
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

    // TEMPORARY: once we can invalidate commands that have not been witnessed on any shard, we do not need to know the home shard
    static class BlockedBy
    {
        final TxnId txnId;
        final RoutingKeys someRoutingKeys;

        BlockedBy(TxnId txnId, RoutingKeys someRoutingKeys)
        {
            this.txnId = txnId;
            this.someRoutingKeys = someRoutingKeys;
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

        RoutingKeys someRoute = cur.someRoutingKeys();
        if (someRoute == null)
            someRoute = prev.partialDeps.someRoutingKeys(cur.txnId);
        return new BlockedBy(cur.txnId, someRoute);
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

    public Route route()
    {
        if (routingKeys == null)
            return null;

        Preconditions.checkNotNull(homeKey);
        return routingKeys.toRoute(homeKey);
    }

    public RoutingKeys someRoutingKeys()
    {
        if (routingKeys != null)
            return routingKeys;

        if (partialTxn != null)
            return partialTxn.keys;

        if (partialDeps != null)
            return partialDeps.keys();

        return null;
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

        // TODO (now): slice/trim the partialTxn
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
