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
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
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
    public enum Outcome
    {
        SUCCESS, REDUNDANT, REJECTED_BALLOT, INCOMPLETE
    }

    public final CommandStore commandStore;
    private final TxnId txnId;
    private RoutingKey homeKey, progressKey;
    private RoutingKeys routingKeys; // TODO (now): replicate only to home shard (one extra step necessary when collecting info from peers)
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
        homeKey(homeKey);
        if (!hasBeen(Committed))
            this.executeAt = executeAt;
        else if (!this.executeAt.equals(executeAt))
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        isGloballyPersistent = true;
    }

    // requires that command != null
    // relies on mutual exclusion for each key
    // note: we do not set status = newStatus, we only use it to decide how we register with the retryLog
    private boolean witness(PartialTxn partialTxn, RoutingKey homeKey, @Nullable RoutingKey progressKey)
    {
        partialTxn(partialTxn);
        homeKey(homeKey);
        progressKey(progressKey);

        if (executeAt != null)
            return true;

        if (!this.partialTxn.covers(commandStore.ranges().at(txnId.epoch)))
            return false;

        Timestamp max = commandStore.maxConflict(partialTxn.keys);
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        executeAt = txnId.compareTo(max) > 0 && txnId.epoch >= commandStore.latestEpoch()
                    ? txnId : commandStore.uniqueNow(max);

        return true;
    }

    public Outcome preaccept(PartialTxn partialTxn, RoutingKey homeKey, @Nullable RoutingKey progressKey)
    {
        if (promised.compareTo(Ballot.ZERO) > 0)
            return Outcome.REJECTED_BALLOT;

        if (hasBeen(PreAccepted))
            return Outcome.REDUNDANT;

        boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
        if (!witness(partialTxn, homeKey, progressKey))
        {
            commandStore.progressLog().unwitnessed(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));
            return Outcome.INCOMPLETE;
        }

        status = PreAccepted;
        commandStore.progressLog().preaccept(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));
        listeners.forEach(this);
        return Outcome.SUCCESS;
    }

    public Outcome accept(Ballot ballot, RoutingKey homeKey, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised.compareTo(ballot) > 0)
            return Outcome.REJECTED_BALLOT;

        if (hasBeen(Committed))
            return Outcome.REDUNDANT;

        if (!witness(null, homeKey, progressKey))
            return Outcome.INCOMPLETE;

        this.partialDeps = partialDeps;
        this.executeAt = executeAt;
        promised = accepted = ballot;
        status = Accepted;

        boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
        commandStore.progressLog().accept(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        listeners.forEach(this);
        return Outcome.SUCCESS;
    }

    public Outcome acceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return Outcome.REJECTED_BALLOT;

        if (hasBeen(Committed))
            return Outcome.REDUNDANT;

        promised = accepted = ballot;
        status = AcceptedInvalidate;

        listeners.forEach(this);
        return Outcome.SUCCESS;
    }

    // relies on mutual exclusion for each key
    public Outcome commit(@Nullable PartialTxn partialTxn, RoutingKey homeKey, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(this.executeAt) && status != Invalidated)
                return Outcome.REDUNDANT;

            commandStore.agent().onInconsistentTimestamp(this, (status == Invalidated ? Timestamp.NONE : this.executeAt), executeAt);
        }

        if (!witness(partialTxn, homeKey, progressKey))
            return Outcome.INCOMPLETE;

        KeyRanges executeRanges = commandStore.ranges().at(executeAt.epoch);
        if (!partialDeps.covers(executeRanges))
            return Outcome.INCOMPLETE;

        boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
        boolean isHomeShard = isProgressShard && progressKey.equals(homeKey);

        this.partialDeps = partialDeps;
        this.executeAt = executeAt;
        this.waitingOnCommit = new TreeMap<>();
        this.waitingOnApply = new TreeMap<>();

        this.status = Committed;
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

        commandStore.progressLog().commit(txnId, isProgressShard, isHomeShard);

        maybeExecute(false);
        listeners.forEach(this);
        return Outcome.SUCCESS;
    }

    public Outcome commitInvalidate()
    {
        if (hasBeen(Committed))
        {
            if (!hasBeen(Invalidated))
                commandStore.agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt);

            return Outcome.REDUNDANT;
        }

        status = Invalidated;

        boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
        commandStore.progressLog().invalidate(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        listeners.forEach(this);
        return Outcome.SUCCESS;
    }

    // TODO (now): stop sending deps with Apply; should seek them asynchronously if not yet known locally
    public boolean apply(RoutingKey homeKey, RoutingKey progressKey, Timestamp executeAt, PartialDeps deps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt))
            return false;
        else if (!hasBeen(Committed))
            commit(null, homeKey, progressKey, executeAt, deps);
        else if (!executeAt.equals(this.executeAt))
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);

        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.status = Executed;

        boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
        commandStore.progressLog().execute(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));

        maybeExecute(false);
        this.listeners.forEach(this);
        return true;
    }

    public boolean recover(PartialTxn txn, RoutingKey homeKey, RoutingKey progressKey, Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        witness(txn, homeKey, progressKey);
        boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
        this.promised = ballot;
        if (status == NotWitnessed)
        {
            status = PreAccepted;
            commandStore.progressLog().preaccept(txnId, isProgressShard, isProgressShard && progressKey.equals(homeKey));
            listeners.forEach(this);
        }
        return true;
    }

    public boolean preAcceptInvalidate(Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        this.promised = ballot;
        return true;
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
                boolean isProgressShard = progressKey != null && handles(txnId.epoch, progressKey);
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
        final Keys someKeys;

        BlockedBy(TxnId txnId, Keys someKeys)
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

        Keys someKeys = cur.someKeys();
        if (someKeys == null)
            someKeys = prev.partialDeps.someKeys(cur.txnId);
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

    public void homeKey(RoutingKey homeKey)
    {
        if (this.homeKey == null) this.homeKey = homeKey;
        else if (!this.homeKey.equals(homeKey)) throw new AssertionError();
    }

    public Keys someKeys()
    {
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

    // does this specific Command instance execute (i.e. does it lose ownership post Commit)
    public boolean executes()
    {
        KeyRanges ranges = commandStore.ranges().at(executeAt.epoch);
        // TODO: take care when merging commandStores, as partialTxn may be incomplete
        return ranges != null && partialTxn.keys.any(ranges, commandStore::hashIntersects);
    }

    public void partialTxn(PartialTxn partialTxn)
    {
        // TODO (now): slice/trim the partialTxn
        if (this.partialTxn == null)
        {
            this.partialTxn = partialTxn;
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
            this.partialTxn = this.partialTxn.with(partialTxn);
        }
    }

    public boolean handles(long epoch, RoutingKey someKey)
    {
        if (!commandStore.hashIntersects(someKey))
            return false;

        KeyRanges ranges = commandStore.ranges().at(epoch);
        if (ranges == null)
            return false;
        return ranges.contains(someKey);
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
