package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.Invalidate.Outcome;
import accord.coordinate.Invalidate.Outcome.Kind;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginInvalidate;
import accord.messages.BeginInvalidate.InvalidateNack;
import accord.messages.BeginInvalidate.InvalidateOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.topology.Shard;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;

class Invalidate extends AsyncFuture<Outcome> implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    public static class Outcome
    {
        static final Outcome EXECUTED = new Outcome(Kind.EXECUTED, null, null);
        static final Outcome INVALIDATED = new Outcome(Kind.INVALIDATED, null, null);

        public enum Kind { PREEMPTED, EXECUTED, INVALIDATED }

        final Kind kind;
        final Txn txn;
        final Key homeKey;

        public Outcome(Kind kind, Txn txn, Key homeKey)
        {
            this.kind = kind;
            this.txn = txn;
            this.homeKey = homeKey;
        }
    }

    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Key someKey;

    final List<Id> invalidateOksFrom = new ArrayList<>();
    final List<InvalidateOk> invalidateOks = new ArrayList<>();
    final QuorumShardTracker preacceptTracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, Key someKey)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.someKey = someKey;
        this.preacceptTracker = new QuorumShardTracker(shard);
    }

    public static Invalidate invalidate(Node node, Ballot ballot, TxnId txnId, Key someKey)
    {
        Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKey);
        node.send(shard.nodes, to -> new BeginInvalidate(txnId, someKey, ballot), invalidate);
        return invalidate;
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || preacceptTracker.hasReachedQuorum())
            return;

        if (!response.isOK())
        {
            InvalidateNack nack = (InvalidateNack) response;
            trySuccess(new Outcome(Kind.PREEMPTED, nack.txn, nack.homeKey));
            return;
        }

        InvalidateOk ok = (InvalidateOk) response;
        invalidateOks.add(ok);
        invalidateOksFrom.add(from);
        if (preacceptTracker.success(from))
            invalidate();
    }

    private void invalidate()
    {
        // first look to see if it has already been
        InvalidateOk acceptOrCommit = maxAcceptedOrLater(invalidateOks);
        if (acceptOrCommit != null)
        {
            switch (acceptOrCommit.status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                    throw new IllegalStateException("Should only have Accepted or later statuses here");
                case Accepted:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Recover recover = new Recover(node, ballot, txnId, acceptOrCommit.txn, acceptOrCommit.homeKey);
                        recover.addCallback((success, fail) -> {
                            if (fail != null) tryFailure(fail);
                            else Execute.execute(node, success)
                                        .addCallback(this);
                        });

                        Set<Id> nodes = recover.tracker.topologies().copyOfNodes();
                        for (int i = 0 ; i < invalidateOks.size() ; ++i)
                        {
                            recover.onSuccess(invalidateOksFrom.get(i), invalidateOks.get(i));
                            nodes.remove(invalidateOksFrom.get(i));
                        }
                        recover.start(nodes);
                    });
                    return;
                case AcceptedInvalidate:
                    break; // latest accept also invalidating, so we're on the same page and should finish our invalidation
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Execute.execute(node, new Agreed(txnId, acceptOrCommit.txn, acceptOrCommit.homeKey, acceptOrCommit.executeAt,
                                                         acceptOrCommit.deps, acceptOrCommit.writes, acceptOrCommit.result))
                               .addCallback(this);
                    });
                    return;
                case Invalidated:
                    trySuccess(Outcome.INVALIDATED);
                    return;
            }
        }

        // if we have witnessed the transaction, but are able to invalidate, do we want to proceed?
        // Probably simplest to do so, but perhaps better for user if we don't.
        proposeInvalidate(node, ballot, txnId, someKey).addCallback((success, fail) -> {
            if (fail != null) tryFailure(fail);
            else trySuccess(Outcome.INVALIDATED);
        });
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (isDone())
            return;

        if (preacceptTracker.failure(from))
            tryFailure(new Timeout());
    }

    @Override
    public void accept(Result result, Throwable fail)
    {
        if (fail != null) tryFailure(fail);
        else trySuccess(Outcome.EXECUTED);
    }
}
