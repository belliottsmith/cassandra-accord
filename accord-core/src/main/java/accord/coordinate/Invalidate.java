package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Invalidate.Outcome;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateNack;
import accord.messages.BeginInvalidation.InvalidateOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.topology.Shard;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;
import static accord.messages.Commit.Invalidate.commitInvalidate;

public class Invalidate extends AsyncFuture<Outcome> implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    public enum Outcome { PREEMPTED, EXECUTED, INVALIDATED }

    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final PartialRoute someRoute;
    final RoutingKey someKey;

    final List<InvalidateOk> invalidateOks = new ArrayList<>();
    final QuorumShardTracker preacceptTracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, PartialRoute someRoute, RoutingKey someKey)
    {
        Preconditions.checkArgument(someRoute.contains(someKey));
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.someRoute = someRoute;
        this.someKey = someKey;
        this.preacceptTracker = new QuorumShardTracker(shard);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, PartialRoute someRoute, RoutingKey someKey)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someRoute, someKey);
        node.send(shard.nodes, to -> new BeginInvalidation(txnId, someKey, ballot), invalidate);
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
            if (nack.homeKey != null)
            {
                node.ifLocalSince(someKey, txnId, instance -> {
                    instance.command(txnId).updateHomeKey(nack.homeKey);
                    return null;
                });
            }
            trySuccess(Outcome.PREEMPTED);
            return;
        }

        InvalidateOk ok = (InvalidateOk) response;
        invalidateOks.add(ok);
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
                    // note: we do not propagate our responses to the Recover instance to avoid mistakes;
                    //       since invalidate contacts only one key, only responses from nodes that replicate
                    //       *only* that key for the transaction will be valid, as the shards on the replica
                    //       that own the other keys may not have responded. It would be possible to filter
                    //       replies now that we have the transaction, but safer to just start from scratch.
                    Recover.recover(node, ballot, txnId, acceptOrCommit.txn, findRoute())
                           .addCallback(this);
                    return;
                case AcceptedInvalidate:
                    break; // latest accept also invalidating, so we're on the same page and should finish our invalidation
                case Committed:
                case ReadyToExecute:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Execute.execute(node, txnId, acceptOrCommit.txn, findRoute(), acceptOrCommit.executeAt,
                                        acceptOrCommit.deps, this);
                    });
                    return;
                case Executed:
                case Applied:
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Persist.persistAndCommit(node, txnId, acceptOrCommit.homeKey, acceptOrCommit.txn, acceptOrCommit.executeAt,
                                        acceptOrCommit.deps, acceptOrCommit.writes, acceptOrCommit.result);
                        trySuccess(Outcome.EXECUTED);
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
            if (fail != null)
            {
                tryFailure(fail);
                return;
            }

            // TODO (now): error handling in other callbacks
            try
            {
                commitInvalidate(node, txnId, someRoute, txnId);
                node.forEachLocalSince(someRoute, txnId, instance -> {
                    instance.command(txnId).commitInvalidate();
                });
            }
            finally
            {
                trySuccess(Outcome.INVALIDATED);
            }
        });
    }

    private Route findRoute()
    {

    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone())
            return;

        if (preacceptTracker.failure(from))
            tryFailure(new Timeout(txnId, null));
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        tryFailure(failure);
    }

    @Override
    public void accept(Result result, Throwable fail)
    {
        if (fail != null)
        {
            if (fail instanceof Invalidated) trySuccess(Outcome.INVALIDATED);
            else tryFailure(fail);
        }
        else
        {
            node.agent().onRecover(node, result, fail);
            trySuccess(Outcome.EXECUTED);
        }
    }
}
