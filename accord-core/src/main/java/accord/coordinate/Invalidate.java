package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.coordinate.Recover.Outcome;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateNack;
import accord.messages.BeginInvalidation.InvalidateOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.topology.Shard;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.Commit.Invalidate.commitInvalidate;

// TODO (now): switch to callback like others
public class Invalidate extends AsyncFuture<Outcome> implements Callback<RecoverReply>, BiConsumer<Outcome, Throwable>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final RoutingKeys someKeys;
    final RoutingKey someKey;

    final List<InvalidateOk> invalidateOks = new ArrayList<>();
    final QuorumShardTracker preacceptTracker;

    private Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, RoutingKeys someKeys, RoutingKey someKey)
    {
        Preconditions.checkArgument(someKeys.contains(someKey));
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.someKeys = someKeys;
        this.someKey = someKey;
        this.preacceptTracker = new QuorumShardTracker(shard);
    }

    public static Invalidate invalidate(Node node, TxnId txnId, RoutingKeys someKeys, RoutingKey someKey)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
        Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKeys, someKey);
        node.send(shard.nodes, to -> new BeginInvalidation(txnId, someKey, ballot), invalidate);
        return invalidate;
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || preacceptTracker.hasReachedQuorum())
            return;

        if (!response.isOk())
        {
            InvalidateNack nack = (InvalidateNack) response;
            if (nack.homeKey != null)
            {
                node.ifLocalSince(someKey, txnId, instance -> {
                    instance.command(txnId).updateHomeKey(nack.homeKey);
                    return null;
                });
            }
            trySuccess(Outcome.Preempted);
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
        Status maxStatus = invalidateOks.stream().map(ok -> ok.status).max(Comparable::compareTo).orElseThrow();
        switch (maxStatus)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
            case PreAccepted:
                throw new IllegalStateException("Should only have Accepted or later statuses here");

            case Accepted:
            case Committed:
            case ReadyToExecute:
            case Executed:
            case Applied:
                Route route = InvalidateOk.findRoute(invalidateOks);
                RoutingKey homeKey = route == null ? InvalidateOk.findHomeKey(invalidateOks) : null;

                if (route != null)
                {
                    RecoverWithRoute.recover(node, ballot, txnId, route, this);
                }
                else if (homeKey != null && homeKey.equals(someKey))
                {
                    throw new IllegalStateException("Received a response from a node that must have known the route, but that did not include it");
                }
                else if (homeKey != null)
                {
                    RecoverWithHomeKey.recover(node, txnId, homeKey, this);
                }
                else
                {
                    throw new IllegalStateException("Received a response from a node that must have known the homeKey, but that did not include it");
                }
                return;

            case AcceptedInvalidate:
            {
                break; // latest accept also invalidating, so we're on the same page and should finish our invalidation
            }
            case Invalidated:
                node.forEachLocalSince(someKeys, txnId, instance -> {
                    instance.command(txnId).commitInvalidate();
                });
                trySuccess(Outcome.Invalidated);
                return;
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
                Route route = InvalidateOk.findRoute(invalidateOks);
                // TODO: commitInvalidate (and others) should skip the network for local applications,
                //  so we do not need to explicitly do so here before notifying the waiter
                commitInvalidate(node, txnId, route != null ? route : someKeys, txnId);
                // TODO: pick a reasonable upper bound, so we don't invalidate into an epoch/commandStore that no longer cares about this command
                node.forEachLocalSince(someKeys, txnId, instance -> {
                    instance.command(txnId).commitInvalidate();
                });
            }
            finally
            {
                trySuccess(Outcome.Invalidated);
            }
        });
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
    public void accept(Outcome success, Throwable fail)
    {
        if (success != null) trySuccess(success);
        else tryFailure(fail);
    }
}
