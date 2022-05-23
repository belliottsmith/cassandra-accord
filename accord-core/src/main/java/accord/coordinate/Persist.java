package accord.coordinate;

import java.util.HashSet;
import java.util.Set;

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.InformOfPersistence;
import accord.primitives.Deps;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

// TODO: do not extend AsyncFuture, just use a simple BiConsumer callback
public class Persist implements Callback<ApplyReply>
{
    final Node node;
    final TxnId txnId;
    final Route route;
    final Timestamp executeAt;
    final QuorumTracker tracker;
    final Set<Id> persistedOn;
    boolean isDone;

    public static void persist(Node node, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Persist persist = new Persist(node, topologies, txnId, route, executeAt);
        node.send(topologies.nodes(), to -> new Apply(to, topologies, txnId, route, executeAt, deps, writes, result), persist);
    }

    public static void persistAndCommit(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies persistTo = node.topology().preciseEpochs(route, executeAt.epoch);
        Persist persist = new Persist(node, persistTo, txnId, route, executeAt);
        node.send(persistTo.nodes(), to -> new Apply(to, persistTo, txnId, route, executeAt, deps, writes, result), persist);
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, persistTo, txnId, txn, route, executeAt, deps);
        }
    }

    private Persist(Node node, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.tracker = new QuorumTracker(topologies);
        this.executeAt = executeAt;
        this.persistedOn = new HashSet<>();
    }

    @Override
    public void onSuccess(Id from, ApplyReply response)
    {
        switch (response)
        {
            default: throw new IllegalStateException();
            case OK:
                persistedOn.add(from);
                if (tracker.success(from) && !isDone)
                {
                    // TODO: send to non-home replicas also, so they may clear their log more easily?
                    Shard homeShard = node.topology().forEpochIfKnown(route.homeKey, txnId.epoch);
                    node.send(homeShard, new InformOfPersistence(txnId, route.homeKey, executeAt, persistedOn));
                    isDone = true;
                }
                break;
            case INSUFFICIENT:
                // TODO (now): implement, must send at least routingKeys
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        // TODO: send knowledge of partial persistence?
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
    }
}
