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
import accord.messages.Commit.Kind;
import accord.messages.InformHomeDurable;
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
    final Txn txn;
    final Timestamp executeAt;
    final Deps deps;
    final QuorumTracker tracker;
    final Set<Id> persistedOn;
    boolean isDone;

    public static void persist(Node node, Topologies topologies, TxnId txnId, Route route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Persist persist = new Persist(node, topologies, txnId, route, txn, executeAt, deps);
        node.send(topologies.nodes(), to -> new Apply(to, topologies, executeAt.epoch, txnId, route, executeAt, deps, writes, result), persist);
    }

    public static void persistAndCommit(Node node, TxnId txnId, Route route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies persistTo = node.topology().forEpoch(route, executeAt.epoch);
        Persist persist = new Persist(node, persistTo, txnId, route, txn, executeAt, deps);
        node.send(persistTo.nodes(), to -> new Apply(to, persistTo, executeAt.epoch, txnId, route, executeAt, deps, writes, result), persist);
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().forEpochRange(route, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, persistTo, txnId, txn, route, executeAt, deps);
        }
    }

    private Persist(Node node, Topologies topologies, TxnId txnId, Route route, Txn txn, Timestamp executeAt, Deps deps)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.route = route;
        this.tracker = new QuorumTracker(topologies);
        this.executeAt = executeAt;
        this.persistedOn = new HashSet<>();
    }

    @Override
    public void onSuccess(Id from, ApplyReply reply)
    {
        switch (reply)
        {
            default: throw new IllegalStateException();
            case Applied:
                persistedOn.add(from);
                if (tracker.success(from) && !isDone)
                {
                    // TODO: send to non-home replicas also, so they may clear their log more easily?
                    Shard homeShard = node.topology().forEpochIfKnown(route.homeKey, txnId.epoch);
                    node.send(homeShard, new InformHomeDurable(txnId, route.homeKey, executeAt, persistedOn));
                    isDone = true;
                }
                break;
            case Insufficient:
                Topologies topologies = node.topology().forEpochRange(route, txnId.epoch, executeAt.epoch);
                node.send(from, new Commit(Kind.Maximal, from, topologies, txnId, txn, route, executeAt, deps, false));
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
