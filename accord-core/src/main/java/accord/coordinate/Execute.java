package accord.coordinate;

import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Data;
import accord.coordinate.tracking.ReadTracker;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Deps;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;

class Execute implements Callback<ReadReply>
{
    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Route route;
    final Timestamp executeAt;
    final Deps deps;
    final Topologies topologies;
    final ReadTracker readTracker;
    final BiConsumer<Result, Throwable> callback;
    private Data data;
    private boolean isDone;

    private Execute(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.executeAt = executeAt;
        this.deps = deps;
        this.topologies = node.topology().forEpoch(route, executeAt.epoch);
        Topologies readTopologies = node.topology().forEpoch(txn.read.keys(), executeAt.epoch);
        this.readTracker = new ReadTracker(readTopologies);
        this.callback = callback;
    }

    private void start()
    {
        Set<Id> readSet = readTracker.computeMinimalReadSetAndMarkInflight();
        Commit.commitAndRead(node, topologies, txnId, txn, route, executeAt, deps, readSet, this);
    }

    public static void execute(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Execute execute = new Execute(node, txnId, txn, route, executeAt, deps, callback);
        execute.start();
    }

    @Override
    public void onSuccess(Id from, ReadReply reply)
    {
        if (isDone)
            return;

        if (!reply.isOK())
        {
            isDone = true;
            callback.accept(null, new Preempted(txnId, route.homeKey));
            return;
        }

        data = data == null ? ((ReadOk) reply).data
                            : data.merge(((ReadOk) reply).data);

        readTracker.recordReadSuccess(from);

        if (readTracker.hasCompletedRead())
        {
            isDone = true;
            Result result = txn.result(data);
            callback.accept(result, null);
            Persist.persist(node, topologies, txnId, route, executeAt, deps, txn.execute(executeAt, data), result);
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        Set<Id> readFrom = readTracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
            node.send(readFrom, to -> new ReadData(to, readTracker.topologies(), txnId, route, executeAt), this);
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        // try again with another random node
        // TODO: API hooks
        if (!(failure instanceof Timeout))
            failure.printStackTrace();

        // TODO: introduce two tiers of timeout, one to trigger a retry, and another to mark the original as failed
        // TODO: if we fail, nominate another coordinator from the homeKey shard to try
        readTracker.recordReadFailure(from);
        Set<Id> readFrom = readTracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
        {
            node.send(readFrom, to -> new ReadData(to, readTracker.topologies(), txnId, route, executeAt), this);
        }
        else if (readTracker.hasFailed())
        {
            if (failure instanceof Timeout)
                failure = ((Timeout) failure).with(txnId, route.homeKey);

            isDone = true;
            callback.accept(null, failure);
        }
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        isDone = true;
        callback.accept(null, failure);
    }
}
