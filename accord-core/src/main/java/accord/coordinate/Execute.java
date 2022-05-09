package accord.coordinate;

import java.util.Set;

import accord.api.Data;
import accord.api.Key;
import accord.coordinate.tracking.ReadTracker;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.topology.Topologies;
import accord.txn.*;
import accord.messages.ReadData.ReadReply;
import accord.txn.Dependencies;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

class Execute extends AsyncFuture<Result> implements Callback<ReadReply>
{
    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;
    final Timestamp executeAt;
    final Topologies topologies;
    final Keys keys;
    final Dependencies deps;
    final ReadTracker readTracker;
    private Data data;

    private Execute(Node node, Agreed agreed)
    {
        this.node = node;
        this.txnId = agreed.txnId;
        this.txn = agreed.txn;
        this.homeKey = agreed.homeKey;
        this.keys = txn.keys();
        this.deps = agreed.deps;
        this.executeAt = agreed.executeAt;

        // TODO: perhaps compose these different behaviours differently?
        if (agreed.applied != null)
        {
            topologies = null;
            readTracker = null;
            Persist.persistAndCommit(node, txnId, homeKey, txn, executeAt, deps, agreed.applied, agreed.result);
            trySuccess(agreed.result);
        }
        else
        {
            Topologies executeTopologies = node.topology().forEpoch(txn, executeAt.epoch);
            Topologies readTopologies = node.topology().forEpoch(txn.read.keys(), executeAt.epoch);
            topologies = executeTopologies;
            readTracker = new ReadTracker(readTopologies);
            Set<Id> readSet = readTracker.computeMinimalReadSetAndMarkInflight();
            Commit.commitAndRead(node, executeTopologies, txnId, txn, homeKey, executeAt, deps, readSet, this);
        }
    }

    @Override
    public void onSuccess(Id from, ReadReply reply)
    {
        if (isDone())
            return;

        if (!reply.isFinal())
            return;

        if (!reply.isOK())
        {
            tryFailure(new Preempted(txnId, homeKey));
            return;
        }

        data = data == null ? ((ReadOk) reply).data
                            : data.merge(((ReadOk) reply).data);

        readTracker.recordReadSuccess(from);

        if (readTracker.hasCompletedRead())
        {
            Result result = txn.result(data);
            trySuccess(result);
            Persist.persist(node, topologies, txnId, homeKey, txn, executeAt, deps, txn.execute(executeAt, data), result);
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        Set<Id> readFrom = readTracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
            node.send(readFrom, to -> new ReadData(to, readTracker.topologies(), txnId, txn, homeKey, executeAt), this);
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        // try again with another random node
        // TODO: API hooks
        if (!(throwable instanceof Timeout))
            throwable.printStackTrace();

        // TODO: introduce two tiers of timeout, one to trigger a retry, and another to mark the original as failed
        // TODO: if we fail, nominate another coordinator from the homeKey shard to try
        readTracker.recordReadFailure(from);
        Set<Id> readFrom = readTracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
        {
            node.send(readFrom, to -> new ReadData(to, readTracker.topologies(), txnId, txn, homeKey, executeAt), this);
        }
        else if (readTracker.hasFailed())
        {
            if (throwable instanceof Timeout)
                throwable = ((Timeout) throwable).with(txnId, homeKey);
            tryFailure(throwable);
        }
    }

    static Future<Result> execute(Node instance, Agreed agreed)
    {
        return new Execute(instance, agreed);
    }
}
