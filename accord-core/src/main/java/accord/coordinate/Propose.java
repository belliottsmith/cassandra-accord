package accord.coordinate;

import java.util.ArrayList;
import java.util.List;

import accord.api.Key;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.messages.Accept;
import accord.messages.Accept.AcceptOk;
import accord.messages.Accept.AcceptReply;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

class Propose extends AsyncFuture<Agreed>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;

    private List<AcceptOk> acceptOks;
    private Timestamp proposed;
    private QuorumTracker acceptTracker;

    Propose(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
    }

    protected void startAccept(Timestamp executeAt, Dependencies deps, Topologies topologies)
    {
        this.proposed = executeAt;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(topologies);
        // TODO: acceptTracker should be a callback itself, with a reference to us for propagating failure
        node.send(acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, homeKey, txn, executeAt, deps), new Callback<AcceptReply>()
        {
            @Override
            public void onSuccess(Id from, AcceptReply response)
            {
                onAccept(from, response);
            }

            @Override
            public void onFailure(Id from, Throwable throwable)
            {
                if (acceptTracker.failure(from))
                    tryFailure(new Timeout());
            }
        });
    }

    private void onAccept(Id from, AcceptReply reply)
    {
        if (isDone())
            return;

        if (!reply.isOK())
        {
            tryFailure(new Preempted());
            return;
        }

        AcceptOk ok = (AcceptOk) reply;
        acceptOks.add(ok);
        if (acceptTracker.success(from))
            onAccepted();
    }

    private void onAccepted()
    {
        Dependencies deps = new Dependencies();
        for (AcceptOk acceptOk : acceptOks)
            deps.addAll(acceptOk.deps);
        agreed(proposed, deps);
    }

    protected void agreed(Timestamp executeAt, Dependencies deps)
    {
        trySuccess(new Agreed(txnId, txn, homeKey, executeAt, deps, null, null));
    }

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class Invalidate extends AsyncFuture<Agreed> implements Callback<AcceptReply>
    {
        final Node node;
        final Ballot ballot;
        final TxnId txnId;
        final Key someKey;

        private final List<AcceptOk> acceptOks = new ArrayList<>();
        private final QuorumShardTracker acceptTracker;

        Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, Key someKey)
        {
            this.node = node;
            this.acceptTracker = new QuorumShardTracker(shard);
            this.ballot = ballot;
            this.txnId = txnId;
            this.someKey = someKey;
        }

        public static Invalidate proposeInvalidate(Node node, Ballot ballot, TxnId txnId, Key someKey)
        {
            Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
            Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKey);
            node.send(shard.nodes, to -> new Accept.Invalidate(ballot, txnId, someKey), invalidate);
            return invalidate;
        }

        @Override
        public void onSuccess(Id from, AcceptReply reply)
        {
            if (isDone())
                return;

            if (!reply.isOK())
            {
                tryFailure(new Preempted());
                return;
            }

            AcceptOk ok = (AcceptOk) reply;
            acceptOks.add(ok);
            if (acceptTracker.success(from))
                trySuccess(null);
        }

        @Override
        public void onFailure(Id from, Throwable throwable)
        {
            if (acceptTracker.failure(from))
                tryFailure(new Timeout());
        }
    }
}
