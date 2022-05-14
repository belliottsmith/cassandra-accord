package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest
{
    public final TxnId txnId;
    public final Txn txn;
    protected final Key homeKey;
    public final Timestamp executeAt;
    public final Deps deps;
    public final Writes writes;
    public final Result result;

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, topologies, txn.keys);
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.deps = deps;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.trySelectProgressKey(txnId, txn.keys, homeKey);
        node.forEachLocalSince(scope(), executeAt,
                               instance -> instance.command(txnId).apply(txn, homeKey, progressKey, executeAt, deps, writes, result));
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        node.reply(replyToNode, replyContext, ApplyOk.INSTANCE);
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public static class ApplyOk implements Reply
    {
        public static final ApplyOk INSTANCE = new ApplyOk();
        public ApplyOk() {}

        @Override
        public String toString()
        {
            return "ApplyOk";
        }

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
