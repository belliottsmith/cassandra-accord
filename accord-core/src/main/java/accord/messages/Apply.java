package accord.messages;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.primitives.TxnId;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest<PartialRoute>
{
    public final TxnId txnId;
    public final Timestamp executeAt;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, topologies, route, Route.SLICER);
        this.txnId = txnId;
        this.deps = deps.slice(scope.covering);
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope, scope.homeKey);
        node.forEachLocalSince(scope(), executeAt,
                               instance -> instance.command(txnId).apply(scope.homeKey, progressKey, executeAt, deps, writes, result));
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
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
