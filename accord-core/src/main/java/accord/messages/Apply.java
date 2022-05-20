package accord.messages;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.Route;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.primitives.TxnId;

import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest
{
    public final TxnId txnId;
    public final Timestamp executeAt;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;

    public Apply(Node.Id to, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, topologies, route);
        this.txnId = txnId;
        // TODO: we shouldn't send deps unless we need to (but need to implement fetching them if they're not present)
        this.deps = deps.slice(scope.covering);
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope, scope.homeKey);
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        node.mapReduceLocalSince(scope(), executeAt, instance -> {
            Command command = instance.command(txnId);
            switch (command.apply(scope.homeKey, progressKey, executeAt, deps, writes, result))
            {
                default:
                case REJECTED_BALLOT:
                    throw new IllegalStateException();
                case INCOMPLETE:
                    throw new UnsupportedOperationException();
                case REDUNDANT:
                case SUCCESS:
                    return ApplyReply.OK;
            }
        }, (r1, r2) -> r2.isOk() ? r1 : r2);
        node.reply(replyToNode, replyContext, ApplyReply.OK);
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public enum ApplyReply implements Reply
    {
        OK, INCOMPLETE;

        public boolean isOk()
        {
            return this == OK;
        }

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }

        @Override
        public String toString()
        {
            return isOk() ? "ApplyOk" : "ApplyIncomplete";
        }

        @Override
        public boolean isFinal()
        {
            return isOk();
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
