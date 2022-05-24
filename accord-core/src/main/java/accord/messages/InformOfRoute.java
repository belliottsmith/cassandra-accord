package accord.messages;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.AbstractRoute;
import accord.primitives.TxnId;

import static accord.messages.SimpleReply.nack;
import static accord.messages.SimpleReply.ok;

public class InformOfRoute implements EpochRequest
{
    final TxnId txnId;
    final AbstractRoute route;
    final long epoch;

    public InformOfRoute(TxnId txnId, AbstractRoute route, long epoch)
    {
        this.txnId = txnId;
        this.route = route;
        this.epoch = epoch;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.selectProgressKey(txnId, route);
        Reply reply = node.ifLocal(progressKey, txnId, instance -> {
            Command command = instance.command(txnId);
            command.informOfRoute(route, epoch, progressKey);
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfRoute{txnId:" + txnId + ',' + "route:" + route + '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_ROUTE_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }
}
