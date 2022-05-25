package accord.messages;

import accord.api.ProgressLog.ProgressShard;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.AbstractRoute;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.messages.SimpleReply.ok;

public class InformDurable extends TxnRequest
{
    final TxnId txnId;
    final Timestamp executeAt;

    public InformDurable(Id to, Topologies topologies, AbstractRoute route, TxnId txnId, Timestamp executeAt)
    {
        super(to, topologies, route);
        this.txnId = txnId;
        this.executeAt = executeAt;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
//        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope);
//        Timestamp at = txnId;
//        if (progressKey == null)
//        {
//            at = executeAt;
//            progressKey = node.selectProgressKey(executeAt.epoch, scope, scope.homeKey);
//        }
//
//        ProgressShard shard = progressKey.equals(scope.homeKey) ? Home : Local;
//        Reply reply = node.ifLocal(progressKey, at, instance -> {
//            Command command = instance.command(txnId);
//            command.informOfRoute(scope, executeAt.epoch, );
//            command.setGloballyPersistent(scope.homeKey, executeAt);
//            instance.progressLog().durable(txnId, null, shard);
//            return ok();
//        });
//
//        if (reply == null)
//            throw new IllegalStateException();
//
//        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_HOME_DURABLE_REQ;
    }
}
