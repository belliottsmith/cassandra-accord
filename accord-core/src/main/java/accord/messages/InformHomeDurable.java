package accord.messages;

import java.util.Set;

import accord.api.ProgressLog.ProgressShard;
import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.messages.SimpleReply.nack;
import static accord.messages.SimpleReply.ok;

public class InformHomeDurable implements Request
{
    final TxnId txnId;
    final RoutingKey homeKey;
    final Timestamp executeAt;
    final Set<Id> persistedOn;

    public InformHomeDurable(TxnId txnId, RoutingKey homeKey, Timestamp executeAt, Set<Id> persistedOn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.persistedOn = persistedOn;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.ifLocal(homeKey, txnId, instance -> {
            instance.command(txnId).setGloballyPersistent(homeKey, executeAt);
            instance.progressLog().durable(txnId, persistedOn, Home);
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
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
