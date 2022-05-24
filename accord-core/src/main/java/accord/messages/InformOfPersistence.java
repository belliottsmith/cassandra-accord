package accord.messages;

import java.util.Set;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.messages.SimpleReply.nack;
import static accord.messages.SimpleReply.ok;

public class InformOfPersistence implements Request
{
    final TxnId txnId;
    final RoutingKey homeKey;
    final Timestamp executeAt;
    final Set<Id> persistedOn;

    public InformOfPersistence(TxnId txnId, RoutingKey homeKey, Timestamp executeAt, Set<Id> persistedOn)
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
            instance.progressLog().executedOnAllShards(txnId, persistedOn);
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
        return MessageType.INFORM_PERSISTED_REQ;
    }
}
