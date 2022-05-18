package accord.messages;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.primitives.TxnId;

import static accord.messages.InformOfTxnId.InformOfTxnIdNack.nack;
import static accord.messages.InformOfTxnId.InformOfTxnIdOk.ok;

public class InformOfTxnId implements EpochRequest
{
    final TxnId txnId;
    final RoutingKey homeKey;

    public InformOfTxnId(TxnId txnId, RoutingKey homeKey)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.ifLocal(homeKey, txnId, instance -> {
            Command command = instance.command(txnId);
            if (!command.hasBeen(Status.PreAccepted))
            {
                command.updateHomeKey(homeKey);
                instance.progressLog().unwitnessed(txnId, true, true);
            }
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfTxn{txnId:" + txnId + '}';
    }

    public interface InformOfTxnIdReply extends Reply
    {
        boolean isOk();
    }

    public static class InformOfTxnIdOk implements InformOfTxnIdReply
    {
        private static final InformOfTxnIdOk instance = new InformOfTxnIdOk();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_TXNID_RSP;
        }

        static InformOfTxnIdReply ok()
        {
            return instance;
        }

        private InformOfTxnIdOk() { }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "InformOfTxnOk";
        }
    }

    public static class InformOfTxnIdNack implements InformOfTxnIdReply
    {
        private static final InformOfTxnIdNack instance = new InformOfTxnIdNack();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_TXNID_RSP;
        }

        static InformOfTxnIdReply nack()
        {
            return instance;
        }

        private InformOfTxnIdNack() { }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InformOfTxnNack";
        }
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_TXNID_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }
}
