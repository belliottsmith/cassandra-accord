package accord.messages;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.primitives.TxnId;

import static accord.messages.InformOfTxn.InformOfTxnNack.nack;
import static accord.messages.InformOfTxn.InformOfTxnOk.ok;

public class InformOfTxn implements EpochRequest
{
    final TxnId txnId;
    final RoutingKey homeKey;

    public InformOfTxn(TxnId txnId, RoutingKey homeKey)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.ifLocal(homeKey, txnId, instance -> {
            // TODO (now): we don't want to preaccept here, only mark it in the progress log
            Command command = instance.ifPresent(txnId);
            if (command == null || !command.hasBeen(Status.PreAccepted))
                instance.progressLog().unwitnessed(txnId);
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

    public interface InformOfTxnReply extends Reply
    {
        boolean isOk();
    }

    public static class InformOfTxnOk implements InformOfTxnReply
    {
        private static final InformOfTxnOk instance = new InformOfTxnOk();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_RSP;
        }

        static InformOfTxnReply ok()
        {
            return instance;
        }

        private InformOfTxnOk() { }

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

    public static class InformOfTxnNack implements InformOfTxnReply
    {
        private static final InformOfTxnNack instance = new InformOfTxnNack();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_RSP;
        }

        static InformOfTxnReply nack()
        {
            return instance;
        }

        private InformOfTxnNack() { }

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
        return MessageType.INFORM_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }
}
