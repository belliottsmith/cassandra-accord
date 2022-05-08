package accord.messages;

import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.txn.TxnId;
import accord.txn.Keys;

public class WaitOnCommit extends TxnRequest
{
    public final TxnId txnId;
    public final Key homeKey;

    public WaitOnCommit(Id to, Topologies topologies, TxnId txnId, Keys keys, Key homeKey)
    {
        super(to, topologies, keys);
        this.txnId = txnId;
        this.homeKey = homeKey;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Key progressKey = node.selectProgressKey(txnId, scope(), homeKey);
        if (progressKey == null)
            node.reply(replyToNode, replyContext, WaitOnCommitReply.NACK);

        Boolean success = node.ifLocal(progressKey, txnId, instance -> {
            Command command = instance.command(txnId);
            switch (command.status())
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    command.addListener(new Listener()
                    {
                        @Override
                        public void onChange(Command command)
                        {
                            switch (command.status())
                            {
                                default: throw new IllegalStateException();
                                case NotWitnessed:
                                case PreAccepted:
                                case Accepted:
                                case AcceptedInvalidate:
                                    return;

                                case Committed:
                                case ReadyToExecute:
                                case Executed:
                                case Applied:
                                case Invalidated:
                            }

                            command.removeListener(this);
                            node.reply(replyToNode, replyContext, WaitOnCommitReply.OK);
                        }
                    });
                    instance.progressLog().waiting(txnId, null);
                    return true;

                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                case Invalidated:
                    node.reply(replyToNode, replyContext, WaitOnCommitReply.OK);
                    return true;
            }
        });

        if (success == null)
            node.reply(replyToNode, replyContext, WaitOnCommitReply.NACK);
    }

    @Override
    public MessageType type()
    {
        return MessageType.WAIT_ON_COMMIT_REQ;
    }

    public static class WaitOnCommitReply implements Reply
    {
        public static final WaitOnCommitReply OK = new WaitOnCommitReply(true);
        public static final WaitOnCommitReply NACK = new WaitOnCommitReply(false);

        final boolean isOk;
        private WaitOnCommitReply(boolean isOk)
        {
            this.isOk = isOk;
        }

        public boolean isOk()
        {
            return isOk;
        }

        @Override
        public MessageType type()
        {
            return MessageType.WAIT_ON_COMMIT_RSP;
        }
    }
}
