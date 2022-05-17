package accord.messages;

import java.util.Set;

import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.DeterministicIdentitySet;

public class ReadData extends TxnRequest<PartialRoute>
{
    static class LocalRead implements Listener
    {
        final TxnId txnId;
        final Node node;
        final Node.Id replyToNode;
        final ReplyContext replyContext;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;

        LocalRead(TxnId txnId, Node node, Id replyToNode, ReplyContext replyContext)
        {
            this.txnId = txnId;
            this.node = node;
            this.replyToNode = replyToNode;
            this.replyContext = replyContext;
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                case Committed:
                    return;

                case Executed:
                case Applied:
                case Invalidated:
                    obsolete();
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        private void read(Command command)
        {
            // TODO: threading/futures (don't want to perform expensive reads within this mutually exclusive context)
            Data next = command.partialTxn().read(command);
            data = data == null ? next : data.merge(next);

            waitingOn.remove(command.commandStore);
            if (waitingOn.isEmpty())
                node.reply(replyToNode, replyContext, new ReadOk(data));
        }

        void obsolete()
        {
            if (!isObsolete)
            {
                isObsolete = true;
                node.reply(replyToNode, replyContext, new ReadNack());
            }
        }

        synchronized void setup(TxnId txnId, PartialRoute scope, Timestamp executeAt)
        {
            waitingOn = node.collectLocal(scope, executeAt, DeterministicIdentitySet::new);
            // FIXME: fix/check thread safety
            CommandStore.onEach(waitingOn, instance -> {
                Command command = instance.command(txnId);
                switch (command.status())
                {
                    default:
                    case NotWitnessed:
                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                    case Committed:
                        instance.progressLog().waiting(txnId, scope);
                        command.addListener(this);
                        break;

                    case Executed:
                    case Applied:
                    case Invalidated:
                        obsolete();
                        break;

                    case ReadyToExecute:
                        if (!isObsolete)
                            read(command);
                }
            });
        }
    }

    public final TxnId txnId;
    public final Timestamp executeAt;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Route route, Timestamp executeAt)
    {
        super(to, topologies, route, Route.SLICER);
        this.txnId = txnId;
        this.executeAt = executeAt;
    }

    public void process(Node node, Node.Id from, ReplyContext replyContext)
    {
        new LocalRead(txnId, node, from, replyContext)
            .setup(txnId, scope(), executeAt);
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public static class ReadReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.READ_RSP;
        }

        public boolean isOK()
        {
            return true;
        }
    }

    public static class ReadNack extends ReadReply
    {
        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "ReadNack";
        }
    }

    public static class ReadOk extends ReadReply
    {
        public final Data data;
        public ReadOk(Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               '}';
    }
}
