package accord.messages;

import java.util.List;

import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;

public class BeginInvalidate implements EpochRequest
{
    final Ballot ballot;
    final TxnId txnId;
    final Key someKey;

    public BeginInvalidate(TxnId txnId, Key someKey, Ballot ballot)
    {
        this.txnId = txnId;
        this.someKey = someKey;
        this.ballot = ballot;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        RecoverReply reply = node.ifLocal(someKey, txnId, instance -> {
            Command command = instance.command(txnId);

            if (!command.preAcceptInvalidate(ballot))
                return new InvalidateNack(command.promised(), command.homeKey());

            return new InvalidateOk(txnId, command.status(), command.accepted(), command.executeAt(), command.savedPartialDeps(),
                                    command.writes(), command.result(), command.routingKeys(), command.homeKey());
        });

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch;
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_INVALIDATE_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginInvalidate{" +
               "txnId:" + txnId +
               ", ballot:" + ballot +
               '}';
    }

    public static class InvalidateOk extends RecoverOk
    {
        public final PartialTxn txn;
        public final RoutingKeys routingKeys;
        public final RoutingKey homeKey;

        public InvalidateOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, PartialDeps deps, Writes writes, Result result, PartialTxn txn, RoutingKeys routingKeys, RoutingKey homeKey)
        {
            super(txnId, status, accepted, executeAt, deps, null, null, false, writes, result);
            this.txn = txn;
            this.routingKeys = routingKeys;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOK()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toString("InvalidateOk");
        }

        public static Route findRoute(List<InvalidateOk> invalidateOks)
        {
            for (InvalidateOk ok : invalidateOks)
            {
                if (ok.routingKeys != null)
                    return ok.routingKeys.toRoute(ok.homeKey);
            }
            throw new IllegalStateException();
        }
    }

    public static class InvalidateNack extends RecoverNack
    {
        public final RoutingKey homeKey;
        public InvalidateNack(Ballot supersededBy, RoutingKey homeKey)
        {
            super(supersededBy);
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOK()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InvalidateNack{supersededBy:" + supersededBy + '}';
        }
    }
}
