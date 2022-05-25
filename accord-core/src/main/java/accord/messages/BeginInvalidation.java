package accord.messages;

import java.util.List;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

public class BeginInvalidation implements EpochRequest
{
    final Ballot ballot;
    final TxnId txnId;
    final RoutingKey someKey;

    public BeginInvalidation(TxnId txnId, RoutingKey someKey, Ballot ballot)
    {
        this.txnId = txnId;
        this.someKey = someKey;
        this.ballot = ballot;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        InvalidateReply reply = node.ifLocal(someKey, txnId, instance -> {
            Command command = instance.command(txnId);

            if (!command.preacceptInvalidate(ballot))
                return new InvalidateNack(command.promised(), command.homeKey());

            return new InvalidateOk(command.status(), command.route(), command.homeKey());
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

    public interface InvalidateReply extends Reply
    {
        boolean isOk();
    }

    public static class InvalidateOk implements InvalidateReply
    {
        public final Status status;
        public final @Nullable RoutingKeys routingKeys;
        public final RoutingKey homeKey;

        public InvalidateOk(Status status, @Nullable RoutingKeys routingKeys, RoutingKey homeKey)
        {
            this.status = status;
            this.routingKeys = routingKeys;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "InvalidateOk{" + status + ',' + routingKeys + ',' + homeKey + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }

        public static Route findRoute(List<InvalidateOk> invalidateOks)
        {
            for (InvalidateOk ok : invalidateOks)
            {
                if (ok.routingKeys != null)
                    return ok.routingKeys.toRoute(ok.homeKey);
            }
            return null;
        }

        public static RoutingKey findHomeKey(List<InvalidateOk> invalidateOks)
        {
            for (InvalidateOk ok : invalidateOks)
            {
                if (ok.homeKey != null)
                    return ok.homeKey;
            }
            return null;
        }
    }

    public static class InvalidateNack implements InvalidateReply
    {
        public final Ballot supersededBy;
        public final RoutingKey homeKey;
        public InvalidateNack(Ballot supersededBy, RoutingKey homeKey)
        {
            this.supersededBy = supersededBy;
            this.homeKey = homeKey;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InvalidateNack{supersededBy:" + supersededBy + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }
    }
}