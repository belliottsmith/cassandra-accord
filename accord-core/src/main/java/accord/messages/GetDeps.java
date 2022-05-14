package accord.messages;

import accord.api.RoutingKey;
import com.google.common.base.Preconditions;

import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import java.util.Collections;

import static accord.messages.PreAccept.calculatePartialDeps;

public class GetDeps extends TxnRequest.WithUnsynced
{
    final Keys keys;
    final Timestamp executeAt;
    final Txn.Kind kind;

    public GetDeps(Id to, Topologies topologies, Route route, TxnId txnId, Txn txn, Timestamp executeAt)
    {
        // TODO: we don't need to send to homeKey here, can use keys directly
        super(to, topologies, txnId, route);
        this.keys = txn.keys().slice(scope.covering);
        this.executeAt = executeAt;
        this.kind = txn.kind();
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        PartialDeps deps = node.mapReduceLocal(this, minEpoch, executeAt.epoch, instance -> {
            // TODO: shrink ranges to those that intersect key
            KeyRanges ranges = instance.ranges().between(minEpoch, executeAt.epoch);
            return calculatePartialDeps(instance, txnId, keys, kind, executeAt, ranges);
        }, PartialDeps::with);

        node.reply(replyToNode, replyContext, new GetDepsOk(deps));
    }

    @Override
    public MessageType type()
    {
        return MessageType.GET_DEPS_REQ;
    }

    @Override
    public String toString()
    {
        return "GetDeps{" +
               "txnId:" + txnId +
               ", keys:" + keys +
               ", executeAt:" + executeAt +
               '}';
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<? extends RoutingKey> keys()
    {
        return keys;
    }

    public static class GetDepsOk implements Reply
    {
        public final PartialDeps deps;

        public GetDepsOk(PartialDeps deps)
        {
            Preconditions.checkNotNull(deps);
            this.deps = deps;
        }

        @Override
        public String toString()
        {
            return toString("GetDepsOk");
        }

        String toString(String kind)
        {
            return kind + "{" + deps + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.GET_DEPS_RSP;
        }
    }

}
