package accord.messages;

import accord.api.ProgressLog.ProgressShard;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.TxnOperation;
import accord.primitives.AbstractRoute;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import java.util.Collections;

import static accord.api.ProgressLog.ProgressShard.Adhoc;
import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.local.TxnOperation.scopeFor;
import static accord.messages.SimpleReply.Ok;

public class InformDurable extends TxnRequest implements TxnOperation
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
        Timestamp at = txnId;
        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope);
        ProgressShard shard;
        if (progressKey == null)
        {
            // we need to pick a progress log, but this node might not have participated in the coordination epoch
            // in this rare circumstance we simply pick a key to select some progress log to coordinate this
            at = executeAt;
            progressKey = node.selectProgressKey(executeAt.epoch, scope, scope.homeKey);
            shard = Adhoc;
        }
        else
        {
            shard = scope.homeKey.equals(progressKey) ? Home : Local;
        }

        // TODO (soon): do not load from disk to perform this update
        Reply reply = node.ifLocal(scopeFor(txnId), progressKey, at.epoch, instance -> {
            Command command = instance.command(txnId);
            command.setGloballyPersistent(scope.homeKey, executeAt);
            instance.progressLog().durable(txnId, scope, shard);
            return Ok;
        });

        if (reply == null)
            throw new IllegalStateException();

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

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<? extends RoutingKey> keys()
    {
        return Collections.emptyList();
    }
}
