package accord.coordinate;

import java.util.Set;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.topology.Topologies;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards extends QuorumReadCoordinator<CheckStatusReply>
{
    final RoutingKeys someKeys;

    /**
     * The epoch until which we want to fetch data from remotely
     */
    final long untilRemoteEpoch;
    final IncludeInfo includeInfo;

    CheckStatusOk merged;

    protected CheckShards(Node node, TxnId txnId, RoutingKeys someKeys, long untilRemoteEpoch, IncludeInfo includeInfo)
    {
        super(node, ensureSufficient(node, txnId, someKeys, untilRemoteEpoch), txnId);
        this.untilRemoteEpoch = untilRemoteEpoch;
        this.someKeys = someKeys;
        this.includeInfo = includeInfo;
    }

    private static Topologies ensureSufficient(Node node, TxnId txnId, RoutingKeys someKeys, long epoch)
    {
        return node.topology().forEpochRange(someKeys, txnId.epoch, epoch);
    }

    @Override
    protected void contact(Set<Id> nodes)
    {
        node.send(nodes, new CheckStatus(txnId, someKeys, untilRemoteEpoch, includeInfo), this);
    }

    protected abstract boolean isSufficient(Id from, CheckStatusOk ok);

    @Override
    protected void onDone(Done done, Throwable failure)
    {
        if (failure != null)
            return;

        if (merged instanceof CheckStatusOkFull)
            merged = ((CheckStatusOkFull) merged).covering(someKeys);

        if (merged.hasExecutedOnAllShards)
            return;

        RoutingKey homeKey = merged.homeKey;
        if (homeKey == null)
            return;
        if (!node.topology().localRangesForEpoch(txnId.epoch).contains(homeKey))
            return;

        node.ifLocal(merged.homeKey, txnId, store -> {
            store.progressLog().durable(txnId, null);
            return null;
        });
    }

    @Override
    protected Action process(Id from, CheckStatusReply reply)
    {
        if (reply.isOk())
        {
            CheckStatusOk ok = (CheckStatusOk) reply;
            if (merged == null) merged = ok;
            else merged = merged.merge(ok);

            if (isSufficient(from, ok))
                return Action.Accept;

            return Action.AcceptQuorum;
        }
        else
        {
            onFailure(from, new IllegalStateException("Submitted command to a replica that did not own the range"));
            return Action.Abort;
        }
    }
}
