package accord.coordinate;

import java.util.Set;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShards extends QuorumReadCoordinator<CheckStatusReply>
{
    final RoutingKeys someKeys;
    final long epoch;
    final IncludeInfo includeInfo;

    CheckStatusOk merged;

    CheckShards(Node node, TxnId txnId, RoutingKeys someKeys, long epoch, IncludeInfo includeInfo)
    {
        super(node, node.topology().forEpoch(someKeys, epoch), txnId);
        this.epoch = epoch;
        this.someKeys = someKeys;
        this.includeInfo = includeInfo;
    }

    @Override
    void contact(Set<Id> nodes)
    {
        node.send(nodes, new CheckStatus(txnId, someKeys, epoch, includeInfo), this);
    }

    abstract boolean isSufficient(Id from, CheckStatusOk ok);

    @Override
    Action process(Id from, CheckStatusReply reply)
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
