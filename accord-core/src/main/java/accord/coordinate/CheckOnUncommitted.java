package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.primitives.AbstractRoute;
import accord.primitives.TxnId;

import static accord.local.Status.Committed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnUncommitted extends CheckOnCommitted
{
    CheckOnUncommitted(Node node, TxnId txnId, AbstractRoute route, long shardEpoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        super(node, txnId, route, shardEpoch, callback);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, AbstractRoute route, long epoch,
                                                        BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, route, epoch, callback);
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ok.status.hasBeen(Committed);
    }

    @Override
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
                break;
            case AcceptedInvalidate:
            case Accepted:
            case PreAccepted:
                RoutingKey progressKey = node.trySelectProgressKey(txnId, full.route);
                node.forEachLocal(someKeys, txnId.epoch, full.executeAt.epoch, commandStore -> {
                    commandStore.command(txnId).preaccept(full.partialTxn, full.route, progressKey);
                });
                break;
            case Executed:
            case Applied:
            case Committed:
            case ReadyToExecute:
                super.onSuccessCriteriaOrExhaustion(full);
            case Invalidated:
                node.forEachLocalSince(someKeys, txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.commitInvalidate();
                });
        }
    }
}
