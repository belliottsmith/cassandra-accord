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
    CheckOnUncommitted(Node node, TxnId txnId, AbstractRoute route, long srcEpoch, long trgEpoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        super(node, txnId, route, srcEpoch, trgEpoch, callback);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, AbstractRoute route, long srcEpoch, long trgEpoch,
                                                        BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, route, srcEpoch, trgEpoch, callback);
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ((CheckStatusOkFull)ok).fullStatus.hasBeen(Committed);
    }

    @Override
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.fullStatus)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
                break;
            case AcceptedInvalidate:
            case Accepted:
            case PreAccepted:
                RoutingKey progressKey = node.trySelectProgressKey(txnId, full.route);
                node.forEachLocal(someKeys, txnId, (full.executeAt == null ? txnId : full.executeAt), commandStore -> {
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
