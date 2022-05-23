package accord.coordinate;

import java.util.function.BiConsumer;

import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.primitives.RoutingKeys;
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
    CheckOnUncommitted(Node node, TxnId txnId, RoutingKeys someKeys, long shardEpoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        super(node, txnId, someKeys, shardEpoch, callback);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, RoutingKeys someKeys, long epoch,
                                                        BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, someKeys, epoch, callback);
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ok.status.hasBeen(Committed);
    }

    @Override
    void onDone(Done done, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else
        {
            switch (done)
            {
                default: throw new IllegalStateException();
                case Success:
                    break;
                case Exhausted:
                case ReachedQuorum:
                    TO DO COMPILE ERROR

            }
        }
    }

    @Override
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
            case AcceptedInvalidate:
                break;
            case PreAccepted:
            case Accepted:
                node.forEachLocalSince(full.partialTxn.keys, txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.updateHomeKey(full.homeKey);
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
