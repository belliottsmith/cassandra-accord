package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

import static accord.local.Status.Executed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnCommitted extends CheckShards
{
    final BiConsumer<CheckStatusOkFull, Throwable> callback;

    CheckOnCommitted(Node node, TxnId txnId, RoutingKeys someKeys, long someEpoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        super(node, txnId, someKeys, someEpoch, IncludeInfo.All);
        this.callback = callback;
    }

    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, RoutingKeys someKeys, long epoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, someKeys, epoch, callback);
        checkOnCommitted.start();
        return checkOnCommitted;
    }

    @Override
    boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ok.status.hasBeen(Executed);
    }

    @Override
    void onDone(Done done, Throwable failure)
    {
        TO DO COMPILE ERROR
    }

    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull max)
    {
        switch (max.status)
        {
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case Invalidated:
                return;
        }

        // TODO (now): is this safe? Need to make sure we have invoked with a complete PartialRoute
        RoutingKey progressKey = node.trySelectProgressKey(txnId, max.partialTxn.keys, max.homeKey);
        switch (max.status)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                node.forEachLocalSince(max.partialTxn.keys, max.executeAt.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.apply(max.homeKey, progressKey, max.executeAt, max.committedDeps, max.writes, max.result);
                });
                node.forEachLocal(max.partialTxn.keys, txnId.epoch, max.executeAt.epoch - 1, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(max.homeKey, progressKey, max.executeAt, max.committedDeps, max.partialTxn);
                });
                break;
            case Committed:
            case ReadyToExecute:
                node.forEachLocalSince(max.partialTxn.keys, txnId.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(max.homeKey, progressKey, max.executeAt, max.committedDeps, max.partialTxn);
                });
        }
    }
}
