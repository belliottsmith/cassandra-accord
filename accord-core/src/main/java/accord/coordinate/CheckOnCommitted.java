package accord.coordinate;

import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.local.Status.Executed;

/**
 * Check on the status of a known-committed transaction. Returns early if any result indicates Executed, otherwise
 * waits only for a quorum and returns the maximum result. Updates local command stores based on the obtained information.
 *
 * If a command is durable (i.e. executed on a majority on all shards) this is sufficient to replicate the command locally.
 */
public class CheckOnCommitted extends CheckShards
{
    final BiConsumer<? super CheckStatusOkFull, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final long untilLocalEpoch;

    CheckOnCommitted(Node node, TxnId txnId, AbstractRoute route, long untilRemoteEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        // TODO (now): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, route, untilRemoteEpoch, IncludeInfo.All);
        this.callback = callback;
        this.untilLocalEpoch = untilLocalEpoch;
    }

    // TODO (now): many callers only need to consult precisely executeAt.epoch remotely
    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, AbstractRoute route, long untilRemoteEpoch, long untilLocalEpoch, BiConsumer<? super CheckStatusOkFull, Throwable> callback)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, route, untilRemoteEpoch, untilLocalEpoch, callback);
        checkOnCommitted.start();
        return checkOnCommitted;
    }

    protected AbstractRoute route()
    {
        return (AbstractRoute) someKeys;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ((CheckStatusOkFull)ok).fullStatus.hasBeen(Executed);
    }

    @Override
    protected void onDone(Done done, Throwable failure)
    {
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            super.onDone(done, null);
            onSuccessCriteriaOrExhaustion((CheckStatusOkFull) merged);
        }
    }

    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.fullStatus)
        {
            case Invalidated:
                node.forEachLocal(someKeys, txnId.epoch, untilLocalEpoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commitInvalidate();
                });
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                return;
        }

        Timestamp executeAt = full.executeAt;
        KeyRanges minCommitRanges = node.topology().localRangesForEpoch(txnId.epoch);
        KeyRanges minExecuteRanges = node.topology().localRangesForEpochs(executeAt.epoch, Math.max(executeAt.epoch, untilLocalEpoch));
        KeyRanges allRanges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);

        PartialRoute route = route().slice(allRanges);
        RoutingKey progressKey = node.trySelectProgressKey(txnId, route);

        boolean canCommit = route.covers(minCommitRanges);
        boolean canExecute = route.covers(minExecuteRanges);

        Preconditions.checkState(canCommit);
        Preconditions.checkState(untilRemoteEpoch < full.executeAt.epoch || canExecute);
        Preconditions.checkState(full.partialTxn.covers(route));
        Preconditions.checkState(full.committedDeps.covers(route));

        PartialTxn partialTxn = full.partialTxn.reconstitutePartial(route).slice(allRanges, true);
        PartialDeps partialDeps = full.committedDeps.reconstitutePartial(route).slice(allRanges);

        switch (full.fullStatus)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                if (canExecute)
                {
                    // TODO: assert that the outcome is Success or Redundant, but only for those we expect to succeed
                    //  (i.e. those covered by Route)
                    node.forEachLocal(route, txnId.epoch, executeAt.epoch - 1, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(route(), progressKey, partialTxn, executeAt, partialDeps);
                    });

                    node.forEachLocal(route, executeAt.epoch, untilLocalEpoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(route(), progressKey, partialTxn, executeAt, partialDeps);
                        command.apply(untilLocalEpoch, route, executeAt, partialDeps, full.writes, full.result);
                    });
                    break;
                }
            case Committed:
            case ReadyToExecute:
                node.forEachLocal(route, txnId.epoch, untilLocalEpoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(route(), progressKey, partialTxn, executeAt, partialDeps);
                });
        }

        callback.accept(full, null);
    }
}
