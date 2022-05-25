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
    public static class Result extends CheckStatusOkFull
    {
        final Status outcome;

        protected Result(CheckStatusOkFull propagate, Status outcome)
        {
            super(propagate.status, propagate.promised, propagate.accepted, propagate.isCoordinating,
                  propagate.hasExecutedOnAllShards, propagate.route, propagate.homeKey, propagate.partialTxn,
                  propagate.executeAt, propagate.committedDeps, propagate.writes, propagate.result);
            this.outcome = outcome;
        }
    }

    final BiConsumer<? super Result, Throwable> callback;

    CheckOnCommitted(Node node, TxnId txnId, AbstractRoute route, long someEpoch, BiConsumer<? super Result, Throwable> callback)
    {
        // TODO (now): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, route, someEpoch, IncludeInfo.All);
        this.callback = callback;
    }

    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, AbstractRoute route, long epoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, route, epoch, callback);
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
        return ok.status.hasBeen(Executed);
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
            onSuccessCriteriaOrExhaustion(((CheckStatusOkFull) merged).covering(someKeys));
        }
    }

    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                return;
            case Invalidated:
                throw new IllegalStateException();
        }

        KeyRanges minCommitRanges = node.topology().localRangesForEpoch(txnId.epoch);
        KeyRanges minExecuteRanges = node.topology().localRangesForEpoch(full.executeAt.epoch);

        AbstractRoute route = route();
        RoutingKey progressKey = node.trySelectProgressKey(txnId, route);

        boolean canCommit = route.covers(minCommitRanges);
        boolean canExecute = route.covers(minExecuteRanges);

        Preconditions.checkState(canCommit);
        Preconditions.checkState(epoch < full.executeAt.epoch || canExecute);
        Preconditions.checkState(full.partialTxn.covers(route));
        Preconditions.checkState(full.committedDeps.covers(route));

        switch (full.status)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                if (canExecute)
                {
                    // TODO: assert that the outcome is Success or Redundant, but only for those we expect to succeed
                    //  (i.e. those covered by Route)
                    node.forEachLocal(route, txnId.epoch, full.executeAt.epoch - 1, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(route, progressKey, full.partialTxn, full.executeAt, full.committedDeps);
                    });

                    node.forEachLocalSince(route, full.executeAt.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(route, progressKey, full.partialTxn, full.executeAt, full.committedDeps);
                        command.apply(route, full.executeAt, full.committedDeps, full.writes, full.result);
                    });
                    break;
                }
            case Committed:
            case ReadyToExecute:
                node.forEachLocalSince(route, txnId.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(route, progressKey, full.partialTxn, full.executeAt, full.committedDeps);
                });
        }
    }
}
