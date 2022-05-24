package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

import static accord.local.Status.Executed;

/**
 * Check on the status of a locally-committed transaction. Returns early if any result indicates Executed, otherwise
 * waits only for a quorum and returns the maximum result. Updates local command stores based on the obtained information.
 *
 * If a command is durable (i.e. executed on a majority on all shards) this is sufficient to replicate the command locally.
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
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            super.onDone(done, failure);
            onSuccessCriteriaOrExhaustion((CheckStatusOkFull) merged);
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
            case Invalidated:
                return;
        }

        KeyRanges coordinateRanges = node.topology().localRangesForEpoch(txnId.epoch);
        KeyRanges executeRanges = node.topology().localRangesForEpoch(full.executeAt.epoch);
        KeyRanges allRanges = coordinateRanges.union(executeRanges);

        AbstractRoute mergedRoute = someKeys instanceof AbstractRoute ? AbstractRoute.merge(full.route, (AbstractRoute) someKeys) : full.route;
        boolean canExecute = mergedRoute.covers(executeRanges);
        boolean canCommit = mergedRoute.covers(coordinateRanges);

        if (epoch == txnId.epoch && !canCommit)
            throw new IllegalStateException();

        if (epoch == full.executeAt.epoch && !canExecute)
            throw new IllegalStateException();

        boolean ownsHomeShard = coordinateRanges.contains(full.homeKey);
        AbstractRoute route = ownsHomeShard ? mergedRoute : mergedRoute.slice(allRanges);
        RoutingKey progressKey = node.trySelectProgressKey(txnId, route);

        switch (full.status)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                if (canExecute)
                {
                    node.forEachLocalSince(route, full.executeAt.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.apply(route, progressKey, full.executeAt, full.committedDeps, full.writes, full.result);
                    });
                }
                if (canCommit)
                {
                    node.forEachLocal(route, txnId.epoch, full.executeAt.epoch - 1, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(route, progressKey, full.executeAt, full.committedDeps, full.partialTxn);
                    });
                }
                break;
            case Committed:
            case ReadyToExecute:
                if (canCommit)
                {
                    node.forEachLocalSince(route, txnId.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(route, progressKey, full.executeAt, full.committedDeps, full.partialTxn);
                    });
                }
        }
    }
}
