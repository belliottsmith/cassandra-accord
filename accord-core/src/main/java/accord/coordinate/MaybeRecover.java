package accord.coordinate;

import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Ballot;
import accord.primitives.TxnId;

import static accord.local.Status.Accepted;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShards implements BiConsumer<Object, Throwable>
{
    final RoutingKey homeKey;
    final Status knownStatus;
    final Ballot knownPromised;
    final boolean knownPromisedHasBeenAccepted;
    final BiConsumer<CheckStatusOk, Throwable> callback;

    MaybeRecover(Node node, TxnId txnId, RoutingKey homeKey, Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, BiConsumer<CheckStatusOk, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.Route);
        this.homeKey = homeKey;
        this.knownStatus = knownStatus;
        this.knownPromised = knownPromised;
        this.knownPromisedHasBeenAccepted = knownPromiseHasBeenAccepted;
        this.callback = callback;
    }

    public static MaybeRecover maybeRecover(Node node, TxnId txnId, RoutingKey homeKey,
                                                     Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted,
                                                     BiConsumer<CheckStatusOk, Throwable> callback)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, homeKey, knownStatus, knownPromised, knownPromiseHasBeenAccepted, callback);
        maybeRecover.start();
        return maybeRecover;
    }

    @Override
    public void accept(Object unused, Throwable fail)
    {
        callback.accept(null, fail);
    }

    @Override
    boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return hasMadeProgress(ok);
    }

    public boolean hasMadeProgress(CheckStatusOk ok)
    {
        return ok != null && (ok.isCoordinating
                              || ok.status.compareTo(knownStatus) > 0
                              || ok.promised.compareTo(knownPromised) > 0
                              || (!knownPromisedHasBeenAccepted && knownStatus == Accepted && ok.accepted.equals(knownPromised)));
    }

    @Override
    void onDone(Done done, Throwable fail)
    {
        super.onDone(done, fail);
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (merged == null)
        {
            callback.accept(null, new Timeout(txnId, homeKey));
        }
        else
        {
            switch (merged.status)
            {
                default: throw new AssertionError();
                case NotWitnessed:
                    Invalidate.invalidate(node, txnId, someKeys, homeKey)
                              .addCallback(this);
                    break;
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    Preconditions.checkState(merged.route instanceof Route);
                    if (hasMadeProgress(merged)) callback.accept(merged, null);
                    else node.recover(txnId, (Route) merged.route).addCallback(this);
                    break;

                case Invalidated:
                    callback.accept(merged, null);
            }
        }
    }
}
