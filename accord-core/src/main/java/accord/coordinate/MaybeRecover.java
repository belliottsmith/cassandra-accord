package accord.coordinate;

import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.coordinate.Recover.Outcome;
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
public class MaybeRecover extends CheckShards implements BiConsumer<Outcome, Throwable>
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

    public static void maybeRecover(Node node, TxnId txnId, RoutingKey homeKey,
                                                     Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted,
                                                     BiConsumer<CheckStatusOk, Throwable> callback)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, homeKey, knownStatus, knownPromised, knownPromiseHasBeenAccepted, callback);
        maybeRecover.start();
    }

    @Override
    public void accept(Outcome outcome, Throwable fail)
    {
        if (fail != null) callback.accept(null, fail);
        else switch (outcome)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Invalidated:
                callback.accept(null, null);
                break;
            case Preempted:
                callback.accept(null, new Preempted(txnId, homeKey));
        }
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
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
    protected void onDone(Done done, Throwable fail)
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
                case AcceptedInvalidate:
                    if (!(merged.route instanceof Route))
                    {
                        Invalidate.invalidateIfNotWitnessed(node, txnId, someKeys, homeKey)
                                  .addCallback(this);
                        break;
                    }
                case PreAccepted:
                case Accepted:
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
