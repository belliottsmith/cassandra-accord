package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class RecoverWithHomeKey extends CheckShards implements BiConsumer<Object, Throwable>
{
    final RoutingKey homeKey;
    final BiConsumer<Recover.Outcome, Throwable> callback;

    RecoverWithHomeKey(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.Route);
        this.homeKey = homeKey;
        this.callback = callback;
    }

    public static RecoverWithHomeKey recover(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        RecoverWithHomeKey maybeRecover = new RecoverWithHomeKey(node, txnId, homeKey, callback);
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
        return ok.route != null;
    }

    @Override
    void onDone(Done done, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (merged == null || merged.route == null)
        {
            switch (done)
            {
                default: throw new IllegalStateException();
                case Exhausted:
                    callback.accept(null, new Timeout(txnId, homeKey));
                    return;
                case Success:
                    callback.accept(null, new IllegalStateException());
                    return;
                case ReachedQuorum:
                    Invalidate.invalidate(node, txnId, someKeys, homeKey)
                              .addCallback(callback);
            }
        }
        else
        {
            // save routingKeys
            node.ifLocalSince(merged.route.homeKey, txnId, instance -> {
                instance.command(txnId).routingKeys(merged.route);
                return null;
            });

            // start recovery
            node.recover(txnId, merged.route).addCallback(callback);
        }
    }
}
