package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

/**
 * Find the Route of a known (txnId, homeKey) pair
 */
public class FindRoute extends CheckShards
{
    final BiConsumer<Route, Throwable> callback;
    FindRoute(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Route, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.No);
        this.callback = callback;
    }

    public static FindRoute findRoute(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Route, Throwable> callback)
    {
        FindRoute findRoute = new FindRoute(node, txnId, homeKey, callback);
        findRoute.start();
        return findRoute;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ok.route instanceof Route;
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
            super.onDone(done, failure);
            callback.accept(merged == null ? null : (Route)merged.route, null);
        }
    }
}
