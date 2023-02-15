package accord.primitives;

import accord.api.RoutingKey;

public class SyncPoint
{
    public final TxnId txnId;
    public final RoutingKey homeKey;
    public final Deps waitFor;

    public SyncPoint(TxnId txnId, RoutingKey homeKey, Deps waitFor)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.waitFor = waitFor;
    }
}
