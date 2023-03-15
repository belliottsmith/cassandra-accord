package accord.primitives;

import accord.api.RoutingKey;

public class SyncPoint
{
    public final TxnId txnId;
    public final RoutingKey homeKey;
    public final Deps waitFor;
    public final boolean finishedAsync;

    public SyncPoint(TxnId txnId, RoutingKey homeKey, Deps waitFor, boolean finishedAsync)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.waitFor = waitFor;
        this.finishedAsync = finishedAsync;
    }
}
