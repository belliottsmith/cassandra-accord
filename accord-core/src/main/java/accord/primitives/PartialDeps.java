package accord.primitives;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(KeyRanges.EMPTY, Deps.NONE.keys, Deps.NONE.txnIds, Deps.NONE.keyToTxnId);

    final KeyRanges covering;

    PartialDeps(KeyRanges covering, Keys keys, TxnId[] txnIds, int[] keyToTxnId)
    {
        super(keys, txnIds, keyToTxnId);
        this.covering = covering;
    }

    public boolean covers(KeyRanges ranges)
    {
        return covering.contains(ranges);
    }

    public PartialDeps with(PartialDeps that)
    {
        Deps merged = with((Deps) that);
        return new PartialDeps(covering.union(that.covering), merged.keys, merged.txnIds, merged.keyToTxnId);
    }
}
