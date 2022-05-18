package accord.primitives;

import java.util.Arrays;
import java.util.NoSuchElementException;

import accord.api.Key;
import accord.api.RoutingKey;

public class PartialDeps extends Deps
{
    public static final PartialDeps NONE = new PartialDeps(KeyRanges.EMPTY, Deps.NONE.keys, Deps.NONE.txnIds, Deps.NONE.keyToTxnId);

    public final KeyRanges covering;

    PartialDeps(KeyRanges covering, Keys keys, TxnId[] txnIds, int[] keyToTxnId)
    {
        super(keys, txnIds, keyToTxnId);
        this.covering = covering;
    }

    public boolean covers(KeyRanges ranges)
    {
        return covering.contains(ranges);
    }

    public PartialDeps slice(KeyRanges ranges)
    {

    }

    public PartialRoute someRoute(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            throw new NoSuchElementException();

        ensureTxnIdToKey();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdToKey[txnIdIndex - 1];
        int end = txnIdToKey[txnIdIndex];
        RoutingKey[] result = new Key[end - start];
        for (int i = start ; i < end ; ++i)
        {
            result[i - start] = keys.get(txnIdToKey[i]);
            if (i != start && result[i - (1 + start)] == result[i - start])
                throw new AssertionError();
        }
        return new PartialRoute(result);
    }

    public PartialDeps with(PartialDeps that)
    {
        Deps merged = with((Deps) that);
        return new PartialDeps(covering.union(that.covering), merged.keys, merged.txnIds, merged.keyToTxnId);
    }
}
