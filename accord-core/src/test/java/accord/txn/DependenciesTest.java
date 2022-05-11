package accord.txn;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import accord.api.Key;
import accord.impl.IntHashKey;
import accord.local.Node.Id;
import accord.primitives.Dependencies;
import accord.primitives.Keys;
import accord.primitives.TxnId;

public class DependenciesTest
{
    @Test
    public void testRandom()
    {

    }

    private static void testRandom(long seed, int uniqueTxnId, int epochRange, int realRange, int logicalRange, int nodeRange,
                                   int uniqueKeys, int keyRange, int totalCount
                                   )
    {
        Random random = new Random(seed);
        Keys keys; {
        TreeSet<Key> tmp = new TreeSet<>();
            while (tmp.size() < uniqueKeys)
                tmp.add(IntHashKey.key(random.nextInt(keyRange)));
            keys = new Keys(tmp);
        }

        TreeSet<TxnId> txnIds = new TreeSet<>();
        while (txnIds.size() < uniqueTxnId)
            txnIds.add(new TxnId(random.nextInt(epochRange), random.nextInt(realRange), random.nextInt(logicalRange), new Id(random.nextInt(nodeRange))));

        Dependencies.Builder builder = Dependencies.builder(keys);
        Map<Key, Set<TxnId>> keyToTxnId = new HashMap<>();
//        for (int i = 0 ; i < totalCountx/)
    }

}
