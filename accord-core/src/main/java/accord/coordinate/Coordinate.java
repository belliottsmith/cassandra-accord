package accord.coordinate;

import accord.api.ConfigurationService;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.local.Node;
import accord.api.Result;
import accord.local.Node;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

public class Coordinate
{
    private static Future<Result> fetchEpochOrExecute(Node node, Agreed agreed)
    {
        return node.withEpoch(agreed.executeAt.epoch, () -> Execute.execute(node, agreed));
    }

    private static Future<Result> andThenExecute(Node node, Future<Agreed> agree)
    {
        return agree.flatMap(agreed -> fetchEpochOrExecute(node, agreed));
    }

    public static Future<Result> execute(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        Preconditions.checkArgument(node.isReplicaOf(txnId, homeKey));
        // TODO (now): invoke Execute from Coordinate instead of wrapping this
        return andThenExecute(node, Agree.agree(node, txnId, txn, homeKey));
    }

    public static Future<Result> recover(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        // TODO (now): invoke Execute from Recover instead of wrapping this
        return andThenExecute(node, Recover.recover(node, new Ballot(node.uniqueNow()), txnId, txn, homeKey));
    }
}
