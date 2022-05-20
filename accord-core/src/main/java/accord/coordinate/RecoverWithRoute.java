package accord.coordinate;

import java.util.Set;
import java.util.function.BiConsumer;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.Commit;
import accord.primitives.Ballot;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.coordinate.AnyReadCoordinator.Outcome.Accept;
import static accord.coordinate.Recover.Outcome.EXECUTED;
import static accord.coordinate.Recover.Outcome.INVALIDATED;

public class RecoverWithRoute extends AnyReadCoordinator<CheckStatusReply>
{
    static class NotWitnessed extends RuntimeException {}

    final Ballot ballot;
    final Route route;
    final BiConsumer<Recover.Outcome, Throwable> callback;

    CheckStatusOkFull merged;

    public RecoverWithRoute(Node node, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        this(node, node.topology().forEpoch(route, txnId.epoch), ballot, txnId, route, callback);
    }

    private RecoverWithRoute(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        super(node, topologies, txnId);
        this.ballot = ballot;
        this.route = route;
        this.callback = callback;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch;
    }

    public static RecoverWithRoute recover(Node node, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch), txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, topologies, ballot, txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch), ballot, txnId, route, callback);
    }

    public static RecoverWithRoute recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Route route, BiConsumer<Recover.Outcome, Throwable> callback)
    {
        RecoverWithRoute recover = new RecoverWithRoute(node, topologies, ballot, txnId, route, callback);
        recover.start(topologies.nodes());
        return recover;
    }

    @Override
    void contact(Set<Id> nodes)
    {
        node.send(nodes, to -> new CheckStatus(to, tracker.topologies(), txnId, route, IncludeInfo.All), this);
    }

    @Override
    Outcome process(Id from, CheckStatusReply response)
    {
        if (!response.isOk())
        {
            callback.accept(null, new IllegalStateException());
            return Outcome.Abort;
        }

        CheckStatusOkFull ok = (CheckStatusOkFull) response;
        if (ok.txn == null)
            throw new NotWitnessed();

        //  TODO (now): validate the new response covers the full owned range of the replica
        if (merged == null) merged = ok;
        else merged = (CheckStatusOkFull) merged.merge(ok);
        return Accept;
    }

    @Override
    void onSuccess()
    {
        if (merged.homeKey == null)
            throw new IllegalStateException();
        if (merged.routingKeys == null)
            throw new IllegalStateException();

        Route route = merged.routingKeys.toRoute(merged.homeKey);
        switch (merged.status)
        {
            case NotWitnessed:
                Invalidate.invalidate(node, txnId, route, route.homeKey)
                          .addCallback(callback);
                break;
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case Committed:
            case ReadyToExecute:
                Txn txn = merged.txn.reconstitute(route);
                Recover.recover(node, txnId, txn, route, callback);
                break;
            case Executed:
            case Applied:
                // TODO (now): we should persistAndCommit even with a PartialTxn that does not cover the whole range
                //             to do this we need to support Command.commit with txn that only covers executeAt
                Persist.persistAndCommit(node, txnId, route, merged.executeAt, merged.deps.reconstitute(route), merged.writes, merged.result);
                callback.accept(EXECUTED, null);
                break;
            case Invalidated:
                Commit.Invalidate.commitInvalidate(node, txnId, route, merged.executeAt);
                callback.accept(INVALIDATED, null);
        }
    }

    @Override
    void onFailure(Throwable fail)
    {
        callback.accept(null, fail);
    }
}
