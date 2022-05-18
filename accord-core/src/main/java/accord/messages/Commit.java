package accord.messages;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.primitives.TxnId;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData
{
    public final @Nullable PartialTxn partialTxn;
    public final PartialDeps deps;
    public final boolean read;

    private transient Defer defer;

    public Commit(Id to, Topologies topologies, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, boolean read)
    {
        super(to, topologies, txnId, route, executeAt);
        this.deps = deps.slice(scope.covering);
        PartialTxn partialTxn = null;
        if (executeAt.epoch != txnId.epoch)
        {
            KeyRanges executeRanges = topologies.forEpoch(executeAt.epoch).rangesForNode(to);
            KeyRanges commitRanges = topologies.forEpoch(executeAt.epoch).rangesForNode(to);
            KeyRanges extraRanges = executeRanges.difference(commitRanges);
            if (!extraRanges.isEmpty()) partialTxn = txn.slice(extraRanges, commitRanges.contains(route.homeKey));
        }
        this.partialTxn = partialTxn;
        this.read = read;
    }

    // TODO (now): accept Topology not Topologies
    // TODO: do not commit if we're already ready to execute (requires extra info in Accept responses)
    public static void commitAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, Set<Id> readSet, Callback<ReadReply> callback)
    {
        for (Node.Id to : executeTopologies.nodes())
        {
            boolean read = readSet.contains(to);
            Commit send = new Commit(to, executeTopologies, txnId, txn, route, executeAt, deps, read);
            if (read) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, executeTopologies, txnId, txn, route, executeAt, deps);
        }
    }

    public static void commit(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps)
    {
        Topologies commitTo = node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch);
        for (Node.Id to : commitTo.nodes())
        {
            Commit send = new Commit(to, commitTo, txnId, txn, route, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Set<Id> doNotCommitTo, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps)
    {
        for (Node.Id to : commitTo.nodes())
        {
            if (doNotCommitTo.contains(to))
                continue;

            Commit send = new Commit(to, commitTo, txnId, txn, route, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Topologies appliedTo, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps)
    {
        // TODO (now): if we switch to Topology rather than Topologies we can avoid sending commits to nodes that Apply the same
        commit(node, commitTo, Collections.emptySet(), txnId, txn, route, executeAt, deps);
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope);
        node.forEachLocal(scope(), txnId.epoch, executeAt.epoch,
                          instance -> instance.command(txnId).commit(scope.homeKey, progressKey, executeAt, deps, partialTxn));

        if (read)
            super.process(node, from, replyContext);
    }

    @Override
    public MessageType type()
    {
        return MessageType.COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + deps +
               ", read: " + read +
               '}';
    }

    // TODO: should use RoutingKeys or PartialRoute
    public static class Invalidate extends TxnRequest
    {
        final TxnId txnId;

        public Invalidate(Id to, Topologies topologies, TxnId txnId, AbstractRoute route)
        {
            super(to, topologies, route);
            this.txnId = txnId;
        }

        public static void commitInvalidate(Node node, TxnId txnId, AbstractRoute someRoute, Timestamp until)
        {
            Topologies commitTo = node.topology().preciseEpochs(someRoute, txnId.epoch, until.epoch);
            commitInvalidate(node, commitTo, txnId, someRoute);
        }

        public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, AbstractRoute someRoute)
        {
            for (Node.Id to : commitTo.nodes())
            {
                Invalidate send = new Invalidate(to, commitTo, txnId, someRoute);
                node.send(to, send);
            }
        }

        public void process(Node node, Id from, ReplyContext replyContext)
        {
            node.forEachLocal(scope(), txnId.epoch, instance -> instance.command(txnId).commitInvalidate());
        }

        @Override
        public MessageType type()
        {
            return MessageType.COMMIT_REQ;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
