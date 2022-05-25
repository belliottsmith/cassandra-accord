package accord.messages;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.KeyRanges;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.topology.Topologies;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.primitives.TxnId;

import static accord.local.Status.PreAccepted;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData
{
    public final @Nullable PartialTxn partialTxn;
    public final PartialDeps partialDeps;
    public final @Nullable Route route;
    public final boolean read;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private transient Defer defer;

    public enum Kind { Minimal, Maximal }

    public Commit(Kind kind, Id to, Topologies topologies, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, boolean read)
    {
        super(to, topologies, txnId, route, executeAt);

        Route sendRoute = null;
        PartialTxn partialTxn = null;
        KeyRanges commitRanges = topologies.forEpoch(txnId.epoch).rangesForNode(to);
        if (kind == Kind.Maximal)
        {
            boolean isHome = commitRanges.contains(route.homeKey);
            partialTxn = txn.slice(scope.covering, isHome);
            if (isHome)
                sendRoute = route;
        }
        else if (executeAt.epoch != txnId.epoch)
        {
            KeyRanges executeRanges = topologies.forEpoch(executeAt.epoch).rangesForNode(to);
            KeyRanges extraRanges = executeRanges.difference(commitRanges);
            if (!extraRanges.isEmpty())
                partialTxn = txn.slice(extraRanges, commitRanges.contains(route.homeKey));
        }

        this.partialTxn = partialTxn;
        this.partialDeps = deps.slice(scope.covering);
        this.route = sendRoute;
        this.read = read;
    }

    // TODO (now): accept Topology not Topologies
    // TODO: do not commit if we're already ready to execute (requires extra info in Accept responses)
    public static void commitAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, Set<Id> readSet, Callback<ReadReply> callback)
    {
        for (Node.Id to : executeTopologies.nodes())
        {
            boolean read = readSet.contains(to);
            Commit send = new Commit(Kind.Minimal, to, executeTopologies, txnId, txn, route, executeAt, deps, read);
            if (read) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (txnId.epoch != executeAt.epoch)
        {
            Topologies earlierTopologies = node.topology().forEpochRange(route, txnId.epoch, executeAt.epoch - 1);
            Commit.commit(node, earlierTopologies, executeTopologies, txnId, txn, route, executeAt, deps);
        }
    }

    public static void commit(Node node, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps)
    {
        Topologies commitTo = node.topology().forEpochRange(route, txnId.epoch, executeAt.epoch);
        for (Node.Id to : commitTo.nodes())
        {
            Commit send = new Commit(Kind.Minimal, to, commitTo, txnId, txn, route, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Set<Id> doNotCommitTo, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps)
    {
        for (Node.Id to : commitTo.nodes())
        {
            if (doNotCommitTo.contains(to))
                continue;

            Commit send = new Commit(Kind.Minimal, to, commitTo, txnId, txn, route, executeAt, deps, false);
            node.send(to, send);
        }
    }

    public static void commit(Node node, Topologies commitTo, Topologies appliedTo, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps)
    {
        // TODO (now): avoid sending commits to nodes that Apply the same
        commit(node, commitTo, Collections.emptySet(), txnId, txn, route, executeAt, deps);
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope);
        ReadNack reply = node.mapReduceLocal(scope(), txnId.epoch, executeAt.epoch, instance -> {
            Command command = instance.command(txnId);
            switch (command.commit(route != null ? route : scope, progressKey, executeAt, partialDeps, partialTxn))
            {
                default:
                case Success:
                case Redundant:
                    return null;

                case Insufficient:
                    Preconditions.checkState(!command.hasBeen(PreAccepted));
                    if (defer == null)
                        defer = new Defer(PreAccepted, this, node, from, replyContext);
                    defer.add(command, instance);
                    return ReadNack.NotCommitted;
            }
        }, (r1, r2) -> r1 != null ? r1 : r2);

        if (reply != null)
            node.reply(from, replyContext, reply);
        else if (read)
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
               ", deps: " + partialDeps +
               ", read: " + read +
               '}';
    }

    public static class Invalidate implements EpochRequest
    {
        public static void commitInvalidate(Node node, TxnId txnId, RoutingKeys someKeys, Timestamp until)
        {
            Topologies commitTo = node.topology().forEpochRange(someKeys, txnId.epoch, until.epoch);
            commitInvalidate(node, commitTo, txnId, someKeys);
        }

        public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, RoutingKeys someKeys)
        {
            for (Node.Id to : commitTo.nodes())
            {
                Invalidate send = new Invalidate(to, commitTo, txnId, someKeys);
                node.send(to, send);
            }
        }

        final TxnId txnId;
        final RoutingKeys scope;
        final long waitForEpoch;
        final long invalidateUntilEpoch;

        public Invalidate(Id to, Topologies topologies, TxnId txnId, RoutingKeys someKeys)
        {
            this.txnId = txnId;
            this.invalidateUntilEpoch = topologies.currentEpoch();
            int latestRelevantIndex = latestRelevantEpochIndex(to, topologies, someKeys);
            this.waitForEpoch = computeWaitForEpoch(to, topologies, latestRelevantIndex);
            this.scope = computeScope(to, topologies, someKeys, latestRelevantIndex, RoutingKeys::slice, RoutingKeys::union);
        }

        @Override
        public long waitForEpoch()
        {
            return waitForEpoch;
        }

        public void process(Node node, Id from, ReplyContext replyContext)
        {
            node.forEachLocal(scope, txnId.epoch, invalidateUntilEpoch, instance -> instance.command(txnId).commitInvalidate());
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
