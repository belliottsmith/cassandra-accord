package accord.messages;

import java.util.function.BiConsumer;

import java.util.Objects;

import javax.annotation.Nullable;

import accord.primitives.*;
import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.TxnRequest.WithUnsynced;
import accord.topology.Topologies;
import accord.local.Command;
import accord.primitives.Deps;
import accord.primitives.TxnId;

public class PreAccept extends WithUnsynced
{
    public final PartialTxn txn;
    public final @Nullable Route route; // ordinarily only set on home shard
    public final long maxEpoch;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, Route route)
    {
        super(to, topologies, txnId, route);
        this.txn = txn.slice(scope.covering, route.contains(route.homeKey));
        this.maxEpoch = topologies.currentEpoch();
        this.route = scope.contains(scope.homeKey) ? route : null;
    }

    @VisibleForTesting
    public PreAccept(PartialRoute scope, long epoch, TxnId txnId, PartialTxn txn, @Nullable Route route)
    {
        super(scope, epoch, txnId);
        this.txn = txn;
        this.maxEpoch = epoch;
        this.route = route;
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        // TODO: verify we handle all of the scope() keys
        RoutingKey progressKey = progressKey(node, scope.homeKey);
        node.reply(from, replyContext, node.mapReduceLocal(scope(), minEpoch, maxEpoch, instance -> {
            // note: this diverges from the paper, in that instead of waiting for JoinShard,
            //       we PreAccept to both old and new topologies and require quorums in both.
            //       This necessitates sending to ALL replicas of old topology, not only electorate (as fast path may be unreachable).
            Command command = instance.command(txnId);
            switch (command.preaccept(txn, route != null ? route : scope, progressKey))
            {
                default:
                case Insufficient:
                    throw new IllegalStateException();

                case Success:
                case Redundant:
                     return new PreAcceptOk(txnId, command.executeAt(), calculatePartialDeps(instance, txnId, txn.keys, txn.kind, txnId, instance.ranges().at(txnId.epoch)));

                case RejectedBallot:
                    return PreAcceptNack.INSTANCE;
            }
        }, (r1, r2) -> {
            if (!r1.isOk()) return r1;
            if (!r2.isOk()) return r2;
            PreAcceptOk ok1 = (PreAcceptOk) r1;
            PreAcceptOk ok2 = (PreAcceptOk) r2;
            PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
            Deps deps = ok1.deps.with(ok2.deps);
            if (deps == okMax.deps)
                return okMax;
            return new PreAcceptOk(txnId, okMax.witnessedAt, deps);
        }));
    }

    @Override
    public MessageType type()
    {
        return MessageType.PREACCEPT_REQ;
    }

    public interface PreAcceptReply extends Reply
    {
        @Override
        default MessageType type()
        {
            return MessageType.PREACCEPT_RSP;
        }

        boolean isOk();
    }

    public static class PreAcceptOk implements PreAcceptReply
    {
        public final TxnId txnId;
        public final Timestamp witnessedAt;
        public final Deps deps;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, Deps deps)
        {
            this.txnId = txnId;
            this.witnessedAt = witnessedAt;
            this.deps = deps;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreAcceptOk that = (PreAcceptOk) o;
            return witnessedAt.equals(that.witnessedAt) && deps.equals(that.deps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(witnessedAt, deps);
        }

        @Override
        public String toString()
        {
            return "PreAcceptOk{" +
                    "txnId:" + txnId +
                    ", witnessedAt:" + witnessedAt +
                    ", deps:" + deps +
                    '}';
        }
    }

    public static class PreAcceptNack implements PreAcceptReply
    {
        public static final PreAcceptNack INSTANCE = new PreAcceptNack();

        private PreAcceptNack() {}

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "PreAcceptNack{}";
        }
    }

    static Deps calculateDeps(CommandStore commandStore, TxnId txnId, Keys keys, Txn.Kind kindOfTxn, Timestamp executeAt)
    {
        try (Deps.OrderedBuilder builder = Deps.orderedBuilder(false);)
        {
            return calculateDeps(commandStore, txnId, keys, kindOfTxn, executeAt, builder);
        }
    }

    static PartialDeps calculatePartialDeps(CommandStore commandStore, TxnId txnId, Keys keys, Txn.Kind kindOfTxn, Timestamp executeAt, KeyRanges ranges)
    {
        try (PartialDeps.OrderedBuilder builder = PartialDeps.orderedBuilder(ranges, false);)
        {
            return calculateDeps(commandStore, txnId, keys, kindOfTxn, executeAt, builder);
        }
    }

    private static <T extends Deps> T calculateDeps(CommandStore commandStore, TxnId txnId, Keys keys, Txn.Kind kindOfTxn, Timestamp executeAt, Deps.AbstractOrderedBuilder<T> builder)
    {
        keys.forEach(key -> {
            CommandsForKey forKey = commandStore.maybeCommandsForKey(key);
            if (forKey == null)
                return;

            builder.nextKey(key);
            forKey.uncommitted.headMap(executeAt, false).forEach(conflicts(txnId, kindOfTxn.isWrite(), builder));
            forKey.committedByExecuteAt.headMap(executeAt, false).forEach(conflicts(txnId, kindOfTxn.isWrite(), builder));
        });

        return builder.build();
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", scope:" + scope +
               '}';
    }

    private static BiConsumer<Timestamp, Command> conflicts(TxnId txnId, boolean isWrite, Deps.AbstractOrderedBuilder<?> builder)
    {
        return (ts, command) -> {
            if (!txnId.equals(command.txnId()) && (isWrite || command.partialTxn().isWrite()))
                builder.add(command.txnId());
        };
    }
}
