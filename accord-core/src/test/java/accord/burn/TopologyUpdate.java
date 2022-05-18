package accord.burn;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.TestableConfigurationService;
import accord.local.Command;
import accord.local.Node;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.MessageTask;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class TopologyUpdate
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyUpdate.class);

    private static final Set<Long> pendingTopologies = Sets.newConcurrentHashSet();

    private static class CommandSync
    {
        private final Status status;
        private final TxnId txnId;
        private final PartialTxn partialTxn;
        private final RoutingKey homeKey;
        private final Timestamp executeAt;
        private final long epoch;

        private final PartialDeps partialDeps;
        private final Writes writes;
        private final Result result;

        public CommandSync(Command command, long epoch)
        {
            // TODO (now): AcceptInvalidate and CommitInvalidate can leave these fields null
            Preconditions.checkArgument(command.hasBeen(Status.PreAccepted));
            this.txnId = command.txnId();
            this.partialTxn = command.partialTxn();
            this.homeKey = command.homeKey();
            this.status = command.status();
            this.executeAt = command.executeAt();
            this.partialDeps = command.savedPartialDeps();
            this.writes = command.writes();
            this.result = command.result();
            this.epoch = epoch;
        }

        public void process(Node node)
        {
            if (!node.topology().hasEpoch(txnId.epoch))
            {
                node.configService().fetchTopologyForEpoch(txnId.epoch);
                node.topology().awaitEpoch(txnId.epoch).addListener(() -> process(node));
                return;
            }

            // TODO: can skip the homeKey if it's not a participating key in the transaction
            node.forEachLocalSince(partialTxn.keys, epoch, commandStore -> {
                commandStore.command(txnId).propagate(epoch, status, partialTxn, partialDeps, homeKey, executeAt, writes, result);
            });
        }
    }

    public static <T> BiConsumer<T, Throwable> dieOnException()
    {
        return (result, throwable) -> {
            if (throwable != null)
            {
                logger.error("", throwable);
                System.exit(1);
            }
        };
    }

    public static <T> Future<T> dieExceptionally(Future<T> stage)
    {
        return stage.addCallback(dieOnException());
    }

    public static MessageTask notify(Node originator, Collection<Node.Id> cluster, Topology update)
    {
        pendingTopologies.add(update.epoch());
        return MessageTask.begin(originator, cluster, "TopologyNotify:" + update.epoch(), (node, from) -> {
            long nodeEpoch = node.topology().epoch();
            if (nodeEpoch + 1 < update.epoch())
                return false;
            ((TestableConfigurationService) node.configService()).reportTopology(update);
            return true;
        });
    }

    private static Collection<Node.Id> allNodesFor(Txn txn, Topology... topologies)
    {
        Set<Node.Id> result = new HashSet<>();
        for (Topology topology : topologies)
            result.addAll(topology.forKeys(txn.keys()).nodes());
        return result;
    }

    private static Stream<MessageTask> syncEpochCommands(Node node, long epoch, KeyRanges ranges, Function<CommandSync, Collection<Node.Id>> recipients, long forEpoch, boolean committedOnly)
    {
        Map<TxnId, CommandSync> syncMessages = new ConcurrentHashMap<>();
        Consumer<Command> commandConsumer = command -> syncMessages.put(command.txnId(), new CommandSync(command, epoch));
        if (committedOnly)
            node.forEachLocal(commandStore -> commandStore.forCommittedInEpoch(ranges, epoch, commandConsumer));
        else
            node.forEachLocal(commandStore -> commandStore.forEpochCommands(ranges, epoch, commandConsumer));
        return syncMessages.values().stream().map(cmd -> MessageTask.of(node, recipients.apply(cmd), "Sync:" + cmd.txnId + ':' + epoch + ':' + forEpoch, cmd::process));
    }

    private static final boolean COMMITTED_ONLY = true;

    /**
     * Syncs all replicated commands. Overkill, but useful for confirming issues in optimizedSync
     */
    private static Stream<MessageTask> thoroughSync(Node node, long syncEpoch)
    {
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> node.topology().withUnsyncEpochs(cmd.partialTxn.keys, syncEpoch).nodes();

        KeyRanges ranges = localTopology.ranges();
        Stream<MessageTask> messageStream = Stream.empty();
        for (long epoch=1; epoch<=syncEpoch; epoch++)
        {
            messageStream = Stream.concat(messageStream, syncEpochCommands(node, epoch, ranges, allNodes, syncEpoch, COMMITTED_ONLY));
        }
        return messageStream;
    }

    /**
     * Syncs all newly replicated commands when nodes are gaining ranges and the current epoch
     */
    private static Stream<MessageTask> optimizedSync(Node node, long syncEpoch)
    {
        long nextEpoch = syncEpoch + 1;
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Topology nextTopology = node.configService().getTopologyForEpoch(nextEpoch);
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> node.topology().withUnsyncEpochs(cmd.partialTxn.keys, syncEpoch).nodes();

        // backfill new replicas with operations from prior epochs
        Stream<MessageTask> messageStream = Stream.empty();
        for (Shard syncShard : localTopology)
        {
            for (Shard nextShard : nextTopology)
            {
                // do nothing if there's no change
                if (syncShard.range.equals(nextShard.range) && syncShard.nodeSet.equals(nextShard.nodeSet))
                    continue;

                KeyRange intersection = syncShard.range.intersection(nextShard.range);

                if (intersection == null)
                    continue;

                Set<Node.Id> newNodes = Sets.difference(nextShard.nodeSet, syncShard.nodeSet);

                if (newNodes.isEmpty())
                    continue;

                KeyRanges ranges = KeyRanges.single(intersection);
                for (long epoch=1; epoch<syncEpoch; epoch++)
                    messageStream = Stream.concat(messageStream, syncEpochCommands(node,
                                                                                   epoch,
                                                                                   ranges,
                                                                                   cmd -> newNodes,
                                                                                   syncEpoch, COMMITTED_ONLY));
            }
        }


        // update all current and future replicas with the contents of the sync epoch
        messageStream = Stream.concat(messageStream, syncEpochCommands(node,
                                                                       syncEpoch,
                                                                       localTopology.ranges(),
                                                                       allNodes,
                                                                       syncEpoch, COMMITTED_ONLY));
        return messageStream;
    }

    public static Future<Void> sync(Node node, long syncEpoch)
    {
        Stream<MessageTask> messageStream = optimizedSync(node, syncEpoch);

        Iterator<MessageTask> iter = messageStream.iterator();
        if (!iter.hasNext())
        {
            return ImmediateFuture.success(null);
        }

        MessageTask first = iter.next();
        MessageTask last = first;
        while (iter.hasNext())
        {
            MessageTask next = iter.next();
            last.addListener(next);
            last = next;
        }

        first.run();
        return dieExceptionally(last);
    }

    public static Future<Void> syncEpoch(Node originator, long epoch, Collection<Node.Id> cluster)
    {
        Future<Void> future = dieExceptionally(sync(originator, epoch)
                .flatMap(v -> MessageTask.apply(originator, cluster, "SyncComplete:" + epoch, (node, from) -> node.onEpochSyncComplete(originator.id(), epoch))));
        future.addCallback((unused, throwable) -> pendingTopologies.remove(epoch));
        return future;
    }

    public static int pendingTopologies()
    {
        return pendingTopologies.size();
    }
}
