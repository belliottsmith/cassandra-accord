package accord.impl.mock;

import accord.coordinate.FetchData;
import accord.coordinate.FetchData.Outcome;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.*;
import accord.messages.*;
import accord.primitives.AbstractRoute;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies.Single;
import accord.topology.Topology;

import org.apache.cassandra.utils.concurrent.AsyncPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static accord.impl.mock.MockCluster.configService;
import static accord.messages.SimpleReply.Ok;

public class EpochSync implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger(EpochSync.class);

    private final Iterable<Node> cluster;
    private final long syncEpoch;
    private final long nextEpoch;

    public EpochSync(Iterable<Node> cluster, long syncEpoch)
    {
        this.cluster = cluster;
        this.syncEpoch = syncEpoch;
        this.nextEpoch = syncEpoch + 1;
    }

    private static class SyncCommitted implements Request
    {
        private final TxnId txnId;
        private AbstractRoute route;
        private final Timestamp executeAt;
        private final long epoch;

        public SyncCommitted(Command command, long epoch)
        {
            this.epoch = epoch;
            this.txnId = command.txnId();
            this.route = command.route();
            this.executeAt = command.executeAt();
        }

        void update(Command command)
        {
            route = AbstractRoute.merge(route, command.route());
        }

        @Override
        public void process(Node node, Node.Id from, ReplyContext replyContext)
        {
            FetchData.fetchCommitted(node, txnId, route, executeAt, epoch, (outcome, fail) -> {
                if (fail != null) process(node, from, replyContext);
                else if (outcome != Outcome.Success) throw new IllegalStateException();
                else node.reply(from, replyContext, Ok);
            });
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class CommandSync extends AsyncPromise<Void> implements Callback<SimpleReply>
    {
        private final QuorumTracker tracker;

        public CommandSync(Node node, AbstractRoute route, SyncCommitted message, Topology topology)
        {
            this.tracker = new QuorumTracker(new Single(topology.forKeys(route), false));
            node.send(tracker.nodes(), message, this);
        }

        @Override
        public synchronized void onSuccess(Node.Id from, SimpleReply reply)
        {
            tracker.success(from);
            if (tracker.hasReachedQuorum())
                setSuccess(null);
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable failure)
        {
            tracker.failure(from);
            if (tracker.hasFailed())
                tryFailure(failure);
        }

        @Override
        public void onCallbackFailure(Throwable failure)
        {
            tryFailure(failure);
        }

        public static void sync(Node node, AbstractRoute route, SyncCommitted message, Topology topology)
        {
            try
            {
                new CommandSync(node, route, message, topology).get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SyncComplete implements Request
    {
        private final long epoch;

        public SyncComplete(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            configService(on).reportSyncComplete(from, epoch);
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class NodeSync implements Runnable
    {
        private final Node node;
        private final Topology syncTopology;
        private final Topology nextTopology;

        public NodeSync(Node node)
        {
            this.node = node;
            syncTopology = node.configService().getTopologyForEpoch(syncEpoch).forNode(node.id());
            nextTopology = node.configService().getTopologyForEpoch(nextEpoch);
        }

        @Override
        public void run()
        {
            Map<TxnId, SyncCommitted> syncMessages = new ConcurrentHashMap<>();
            Consumer<Command> commandConsumer = command -> syncMessages.computeIfAbsent(command.txnId(), id -> new SyncCommitted(command, syncEpoch))
                                                               .update(command);

            node.forEachLocal(commandStore -> commandStore.forCommittedInEpoch(syncTopology.ranges(), syncEpoch, commandConsumer));

            for (SyncCommitted send : syncMessages.values())
                CommandSync.sync(node, send.route, send, nextTopology);

            // TODO (now): this needs to be made reliable; we need an Ack and retry
            SyncComplete syncComplete = new SyncComplete(syncEpoch);
            node.send(nextTopology.nodes(), syncComplete);
        }
    }

    private void syncNode(Node node)
    {
        new NodeSync(node).run();
    }

    @Override
    public void run()
    {
        logger.info("Beginning sync of epoch: {}", syncEpoch);
        cluster.forEach(this::syncNode);
    }

    public static void sync(MockCluster cluster, long epoch)
    {
        new EpochSync(cluster, epoch).run();
    }
}
