package accord.impl.basic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import accord.api.MessageSink;
import accord.burn.BurnTestConfigurationService;
import accord.burn.TopologyUpdates;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntHashKey;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TopologyFactory;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.messages.Reply;
import accord.messages.Request;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;

public class SimpleCluster extends Cluster
{
    final Node.Id[] ids;
    final List<Throwable> failures = new ArrayList<>();
    final List<Packet> clientResponses;
    long now;
    Consumer<Reply> replies;

    private SimpleCluster(Node.Id[] ids, Supplier<PendingQueue> queueSupplier, Function<Node.Id, Node> lookup)
    {
        this(ids, queueSupplier, lookup, new ArrayList<>());
    }

    private SimpleCluster(Node.Id[] ids, Supplier<PendingQueue> queueSupplier, Function<Node.Id, Node> lookup, List<Packet> clientResponses)
    {
        super(queueSupplier, lookup, clientResponses::add);
        this.clientResponses = clientResponses;
        this.ids = ids;
    }

    public void setNow(long now)
    {
        this.now = now;
    }

    public static SimpleCluster create(int count)
    {
        return create(count, DefaultRandom::new);
    }

    public static SimpleCluster create(int count, Supplier<RandomSource> randomSupplier)
    {
        Node.Id[] nodes = new Node.Id[count];
        for (int i = 1 ; i <= nodes.length ; ++i)
            nodes[i - 1] = new Node.Id(i);

        TopologyFactory topologyFactory = new TopologyFactory(count, IntHashKey.ranges(1));
        TopologyUpdates topologyUpdates = new TopologyUpdates();
        Topology topology = topologyFactory.toTopology(nodes);

        Map<Node.Id, Node> lookup = new LinkedHashMap<>();
        SimpleCluster result = new SimpleCluster(nodes, new RandomDelayQueue.Factory(randomSupplier.get()), lookup::get);
        for (Node.Id node : nodes)
        {
            MessageSink messageSink = result.create(node, randomSupplier.get());
            BurnTestConfigurationService configService = new BurnTestConfigurationService(node, messageSink, randomSupplier, topology, lookup::get, topologyUpdates);
            lookup.put(node, new Node(node, messageSink, configService, () -> result.now,
                                      () -> new ListStore(node), new ShardDistributor.EvenSplit<>(8, ignore -> new IntHashKey.Splitter()),
                                      new ListAgent(30L, result.failures::add),
                                      randomSupplier.get(), result, SizeOfIntersectionSorter.SUPPLIER,
                                      SimpleProgressLog::new, InMemoryCommandStores.Synchronized::new));
        }

        return result;
    }

    public Node get(int i)
    {
        return lookup.apply(ids[i - 1]);
    }

    public void send(int from, int to, long messageId, Request send)
    {
        processNext(new Packet(ids[from - 1], ids[to - 1], messageId, send));
    }

    public void send(int from, int to, long messageId, Request send, Consumer<Reply> replies)
    {
        this.replies = replies;
        try
        {
            processNext(new Packet(ids[from - 1], ids[to - 1], messageId, send));
        }
        finally
        {
            this.replies = null;
        }
    }

    public void reply(int from, int to, long replyId, Reply send)
    {
        processNext(new Packet(ids[from - 1], ids[to - 1], replyId, send));
    }

    @Override
    void add(Node.Id from, Node.Id to, long replyId, Reply send)
    {
        if (replies != null) replies.accept(send);
        else super.add(from, to, replyId, send);
    }
}
