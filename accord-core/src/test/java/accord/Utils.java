/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord;

import accord.api.Agent;
import accord.api.ConfigurationService;
import accord.api.DataStore;
import accord.api.MessageSink;
import accord.api.ProgressLog;
import accord.api.Scheduler;
import accord.api.TestableConfigurationService;
import accord.api.TopologySorter;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.local.CommandStores;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.local.Node;
import accord.impl.mock.MockStore;
import accord.primitives.Ranges;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.primitives.Txn;
import accord.primitives.Keys;
import accord.utils.DefaultRandom;
import accord.utils.EpochFunction;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.ThreadPoolScheduler;

import com.google.common.collect.Sets;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static accord.utils.async.AsyncChains.awaitUninterruptibly;

public class Utils
{
    public static Node.Id id(int i)
    {
        return new Node.Id(i);
    }

    public static List<Node.Id> ids(int num)
    {
        List<Node.Id> rlist = new ArrayList<>(num);
        for (int i=0; i<num; i++)
        {
            rlist.add(id(i+1));
        }
        return rlist;
    }

    public static List<Node.Id> ids(int first, int last)
    {
        Invariants.checkArgument(last >= first);
        List<Node.Id> rlist = new ArrayList<>(last - first + 1);
        for (int i=first; i<=last; i++)
            rlist.add(id(i));

        return rlist;
    }

    public static List<Node.Id> idList(int... ids)
    {
        List<Node.Id> list = new ArrayList<>(ids.length);
        for (int i : ids)
            list.add(new Node.Id(i));
        return list;
    }

    public static Set<Node.Id> idSet(int... ids)
    {
        Set<Node.Id> set = Sets.newHashSetWithExpectedSize(ids.length);
        for (int i : ids)
            set.add(new Node.Id(i));
        return set;
    }

    public static Ranges ranges(Range... ranges)
    {
        return Ranges.of(ranges);
    }

    public static Txn writeTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, MockStore.update(keys));
    }

    public static Txn writeTxn(Ranges ranges)
    {
        return new Txn.InMemory(ranges, MockStore.read(ranges), MockStore.QUERY, MockStore.update(ranges));
    }

    public static Txn readTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY);
    }

    public static Shard shard(Range range, List<Node.Id> nodes, Set<Node.Id> fastPath)
    {
        return new Shard(range, nodes, fastPath);
    }

    public static Topology topology(long epoch, Shard... shards)
    {
        return new Topology(epoch, shards);
    }

    public static Topology topology(Shard... shards)
    {
        return topology(1, shards);
    }

    public static Topologies topologies(Topology... topologies)
    {
        return new Topologies.Multi(SizeOfIntersectionSorter.SUPPLIER, topologies);
    }

    public static Node createNode(Node.Id nodeId, Topology topology, MessageSink messageSink, MockCluster.Clock clock)
    {
        Node node = new NodeBuilder(nodeId)
                .withMessageSink(messageSink)
                .withClock(clock)
                .buildAndStart();
        ((TestableConfigurationService) node.configService()).reportTopology(topology);
        return node;
    }

    public static class NodeBuilder
    {
        final Node.Id id;
        MessageSink messageSink = Mockito.mock(MessageSink.class);
        ConfigurationService configService;
        LongSupplier nowSupplier = new MockCluster.Clock(100);
        Supplier<DataStore> dataSupplier;
        {
            MockStore store = new MockStore();
            dataSupplier = () -> store;
        }
        ShardDistributor shardDistributor = new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter());
        Agent agent = new TestAgent();
        RandomSource random = new DefaultRandom(42);
        Scheduler scheduler = new ThreadPoolScheduler();
        TopologySorter.Supplier topologySorter = SizeOfIntersectionSorter.SUPPLIER;
        Function<Node, ProgressLog.Factory> progressLogFactory = SimpleProgressLog::new;
        CommandStores.Factory factory = InMemoryCommandStores.Synchronized::new;
        List<Topology> topologies = Collections.emptyList();

        public NodeBuilder(Node.Id id)
        {
            this.id = id;
        }

        public NodeBuilder withTopologies(Topology... topologies)
        {
            this.topologies = Arrays.asList(topologies);
            this.topologies.sort(Comparator.comparingLong(Topology::epoch));
            return this;
        }

        public NodeBuilder withProgressLog(ProgressLog.Factory factory)
        {
            return withProgressLog((Function<Node, ProgressLog.Factory>) ignore -> factory);
        }

        public NodeBuilder withProgressLog(Function<Node, ProgressLog.Factory> fn)
        {
            this.progressLogFactory = fn;
            return this;
        }

        public NodeBuilder withMessageSink(MessageSink messageSink)
        {
            this.messageSink = Objects.requireNonNull(messageSink);
            return this;
        }

        public NodeBuilder withClock(LongSupplier clock)
        {
            this.nowSupplier = clock;
            return this;
        }

        public NodeBuilder withShardDistributor(ShardDistributor shardDistributor)
        {
            this.shardDistributor = shardDistributor;
            return this;
        }

        public <T> NodeBuilder withShardDistributorFromSplitter(Function<Ranges, ? extends ShardDistributor.EvenSplit.Splitter<T>> splitter)
        {
            return withShardDistributor(new ShardDistributor.EvenSplit(8, splitter));
        }

        public Node build()
        {
            ConfigurationService configService = this.configService;
            if (configService == null)
                configService = new MockConfigurationService(messageSink, EpochFunction.noop());
            return new Node(id, messageSink, configService, nowSupplier, dataSupplier, shardDistributor, agent, random, scheduler, topologySorter, progressLogFactory, factory);
        }

        public Node buildAndStart()
        {
            Node node = build();
            awaitUninterruptibly(node.start());
            for (Topology t : topologies)
                ((TestableConfigurationService) node.configService()).reportTopology(t);
            return node;
        }
    }
}
