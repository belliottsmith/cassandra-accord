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

package accord.local;

import accord.api.Key;
import accord.api.TestableConfigurationService;
import accord.burn.random.FrequentLargeRange;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.TopologyMismatch;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.impl.basic.Cluster;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingRunnable;
import accord.impl.basic.PropagatingPendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.impl.list.ListAgent;
import accord.messages.MessageType;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Timestamp;
import accord.topology.TopologyUtils;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.Utils.listWriteTxn;
import static accord.utils.Property.qt;
import static accord.utils.Utils.addAll;

class CommandsTest
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsTest.class);

    @Test
    void removeRangesValidate()
    {
        Gen<List<Node.Id>> nodeGen = Gens.lists(AccordGens.nodes()).ofSizeBetween(1, 10);
        qt().check(rs -> {
            List<Node.Id> nodes = nodeGen.next(rs);
            nodes.sort(Comparator.naturalOrder());
            logger.info("Running with {} nodes", nodes.size());
            int rf = Math.min(3, nodes.size());
            Range[] prefix0 = PrefixedIntHashKey.ranges(0, nodes.size());
            Range[] prefix1 = PrefixedIntHashKey.ranges(1, nodes.size());
            Range[] allRanges = addAll(prefix0, prefix1);

            Topology initialTopology = TopologyUtils.topology(1, nodes, Ranges.of(allRanges), rf);
            Topology updatedTopology = TopologyUtils.topology(2, nodes, Ranges.of(prefix0), rf); // drop prefix1

            cluster(rs::fork, nodes, initialTopology, nodeMap -> new Request()
            {
                @Override
                public void process(Node node, Node.Id from, ReplyContext replyContext)
                {
                    Ranges localRange = Ranges.ofSortedAndDeoverlapped(prefix1); // make sure to use the range removed

                    Gen<Key> keyGen = AccordGens.prefixedIntHashKeyInsideRanges(localRange);
                    Keys keys = Keys.of(Gens.lists(keyGen).unique().ofSizeBetween(1, 10).next(rs));
                    Txn txn = listWriteTxn(from, keys);

                    TxnId txnId = node.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);

                    for (Node n : nodeMap.values())
                        ((TestableConfigurationService) n.configService()).reportTopology(updatedTopology);

                    node.coordinate(txnId, txn).addCallback((success, failure) -> {
                        if (failure == null)
                        {
                            node.agent().onUncaughtException(new AssertionError("Expected TopologyMismatch exception, but txn was success"));
                        }
                        else if (!(failure instanceof TopologyMismatch))
                        {
                            if (failure instanceof Timeout || failure instanceof Preempted)
                            {
                                logger.warn("{} seen...", failure.getClass().getSimpleName());
                            }
                            else
                            {
                                node.agent().onUncaughtException(new AssertionError("Expected TopologyMismatch exception, but failed with different exception", failure));
                            }
                        }
                    });
                }

                @Override
                public MessageType type()
                {
                    return null;
                }
            });
        });
    }

    static void cluster(Supplier<RandomSource> randomSupplier, List<Node.Id> nodes, Topology initialTopology, Function<Map<Node.Id, Node>, Request> init)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        PropagatingPendingQueue queue = new PropagatingPendingQueue(failures, new RandomDelayQueue(randomSupplier.get()));
        RandomSource retryRandom = randomSupplier.get();
        Consumer<Runnable> retryBootstrap = retry -> {
            long delay = retryRandom.nextInt(1, 15);
            queue.add((PendingRunnable) retry::run, delay, TimeUnit.SECONDS);
        };
        Function<BiConsumer<Timestamp, Ranges>, ListAgent> agentSupplier = onStale -> new ListAgent(1000L, failures::add, retryBootstrap, onStale);
        RandomSource nowRandom = randomSupplier.get();
        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource forked = nowRandom.fork();
            // TODO (now): meta-randomise scale of clock drift
            return FrequentLargeRange.builder(forked)
                                     .ratio(1, 5)
                                     .small(50, 5000, TimeUnit.MICROSECONDS)
                                     .large(1, 10, TimeUnit.MILLISECONDS)
                                     .build()
                                     .mapAsLong(j -> Math.max(0, queue.nowInMillis() + TimeUnit.NANOSECONDS.toMillis(j)))
                                     .asLongSupplier(forked);
        };
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(1000L, failures::add, retryBootstrap, (i1, i2) -> {
            throw new IllegalAccessError("Global executor should enver get a stale event");
        }));
        TopologyFactory topologyFactory = new TopologyFactory(initialTopology.maxRf(), initialTopology.ranges().stream().toArray(Range[]::new))
        {
            @Override
            public Topology toTopology(Node.Id[] cluster)
            {
                return initialTopology;
            }
        };
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Map<Node.Id, Node>> nodeMap = new AtomicReference<>();
        Cluster.run(nodes.toArray(Node.Id[]::new),
                    MessageListener.Noop.INSTANCE,
                    () -> queue,
                    (id, onStale) -> globalExecutor.withAgent(agentSupplier.apply(onStale)),
                    queue::checkFailures,
                    ignore -> {
                    },
                    randomSupplier,
                    nowSupplier,
                    topologyFactory,
                    new Supplier<>()
                    {
                        private Iterator<Request> requestIterator = null;
                        private final RandomSource rs = randomSupplier.get();
                        @Override
                        public Packet get()
                        {
                            if (requestIterator == null)
                                requestIterator = Collections.singleton(init.apply(nodeMap.get())).iterator();
                            if (!requestIterator.hasNext())
                                return null;
                            Node.Id id = rs.pick(nodes);
                            return new Packet(id, id, counter.incrementAndGet(), requestIterator.next());
                        }
                    },
                    Runnable::run,
                    nodeMap::set);
        if (!failures.isEmpty())
        {
            AssertionError error = new AssertionError("Unexpected errors detected");
            failures.forEach(error::addSuppressed);
            throw error;
        }
    }
}