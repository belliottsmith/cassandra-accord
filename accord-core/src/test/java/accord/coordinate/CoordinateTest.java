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

package accord.coordinate;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.InMemoryCommandStore;
import accord.impl.IntKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.impl.mock.MockCluster;
import accord.api.Result;
import accord.impl.mock.MockStore;
import accord.local.Status;
import accord.messages.*;
import accord.primitives.*;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChains.awaitUninterruptibly;
import static accord.utils.async.AsyncChains.getUninterruptibly;

public class CoordinateTest
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinateTest.class);
    @Test
    void simpleTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Keys keys = keys(10);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }
    @Test
    void simpleRangeTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Ranges keys = ranges(range(1, 2));
            Txn txn = writeTxn(keys);
            FullRangeRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void slowPathTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(7).replication(7).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            Txn txn = writeTxn(keys(10));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    private TxnId coordinate(Node node, long clock, Keys keys) throws Throwable
    {
        TxnId txnId = node.nextTxnId(Write, Key);
        txnId = new TxnId(txnId.epoch(), txnId.hlc() + clock, Write, Key, txnId.node);
        Txn txn = writeTxn(keys);
        Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, node.computeRoute(txnId, txn.keys())));
        Assertions.assertEquals(MockStore.RESULT, result);
        return txnId;
    }

    @Test
    void multiKeyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(6).maxKey(600).build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId1 = coordinate(node, 100, keys(50, 350, 550));
            TxnId txnId2 = coordinate(node, 150, keys(250, 350, 450));
            TxnId txnId3 = coordinate(node, 125, keys(50, 60, 70, 80, 350, 550));
        }
    }

    @Test
    void writeOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            Keys keys = keys(10);
            Txn txn = new Txn.InMemory(keys, MockStore.read(Keys.EMPTY), MockStore.QUERY, MockStore.update(keys));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void readOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            Keys keys = keys(10);
            Txn txn = new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, MockStore.update(Keys.EMPTY));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void simpleTxnThenReadOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Keys oneKey = keys(10);
            Keys twoKeys = keys(10, 20);
            Txn txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.QUERY, MockStore.update(twoKeys));
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, txn.keys().toRoute(oneKey.get(0).toUnseekable())));
            Assertions.assertEquals(MockStore.RESULT, result);

            txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.QUERY, MockStore.update(Keys.EMPTY));
            result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    private static class NoOpProgressLog implements ProgressLog
    {
        @Override public void unwitnessed(TxnId txnId, RoutingKey homeKey, ProgressShard shard) {}
        @Override public void preaccepted(Command command, ProgressShard shard) {}
        @Override public void accepted(Command command, ProgressShard shard) {}
        @Override public void committed(Command command, ProgressShard shard) {}
        @Override public void readyToExecute(Command command, ProgressShard shard) {}
        @Override public void executed(Command command, ProgressShard shard) {}
        @Override public void invalidated(Command command, ProgressShard shard) {}
        @Override public void durableLocal(TxnId txnId) {}
        @Override public void durable(Command command, @Nullable Set<Node.Id> persistedOn) {}
        @Override public void durable(TxnId txnId, @Nullable Unseekables<?, ?> unseekables, ProgressShard shard) {}
        @Override public void waiting(TxnId blockedBy, Status.Known blockedUntil, Unseekables<?, ?> blockedOn) {}
    }

    private static class NoopProgressLogFactory implements ProgressLog.Factory
    {
        private static final NoOpProgressLog INSTANCE = new NoOpProgressLog();
        @Override
        public ProgressLog create(CommandStore store)
        {
            return INSTANCE;
        }
    }

    public static AsyncResult<Void> recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        AsyncResult.Settable<Void> result = AsyncResults.settable();
        Recover.recover(node, txnId, txn, route, (r, t) -> {
            if (t == null)
                result.trySuccess(null);
            else
                result.tryFailure(t);
        });
        return result;
    }

    @Test
    void bugTest() throws Throwable
    {
        MockCluster.Builder builder = MockCluster.builder();
        builder.nodes(5);
        builder.replication(5);
        builder.progressLogFactory(i -> new NoopProgressLogFactory());
        IntKey.Raw key = IntKey.key(10);
        Keys keys = keys(10);
        try (MockCluster cluster = builder.build())
        {
            TxnId tauId;
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
            {
                cluster.networkFilter.isolate(id(1));
                cluster.networkFilter.isolate(id(2));
                cluster.networkFilter.isolate(id(3));
                cluster.networkFilter.addFilter(i -> true, i -> true, message -> message instanceof PreAccept.PreAcceptOk);
                Node coordinator = cluster.get(5);
                tauId = coordinator.nextTxnId(Write, Key);
                logger.info("tau: {}", tauId);
                try
                {
                    getUninterruptibly(Coordinate.coordinate(coordinator, tauId, txn, route));
                    Assertions.fail();
                }
                catch (ExecutionException e)
                {
                    Assertions.assertTrue(e.getCause() instanceof Timeout);
                }
                Thread.sleep(1000);
                cluster.forEach(node -> {
                    InMemoryCommandStore commandStore = (InMemoryCommandStore) node.commandStores().unsafeForKey(key);
                    InMemoryCommandStore.GlobalCommand command = commandStore.ifPresent(tauId);
                    if (idSet(1, 2, 3).contains(node.id()))
                    {
                        Assertions.assertNull(command);
                    }
                    else
                    {
                        Assertions.assertTrue(idSet(4, 5).contains(node.id()));
                        Assertions.assertNotNull(command);
                        Assertions.assertTrue(command.value().hasBeen(Status.PreAccepted));
                        Assertions.assertFalse(command.value().hasBeen(Status.Accepted));
                    }
                });
            }
            cluster.networkFilter.clear();

            TxnId gammaId;
            {
                cluster.networkFilter.isolate(id(4));
                cluster.networkFilter.isolate(id(5));
                cluster.networkFilter.addFilter(i -> true, i -> !i.equals(id(1)), message -> message instanceof Commit);
                cluster.networkFilter.addFilter(i -> !i.equals(id(1)), i -> true, message -> !(message instanceof Reply));
//                cluster.networkFilter.addFilter(i -> !i.equals(id(1)), i -> !i.equals(id(1)), message -> message instanceof CheckStatus);
                cluster.networkFilter.addFilter(i -> true, i -> !i.equals(id(1)), message -> message instanceof ReadData);
                cluster.networkFilter.addFilter(i -> true, i -> !i.equals(id(1)), message -> message instanceof Apply);
                Node coordinator = cluster.get(1);
                gammaId = coordinator.nextTxnId(Write, Key);
                logger.info("gamma: {}", gammaId);
                try
                {
                    getUninterruptibly(Coordinate.coordinate(coordinator, gammaId, txn, route));
                    // maybe ok?
                }
                catch (ExecutionException e)
                {
                    Assertions.assertTrue(e.getCause() instanceof Timeout);
                }

                Thread.sleep(1000);
                cluster.forEach(node -> {
                    InMemoryCommandStore commandStore = (InMemoryCommandStore) node.commandStores().unsafeForKey(key);
                    InMemoryCommandStore.GlobalCommand command = commandStore.ifPresent(gammaId);
                    if (node.id().equals(id(1)))
                    {
                        Assertions.assertNotNull(command);
                        Assertions.assertTrue(command.value().hasBeen(Status.Committed));
                    }
                    else if (idSet(2, 3).contains(node.id()))
                    {
                        Assertions.assertNotNull(command);
                        Assertions.assertTrue(command.value().hasBeen(Status.Accepted));
                        Assertions.assertFalse(command.value().hasBeen(Status.Committed));
                    }
                    else
                    {
                        Assertions.assertNull(command);
                    }
                });
            }
            cluster.networkFilter.clear();

            // recover
            {
                cluster.networkFilter.isolate(id(1));
//                cluster.networkFilter.isolate(id(2));
                Node coordinator = cluster.get(2);
//                awaitUninterruptibly(recover(coordinator, tauId, txn, route));
                awaitUninterruptibly(recover(coordinator, gammaId, txn, route));

                Thread.sleep(1000);
                cluster.forEach(node -> {
                    InMemoryCommandStore commandStore = (InMemoryCommandStore) node.commandStores().unsafeForKey(key);
//                    Command tauCmd = commandStore.ifPresent(tauId).value();
                    Command gammaCmd = commandStore.ifPresent(gammaId).value();
                    if (idSet(3, 4, 5).contains(node.id()))
                    {
                        Assertions.assertNotNull(gammaCmd);
                        Assertions.assertTrue(gammaCmd.hasBeen(Status.Committed));
                        Assertions.assertTrue(gammaCmd.executeAt().equals(gammaId));
//                        Assertions.assertNotNull(tauCmd);
//                        Assertions.assertTrue(tauCmd.hasBeen(Status.Committed));
//                        Assertions.assertTrue(tauCmd.executeAt().compareTo(gammaId) >= 0);
                    }
                });
            }

        }

    }
}
