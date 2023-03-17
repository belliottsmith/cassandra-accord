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

import accord.impl.IntHashKey;
import accord.impl.basic.SimpleCluster;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListResult;
import accord.impl.list.ListUpdate;
import accord.impl.list.ListWrite;
import accord.local.Node;
import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.messages.Accept;
import accord.messages.Accept.AcceptReply;
import accord.messages.Apply;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.messages.Reply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullKeyRoute;
import accord.primitives.Keys;
import accord.primitives.Routable;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.DefaultRandom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.impl.IntHashKey.key;
import static accord.messages.Commit.Kind.Maximal;
import static accord.messages.Commit.Kind.Minimal;
import static accord.primitives.Txn.Kind.Write;

public class RecoverTest
{
    private static CommandStore getCommandShard(Node node, Key key)
    {
        return node.unsafeForKey(key);
    }

    private static Command getCommand(Node node, Key key, TxnId txnId)
    {
        CommandStore commandStore = getCommandShard(node, key);
        Assertions.assertTrue(inMemory(commandStore).hasCommand(txnId));
        return inMemory(commandStore).command(txnId).value();
    }

    private static void assertStatus(Node node, Key key, TxnId txnId, Status status)
    {
        Command command = getCommand(node, key, txnId);

        Assertions.assertNotNull(command);
        Assertions.assertEquals(status, command.status());
    }

    private static void assertMissing(Node node, Key key, TxnId txnId)
    {
        CommandStore commandStore = getCommandShard(node, key);
        Assertions.assertFalse(inMemory(commandStore).hasCommand(txnId));
    }

    private static void assertTimeout(Future<?> f)
    {
        try
        {
            f.get();
            Assertions.fail("expected timeout");
        }
        catch (ExecutionException e)
        {
            // timeout expected
            Assertions.assertEquals(Timeout.class, e.getCause().getClass());
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    void counterexampleTest()
    {
        SimpleCluster cluster = SimpleCluster.create(5);
        cluster.setNow(1);
        Keys keys = Keys.of(key(1));
        FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
        Txn txn1, txn2;
        {
            ListUpdate upd1 = new ListUpdate();
            upd1.put(keys.get(0), 1);
            txn1 = new Txn.InMemory(keys, new ListRead(Keys.EMPTY, Keys.EMPTY), new ListQuery(new Id(-1), -1), upd1);
            ListUpdate upd2 = new ListUpdate();
            upd2.put(keys.get(0), 2);
            txn2 = new Txn.InMemory(keys, new ListRead(Keys.EMPTY, Keys.EMPTY), new ListQuery(new Id(-1), -1), upd2);
        }
        TxnId txnId1 = new TxnId(1, 1, Write, Domain.Key, new Id(5));
        TxnId txnId2 = new TxnId(1, 2, Write, Domain.Key, new Id(1));
        Ballot ballot1 = Ballot.fromValues(1, 1, new Id(2));
        Topologies topologies = cluster.get(5).topology().forEpoch(route, 1);
        cluster.send(5, 5, 1, new PreAccept(new Id(5), topologies, txnId1, txn1, route));
        cluster.send(5, 4, 2, new PreAccept(new Id(4), topologies, txnId1, txn1, route));
        cluster.send(1, 1, 3, new PreAccept(new Id(1), topologies, txnId2, txn2, route));
        cluster.send(1, 2, 4, new PreAccept(new Id(2), topologies, txnId2, txn2, route));
        cluster.send(1, 3, 5, new PreAccept(new Id(3), topologies, txnId2, txn2, route));
        cluster.send(1, 1, 6, new Accept(new Id(1), topologies, Ballot.ZERO, txnId2, route, txnId2, keys, Deps.NONE));
        cluster.send(1, 2, 7, new Accept(new Id(2), topologies, Ballot.ZERO, txnId2, route, txnId2, keys, Deps.NONE));
        cluster.send(1, 3, 8, new Accept(new Id(3), topologies, Ballot.ZERO, txnId2, route, txnId2, keys, Deps.NONE));
        cluster.send(1, 1, 9, new Commit(Maximal, new Id(1), topologies.current(), topologies, txnId2, txn2, route, Keys.EMPTY, txnId2, Deps.NONE, false));
        cluster.send(1, 1, 10, new Apply(new Id(1), topologies, topologies, 1, txnId2, route, txn2, txnId2, Deps.NONE, new Writes(txnId2, keys, new ListWrite()), new ListResult(new Id(-1), -1L, txnId2, Keys.EMPTY, Keys.EMPTY, new int[0][], (ListUpdate) txn2.update())));
        List<Reply> recoveries = new ArrayList<>();
        cluster.send(2, 2, 11, new BeginRecovery(new Id(2), topologies, txnId2, txn2, route, ballot1), recoveries::add);
        cluster.send(2, 3, 12, new BeginRecovery(new Id(3), topologies, txnId2, txn2, route, ballot1), recoveries::add);
        cluster.send(2, 4, 13, new BeginRecovery(new Id(4), topologies, txnId2, txn2, route, ballot1), recoveries::add);
        List<Reply> accepts = new ArrayList<>();
        cluster.send(2, 2, 14, new Accept(new Id(2), topologies, ballot1, txnId2, route, txnId2, keys, Deps.NONE), accepts::add);
        cluster.send(2, 3, 15, new Accept(new Id(3), topologies, ballot1, txnId2, route, txnId2, keys, Deps.NONE), accepts::add);
        cluster.send(2, 4, 16, new Accept(new Id(4), topologies, ballot1, txnId2, route, txnId2, keys, Deps.NONE), accepts::add);
//        cluster.send(2, 2, 17, new Commit(Maximal, new Id(2), topologies.current(), topologies, txnId2, txn2, route, Keys.EMPTY, txnId2, ((AcceptReply)accepts.get(2)).deps, false));
//        cluster.send(2, 3, 18, new Commit(Maximal, new Id(3), topologies.current(), topologies, txnId2, txn2, route, Keys.EMPTY, txnId2, ((AcceptReply)accepts.get(2)).deps, false));
//        cluster.send(2, 4, 19, new Commit(Maximal, new Id(4), topologies.current(), topologies, txnId2, txn2, route, Keys.EMPTY, txnId2, ((AcceptReply)accepts.get(2)).deps, false));
        List<Reply> recoveries2 = new ArrayList<>();
        cluster.send(4, 3, 20, new BeginRecovery(new Id(3), topologies, txnId1, txn1, route, ballot1), recoveries2::add);
        cluster.send(4, 4, 21, new BeginRecovery(new Id(4), topologies, txnId1, txn1, route, ballot1), recoveries2::add);
        cluster.send(4, 5, 22, new BeginRecovery(new Id(5), topologies, txnId1, txn1, route, ballot1), recoveries2::add);
        System.out.println();
    }

    // TODO
//    void conflictTest() throws Throwable
//    {
//        Key key = IntKey.key(10);
//        try (MockCluster cluster = MockCluster.builder().nodes(9).replication(9).build())
//        {
//            cluster.networkFilter.isolate(ids(7, 9));
//            cluster.networkFilter.addFilter(anyId(), isId(ids(5, 6)), notMessageType(PreAccept.class));
//
//            TxnId txnId1 = new TxnId(1, 100, 0, id(100));
//            Txn txn1 = writeTxn(Keys.of(key));
//            assertTimeout(Coordinate.coordinate(cluster.get(1), txnId1, txn1, key));
//
//            TxnId txnId2 = new TxnId(1, 50, 0, id(101));
//            Txn txn2 = writeTxn(Keys.of(key));
//            cluster.networkFilter.clear();
//            cluster.networkFilter.isolate(ids(1, 7));
//            assertTimeout(Coordinate.coordinate(cluster.get(9), txnId2, txn2, key));
//
//            cluster.nodes(ids(1, 4)).forEach(n -> assertStatus(n, key, txnId1, Status.Accepted));
//            cluster.nodes(ids(5, 6)).forEach(n -> assertStatus(n, key, txnId1, Status.PreAccepted));
//            cluster.nodes(ids(7, 9)).forEach(n -> assertMissing(n, key, txnId1));
//
//            cluster.nodes(ids(1, 7)).forEach(n -> assertMissing(n, key, txnId2));
//            cluster.nodes(ids(8, 9)).forEach(n -> assertStatus(n, key, txnId2, Status.PreAccepted));
//
//            //
//            cluster.networkFilter.clear();
//            cluster.networkFilter.isolate(ids(1, 4));
//            Recover.recover(cluster.get(8), txnId2, txn2, key).get();
//
//            List<Node> nodes = cluster.nodes(ids(5, 9));
//            Assertions.assertTrue(txnId2.compareTo(txnId1) < 0);
//            nodes.forEach(n -> assertStatus(n, key, txnId2, Status.Applied));
//            nodes.forEach(n -> {
//                assertStatus(n, key, txnId2, Status.Applied);
//                Command command = getCommand(n, key, txnId2);
//                Assertions.assertEquals(txnId1, command.executeAt());
//            });
//        }
//    }
}
