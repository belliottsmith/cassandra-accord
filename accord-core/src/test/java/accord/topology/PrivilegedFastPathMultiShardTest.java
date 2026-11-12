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

package accord.topology;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.IntKey;
import accord.impl.mock.MockCluster.Clock;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Keys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topology;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.range;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static accord.primitives.TxnId.MediumPath.TrackStable;

/**
 * Neither Node.nextTxnId(Txn) (via BestFastPath) nor Node.nextTxnId(Txn, FastPath, MediumPath) may grant a privileged
 * coordinator fast path to a transaction spanning multiple keys or local shards.
 */
public class PrivilegedFastPathMultiShardTest
{
    private static final Id ID1 = id(1);
    private static final Topology TOPOLOGY = topology(1, shard(range(0, 100), idList(1, 2, 3), idSet(1, 2, 3)),
                                                         shard(range(100, 200), idList(1, 2, 3), idSet(1, 2, 3)));

    private static void check(Keys keys, boolean expectPrivileged)
    {
        Node node = createNode(ID1, TOPOLOGY, new RecordingMessageSink(ID1, Network.BLACK_HOLE), new Clock(100));
        try
        {
            Txn txn = writeTxn(keys);
            Assertions.assertEquals(expectPrivileged, node.nextTxnId(txn).hasPrivilegedCoordinator(), "nextTxnId(Txn)");
            for (FastPath requested : new FastPath[] { PrivilegedCoordinatorWithDeps, PrivilegedCoordinatorWithoutDeps })
            {
                TxnId txnId = node.nextTxnId(txn, requested, TrackStable);
                Assertions.assertEquals(expectPrivileged ? requested : Unoptimised, txnId.fastPath(), "nextTxnId(Txn, " + requested + ")");
            }
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void singleShard()
    {
        check(Keys.of(IntKey.key(10)), true);
    }

    @Test
    void multiKeySingleShard()
    {
        check(Keys.of(IntKey.key(10), IntKey.key(20)), false);
    }

    @Test
    void multiShard()
    {
        check(Keys.of(IntKey.key(10), IntKey.key(150)), false);
    }
}
