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

package accord.messages;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.IntKey.Raw;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster.Clock;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginRecovery.RecoverOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.routing;

/**
 * BeginRecovery must wait on an earlier transaction that witnessed the recovering transaction in its proposed deps
 * but is not yet stable, if it may execute after the recovering transaction: it has yet to settle its deps.
 *
 * tE < t0 commits (but is not yet stable) with executeAt > t0 and deps {t0}; recovery of t0 must report tE in simpleWait.
 */
public class BeginRecoveryUnstableEarlierDepTest
{
    private static final Id ID1 = id(1), ID2 = id(2), ID3 = id(3), ID4 = id(4), ID5 = id(5);
    private static final List<Id> IDS = List.of(ID1, ID2, ID3, ID4, ID5);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 5, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));
    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    private static Object lastReply(RecordingMessageSink sink)
    {
        Assertions.assertFalse(sink.responses.isEmpty(), "no reply recorded");
        return sink.responses.get(sink.responses.size() - 1).payload;
    }

    @Test
    void waitsOnCommittedEarlierDepExecutingLater()
    {
        RecordingMessageSink sink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, sink, clock);
        try
        {
            Raw key = IntKey.key(10);
            RoutingKey home = key.toUnseekable();
            Txn txn = writeTxn(Keys.of(key));
            FullRoute<?> route = txn.keys().toRoute(home);

            TxnId tE = clock.idForNode(1, ID3);
            clock.increment(10);
            TxnId t0 = clock.idForNode(1, ID2);
            clock.increment(10);
            Timestamp executeAtE = clock.idForNode(1, ID3);
            Assertions.assertTrue(tE.compareTo(t0) < 0 && t0.compareTo(executeAtE) < 0);

            PreAccept.SerializerSupport.create(t0, route.overlapping(FULL_RANGE), 1, 1, 1, txn.slice(FULL_RANGE, true), null, false, route)
                                       .process(node, ID2, REPLY_CONTEXT);

            Deps deps;
            try (Deps.Builder builder = new Deps.Builder(true))
            {
                builder.add(home, t0);
                deps = builder.build();
            }
            // Committed, not Stable
            Commit.SerializerSupport.create(tE, route.overlapping(FULL_RANGE), 1, 1, Commit.Kind.CommitWithTxn, Ballot.ZERO, executeAtE,
                                            txn.slice(FULL_RANGE, true), deps.intersecting(route), route)
                                    .process(node, ID3, REPLY_CONTEXT);

            Ballot ballot = Ballot.fromValues(1, clock.now(), ID4);
            BeginRecovery.SerializationSupport.create(t0, route.overlapping(FULL_RANGE), 1, 1, txn.slice(FULL_RANGE, true), ballot, route, 1, 0)
                                              .process(node, ID4, REPLY_CONTEXT);
            Object reply = lastReply(sink);
            Assertions.assertTrue(reply instanceof RecoverOk, () -> "Unexpected reply " + reply);
            RecoverOk ok = (RecoverOk) reply;
            Assertions.assertTrue(ok.simpleWait != null && ok.simpleWait.contains(tE),
                                  () -> "recovery of " + t0 + " must wait on " + tE + " (committed, not stable, executing at " + executeAtE + "); simpleWait=" + ok.simpleWait);
        }
        finally
        {
            node.shutdown();
        }
    }
}
