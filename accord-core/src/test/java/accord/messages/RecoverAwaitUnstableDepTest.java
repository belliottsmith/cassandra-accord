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
import accord.messages.RecoverAwait.RecoverAwaitOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.routing;
import static accord.messages.Await.Until.CommittedOrNotFastPathCommit;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.MediumPath.TrackStable;

/**
 * RecoverAwait must decide whether an awaited later transaction witnessed the recovering transaction as BeginRecovery
 * does: an UNSTABLE dependency (one first found by the Accept round, not proposed) does not witness it.
 *
 * T1 > T0 (privileged coordinator with deps) commits on the slow path with ~T0, i.e. it did not witness T0 when
 * PreAccepting. Recovery of T0 that awaits T1 (Recover.awaitSupersedingCoord) must treat T1 as a reject, as
 * BeginRecovery(T0) does once T1 has committed.
 */
public class RecoverAwaitUnstableDepTest
{
    private static final Id ID1 = id(1), ID2 = id(2), ID3 = id(3), ID4 = id(4), ID5 = id(5);
    private static final List<Id> IDS = List.of(ID1, ID2, ID3, ID4, ID5);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 5, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));
    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    private static TxnId privileged(TxnId txnId)
    {
        return new TxnId(txnId.epoch(), txnId.hlc(), PrivilegedCoordinatorWithDeps.bits | TrackStable.bit(), Write, Key, txnId.node);
    }

    private static Object lastReply(RecordingMessageSink sink)
    {
        Assertions.assertFalse(sink.responses.isEmpty(), "no reply recorded");
        return sink.responses.get(sink.responses.size() - 1).payload;
    }

    private static class Outcome
    {
        final RecoverAwaitOk recoverAwait;
        final RecoverOk beginRecovery;

        Outcome(RecoverAwaitOk recoverAwait, RecoverOk beginRecovery)
        {
            this.recoverAwait = recoverAwait;
            this.beginRecovery = beginRecovery;
        }
    }

    private static Outcome run(boolean unstable)
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

            TxnId t0 = privileged(clock.idForNode(1, ID2));
            clock.increment(10);
            TxnId t1 = privileged(clock.idForNode(1, ID3));
            clock.increment(10);
            Assertions.assertTrue(t0.compareTo(t1) < 0);

            PreAccept.SerializerSupport.create(t0, route.overlapping(FULL_RANGE), 1, 1, 1, txn.slice(FULL_RANGE, true), null, false, route)
                                       .process(node, ID2, REPLY_CONTEXT);

            Deps deps;
            try (Deps.Builder builder = new Deps.Builder(true))
            {
                builder.add(home, t0);
                deps = builder.build();
            }
            // as Propose.mergeNewDeps marks a dependency found only by the Accept round
            if (unstable)
                deps = deps.markUnstableBefore(t1);

            Commit.SerializerSupport.create(t1, route.overlapping(FULL_RANGE), 1, 1, Commit.Kind.CommitWithTxn, Ballot.ZERO, t1,
                                            txn.slice(FULL_RANGE, true), deps.intersecting(route), route)
                                    .process(node, ID3, REPLY_CONTEXT);

            RecoverAwait.SerializerSupport.create(t1, RoutingKeys.of(home), CommittedOrNotFastPathCommit, false, 1, 1, -1, t0)
                                          .process(node, ID4, REPLY_CONTEXT);
            Object awaitReply = lastReply(sink);
            Assertions.assertTrue(awaitReply instanceof RecoverAwaitOk, () -> "Unexpected reply " + awaitReply);

            Ballot ballot = Ballot.fromValues(1, clock.now(), ID4);
            BeginRecovery.SerializationSupport.create(t0, route.overlapping(FULL_RANGE), 1, 1, txn.slice(FULL_RANGE, true), ballot, route, 1, 0)
                                              .process(node, ID4, REPLY_CONTEXT);
            Object recoverReply = lastReply(sink);
            Assertions.assertTrue(recoverReply instanceof RecoverOk, () -> "Unexpected reply " + recoverReply);

            return new Outcome((RecoverAwaitOk) awaitReply, (RecoverOk) recoverReply);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void unstableDepDoesNotWitness()
    {
        Outcome outcome = run(true);
        Assertions.assertTrue(outcome.beginRecovery.supersedingRejects, "BeginRecovery should treat ~T0 as not witnessed");
        Assertions.assertEquals(RecoverAwaitOk.Reject, outcome.recoverAwait, "RecoverAwait should agree with BeginRecovery");
    }

    @Test
    void stableDepWitnesses()
    {
        Outcome outcome = run(false);
        Assertions.assertFalse(outcome.beginRecovery.supersedingRejects);
        Assertions.assertEquals(RecoverAwaitOk.Accept, outcome.recoverAwait);
    }
}
