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
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.routing;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;


/**
 * A re-Accept that proposes a later executeAt without changing status must raise maxConflicts.
 *
 * One replica (node 1) of an rf=5 shard sees, for transaction A:
 *   PreAccept(A) -> votes txnId A
 *   Accept(SLOW, ballot b1, executeAt = A)   (recovery b1 found only fast-path votes and proposed txnId)
 *   Accept(SLOW, ballot b2 > b1, executeAt = T > A)  (recovery b2 found replica 2's earlier Accept(ZERO, T))
 * then a conflicting B with A < B < T PreAccepts. Having accepted A at T (with deps that cannot contain B),
 * the replica must not vote B's fast path at B's txnId; it has to propose an executeAt after T.
 * SafeCommandStore.updateMaxConflicts returns early for AcceptedSlow -> AcceptedSlow, so maxConflicts stays at A.
 */
public class MaxConflictsReacceptTest
{
    private static final Id ID1 = id(1), ID2 = id(2), ID3 = id(3), ID4 = id(4), ID5 = id(5);
    private static final List<Id> IDS = java.util.Arrays.asList(ID1, ID2, ID3, ID4, ID5);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 5, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));
    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    private static PreAccept preAccept(TxnId txnId, Txn txn, RoutingKey homeKey)
    {
        FullRoute<?> route = txn.keys().toRoute(homeKey);
        return PreAccept.SerializerSupport.create(txnId, route.overlapping(FULL_RANGE), txnId.epoch(), txnId.epoch(), txnId.epoch(), txn.slice(FULL_RANGE, true), null, false, route);
    }

    private static Accept accept(TxnId txnId, Txn txn, RoutingKey homeKey, Ballot ballot, Timestamp executeAt)
    {
        FullRoute<?> route = txn.keys().toRoute(homeKey);
        Route<?> scope = route.overlapping(FULL_RANGE);
        return Accept.SerializerSupport.create(txnId, scope, txnId.epoch(), txnId.epoch(), Accept.Kind.SLOW, ballot, executeAt,
                                               Deps.NONE.intersecting(scope), Accept.AcceptFlags.encode(txnId, false));
    }

    private static Reply lastReply(RecordingMessageSink sink)
    {
        Assertions.assertFalse(sink.responses.isEmpty(), "no reply recorded");
        return sink.responses.get(sink.responses.size() - 1).payload;
    }

    /**
     * Runs the sequence at one replica and returns B's PreAccept witnessedAt.
     * @param reaccept true: Accept(b1, A) then Accept(b2, T) (the counterexample);
     *                 false: only Accept(b2, T) (control, status changes PreAccepted -> AcceptedSlow)
     */
    private static void run(boolean reaccept)
    {
        RecordingMessageSink sink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, sink, clock);
        try
        {
            Raw key = IntKey.key(10);
            RoutingKey home = key.toUnseekable();
            CommandStore store = node.unsafeForKey(home);
            Keys keys = Keys.of(key);

            TxnId a = new TxnId(1, 100, 0, Write, Key, ID2);
            Txn txnA = writeTxn(keys);
            // replica 2's (ID3) clock was ahead: it proposed T, and A's original coordinator Accept(ZERO, T)ed there
            Timestamp t = Timestamp.fromValues(1, 300, ID3);
            TxnId b = new TxnId(1, 200, 0, Write, Key, ID4);
            Assertions.assertTrue(a.compareTo(b) < 0 && b.compareTo(t) < 0);

            clock.increment(1);
            preAccept(a, txnA, home).process(node, ID2, REPLY_CONTEXT);
            PreAccept.PreAcceptOk okA = (PreAccept.PreAcceptOk) lastReply(sink);
            Assertions.assertEquals(a, okA.witnessedAt, "A should get the fast-path vote at this replica");

            if (reaccept)
            {
                // recovery b1 (coordinator node 5) saw only fast-path votes and proposes executeAt = txnId
                Ballot b1 = Ballot.fromValues(1, 150, ID5);
                accept(a, txnA, home, b1, a).process(node, ID5, REPLY_CONTEXT);
                Assertions.assertTrue(((Accept.AcceptReply) lastReply(sink)).isOk(), "Accept(b1) rejected: " + lastReply(sink));
            }

            // recovery b2 (coordinator node 4) saw replica 2's Accept(ZERO, T) and re-proposes T
            Ballot b2 = Ballot.fromValues(1, 160, ID4);
            accept(a, txnA, home, b2, t).process(node, ID4, REPLY_CONTEXT);
            Assertions.assertTrue(((Accept.AcceptReply) lastReply(sink)).isOk(), "Accept(b2) rejected: " + lastReply(sink));

            Timestamp maxConflict = store.maxConflict(b, keys.toParticipants());

            // conflicting B, A < B < T, PreAccepts at this replica
            clock.increment(1);
            preAccept(b, writeTxn(keys), home).process(node, ID4, REPLY_CONTEXT);
            PreAccept.PreAcceptOk okB = (PreAccept.PreAcceptOk) lastReply(sink);

            Assertions.assertTrue(okB.witnessedAt.compareTo(t) > 0,
                                  "replica accepted A at executeAt=" + t + " (ballot " + b2 + ") but then voted B=" + b
                                  + " fast path with witnessedAt=" + okB.witnessedAt + " < A.executeAt; maxConflict(B)=" + maxConflict
                                  + "; B's deps=" + okB.deps);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void reacceptAtHigherExecuteAtBumpsMaxConflicts()
    {
        run(true);
    }

    @Test
    void controlFirstAcceptAtExecuteAtBumpsMaxConflicts()
    {
        run(false);
    }
}
