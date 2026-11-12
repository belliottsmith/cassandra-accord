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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.Utils;
import accord.api.AsyncExecutor;
import accord.api.MessageSink;
import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.messages.Callback;
import accord.messages.Request;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.Cancellable;

/**
 * Invalidate must tolerate a shard that is final only by virtue of a truncated reply's decision (i.e. without a promise).
 *
 * T is a fast-path (executeAt == txnId) write over key 50 (shard A, nodes 1-3) and key 150 (shard B, nodes 4-6).
 * C0 commits it on the fast path; the commit reaches shard A, which applies T and, after a shard-A durability
 * point, truncates it on node 2 to TruncatedApplyWithOutcome (Cleanup.cleanupWithFullRoute: TRUNCATE_BEFORE is
 * per local shard, and since shard B never applied T durability is not Universal -> truncateWithOutcome).
 * The Commit to shard B is lost, so nodes 4-6 are still PreAccepted with a fast-path vote.
 *
 * An Invalidate for T over {50, 150} then receives exactly the replies those replicas produce in
 * BeginInvalidation.applyInternal (lines 63-91):
 *  - node 2 (Truncated): supersededBy=null, truncated=owned participants, saveStatus=TruncatedApplyWithOutcome,
 *    acceptedFastPath = txnId.equals(executeAt) = true, route = FullRoute (kept by TruncatedApply*)
 *  - nodes 4,5,6 (PreAccepted, promise granted): supersededBy=null, PreAccepted, acceptedFastPath=true, FullRoute
 * Nodes 1 and 3 are slow (their replies are not needed for the tracker to finish).
 *
 * Intended behaviour: the invalidation either recovers T (PrepareRecovery) or reports it truncated; it must
 * not fail an internal invariant. Previously, InvalidationTracker reports Success because shard A is
 * final via hasDecision, then Invalidate.invalidate() falls through the
 * PreAccepted case and Invariants.require(tracker.all(isPromised)) throws for shard A.
 */
public class InvalidateTruncatedShardTest
{
    static Id id(int i) { return new Id(i); }

    static class Sent
    {
        final Id to; final Request request; final Callback<Object> callback;
        Sent(Id to, Request request, Callback<Object> callback) { this.to = to; this.request = request; this.callback = callback; }
    }

    static class RecordingAgent extends TestAgent
    {
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();
        RecordingAgent(MockCluster.Clock clock) { super(clock); }
        @Override public void onException(Throwable t) { exceptions.add(t); }
        @Override public void onException(Throwable t, String context) { exceptions.add(t); }
    }

    static final RoutingKey KA = IntKey.routing(50), KB = IntKey.routing(150);
    static final FullRoute<?> ROUTE = RoutingKeys.of(KA, KB).toRoute(KA);
    // replies exactly as BeginInvalidation.applyInternal builds them for each replica's state
    static final InvalidateReply TRUNCATED_A = new InvalidateReply(null, Ballot.ZERO, SaveStatus.TruncatedApplyWithOutcome, true, ROUTE, ROUTE.homeKey(), RoutingKeys.of(KA));
    static final InvalidateReply PREACCEPTED = new InvalidateReply(null, Ballot.ZERO, SaveStatus.PreAccepted, true, ROUTE, ROUTE.homeKey(), null);

    /** B.n4, B.n5, A.n2 (truncated, decided), B.n6; A.n1/A.n3 slow. Must not throw. */
    @Test
    void invalidateWithTruncatedDecidedShardAndPreAcceptedShard() throws Exception
    {
        Assertions.assertTrue(TRUNCATED_A.hasDecision());
        String recovery = run(new Object[][] { { id(4), PREACCEPTED }, { id(5), PREACCEPTED }, { id(2), TRUNCATED_A }, { id(6), PREACCEPTED } });
        Assertions.assertNotNull(recovery);
    }

    /** control: same txn, but every replica of shard A is still PreAccepted (promised, fast vote); PrepareRecovery starts normally. */
    @Test
    void controlAllShardsPromised() throws Exception
    {
        String recovery = run(new Object[][] { { id(4), PREACCEPTED }, { id(5), PREACCEPTED }, { id(1), PREACCEPTED }, { id(2), PREACCEPTED }, { id(6), PREACCEPTED }, { id(3), PREACCEPTED } });
        // PrepareRecovery proceeds and contacts replicas (first request observed: CheckStatus)
        Assertions.assertNotNull(recovery);
    }

    /** @return simple name of the first non-BeginInvalidation request sent (recovery started) or "finished" */
    private static String run(Object[][] order) throws Exception
    {
        SortedArrayList<Id> shardA = new SortedArrayList<>(new Id[] { id(1), id(2), id(3) });
        SortedArrayList<Id> shardB = new SortedArrayList<>(new Id[] { id(4), id(5), id(6) });
        Topology topology = Utils.topology(1, Utils.shard(IntKey.range(0, 100), shardA),
                                              Utils.shard(IntKey.range(100, 200), shardB),
                                              // the coordinator (node 7) replicates an unrelated range so it has a command store/executor
                                              Utils.shard(IntKey.range(200, 300), new SortedArrayList<>(new Id[] { id(7) })));

        List<Sent> sent = new CopyOnWriteArrayList<>();
        CompletableFuture<String> recoveryStarted = new CompletableFuture<>();
        MessageSink sink = new MessageSink()
        {
            @Override public void send(Id to, Request request) { sent.add(new Sent(to, request, null)); }
            @SuppressWarnings("unchecked")
            @Override public Cancellable send(Id to, Request request, int attempt, AsyncExecutor executor, Callback<?> callback)
            {
                sent.add(new Sent(to, request, (Callback<Object>) callback));
                if (!(request instanceof BeginInvalidation))
                    recoveryStarted.complete(request.getClass().getSimpleName());
                return null;
            }
        };

        MockCluster.Clock clock = new MockCluster.Clock(100);
        RecordingAgent agent = new RecordingAgent(clock);
        // coordinator of the invalidation is a node outside shards A and B, so every reply goes through the sink
        Node coordinator = Utils.createNode(id(7), topology, sink, clock, agent);

        TxnId txnId = new TxnId(1, 10, Txn.Kind.Write, Routable.Domain.Key, id(1));
        Assertions.assertEquals(TxnId.FastPath.Unoptimised, txnId.fastPath());
        Participants<?> invalidateWith = RoutingKeys.of(KA, KB);

        CompletableFuture<Throwable> outcome = new CompletableFuture<>();
        Invalidate.invalidate(coordinator, txnId, invalidateWith, (success, failure) -> outcome.complete(failure == null ? new Throwable("success:" + success) : failure));

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (sent.stream().filter(s -> s.request instanceof BeginInvalidation).count() < 6 && System.nanoTime() < deadline)
            Thread.sleep(10);
        if (outcome.isDone())
            throw new AssertionError("Invalidate completed before any reply", outcome.getNow(null));
        if (!agent.exceptions.isEmpty())
            throw new AssertionError("exception before any reply", agent.exceptions.get(0));
        Assertions.assertEquals(6, sent.stream().filter(s -> s.request instanceof BeginInvalidation).count(), "BeginInvalidation not sent to all replicas");

        for (Object[] o : order)
        {
            Id to = (Id) o[0];
            Sent s = sent.stream().filter(x -> x.to.equals(to) && x.request instanceof BeginInvalidation).findFirst().get();
            s.callback.onSuccess(to, o[1]);
            Thread.sleep(50);
        }

        // Wait (bounded) for either the invalidation to move on (PrepareRecovery sends BeginRecovery) or to complete
        CompletableFuture.anyOf(outcome, recoveryStarted).get(10, TimeUnit.SECONDS);
        Thread.sleep(200);

        Throwable failure = outcome.getNow(null);
        if (failure != null && failure.getMessage() != null && failure.getMessage().startsWith("success:"))
            return "finished";
        if (failure == null && !agent.exceptions.isEmpty())
            failure = agent.exceptions.get(0);
        if (failure != null)
            throw new AssertionError("Invalidate failed instead of recovering/finishing T (recoveryStarted=" + recoveryStarted.getNow(null) + ")", failure);
        return recoveryStarted.getNow(null);
    }
}
