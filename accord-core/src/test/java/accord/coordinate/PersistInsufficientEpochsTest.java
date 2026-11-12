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
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import accord.Utils;
import accord.api.AsyncExecutor;
import accord.api.MessageSink;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.InformDurable;
import accord.messages.PreAccept;
import accord.messages.ReadData;
import accord.messages.Request;
import accord.messages.WaitUntilApplied;
import accord.primitives.PartialDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncChainUtils;

/**
 * PersistSyncPoint answers an ApplyReply(InsufficientEpochs) with a Maximal Apply resend. The reply must not be final,
 * so that the replica remains accounted for by its original callback (as for Insufficient and CommitOrReadNack).
 * Previously the replica was dropped from the tracker, so that once the last outstanding reply arrived the tracker
 * was undecided with nothing inflight and AbstractCoordination threw an invariant failure.
 *
 * Coordinator N1 runs the real CoordinateSyncPoint.exclusive -> PersistSyncPoint path; remote replicas N2, N3
 * are a scripted MessageSink that answers Apply with the reply kinds a real replica produces.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class PersistInsufficientEpochsTest
{
    static final Node.Id N1 = new Node.Id(1);
    static final Node.Id N2 = new Node.Id(2);
    static final Node.Id N3 = new Node.Id(3);
    static final SortedArrayList<Node.Id> ALL = new SortedArrayList<>(new Node.Id[] { N1, N2, N3 });

    static class RecordingAgent extends TestAgent
    {
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();
        RecordingAgent(MockCluster.Clock clock) { super(clock); }
        @Override public void onException(Throwable t) { exceptions.add(t); }
        @Override public void onException(Throwable t, String context) { exceptions.add(t); }
    }

    /** N2 RaceWithRecovery (counted failure), N3 InsufficientEpochs (untracked resend). */
    @Test
    void insufficientEpochsWithRaceWithRecovery() throws Exception
    {
        run(Map.of(N2, ApplyReply.RaceWithRecovery, N3, new ApplyReply(ApplyReply.Kind.InsufficientEpochs, 1)));
    }

    /** Both remote replicas lag an epoch: each answers InsufficientEpochs. */
    @Test
    void twoReplicasInsufficientEpochs() throws Exception
    {
        run(Map.of(N2, new ApplyReply(ApplyReply.Kind.InsufficientEpochs, 1), N3, new ApplyReply(ApplyReply.Kind.InsufficientEpochs, 1)));
    }

    private static void run(Map<Node.Id, ApplyReply> firstApplyReply) throws Exception
    {
        Topology t1 = TopologyUtils.initialTopology(ALL, Ranges.single(IntKey.range(0, 100)), ALL.size());
        MockCluster.Clock clock = new MockCluster.Clock(0);
        RecordingAgent agent = new RecordingAgent(clock);
        List<String> untracked = new CopyOnWriteArrayList<>();
        List<Runnable> timeouts = new CopyOnWriteArrayList<>();
        Node n1 = Utils.createNode(N1, t1, scriptedMessaging(firstApplyReply, untracked, timeouts), clock, agent);
        for (Node.Id node : ALL)
            n1.topology().onReadyToCoordinate(node, t1.epoch());

        TxnId txnId = new TxnId(1, n1.now(), 0, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, n1.id());
        AsyncChainUtils.getUninterruptibly(CoordinateSyncPoint.exclusive(n1, txnId, Ranges.single(IntKey.range(0, 100))), 10, TimeUnit.SECONDS);

        // wait for the Maximal resends, i.e. for the InsufficientEpochs replies to have been processed
        long expectResends = firstApplyReply.values().stream().filter(r -> r.kind == ApplyReply.Kind.InsufficientEpochs).count();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (agent.exceptions.isEmpty() && untracked.stream().filter(s -> s.startsWith("Apply(Maximal)")).count() < expectResends && System.nanoTime() < deadline)
            Thread.sleep(10);
        Thread.sleep(100);
        Assertions.assertTrue(agent.exceptions.isEmpty(), "PersistSyncPoint threw on InsufficientEpochs: " + agent.exceptions);
        Assertions.assertEquals(expectResends, timeouts.size(), "the original callback should remain registered for each InsufficientEpochs reply");

        // the resend's reply cannot reach the original callback, so it will eventually time out; this must be accounted for cleanly
        timeouts.forEach(Runnable::run);
        Thread.sleep(100);
        Assertions.assertTrue(agent.exceptions.stream().allMatch(t -> t instanceof accord.coordinate.Timeout), "PersistSyncPoint must fail cleanly on timeout: " + agent.exceptions);
    }

    private static MessageSink scriptedMessaging(Map<Node.Id, ApplyReply> firstApplyReply, List<String> untracked, List<Runnable> timeouts)
    {
        MessageSink msg = Mockito.mock(MessageSink.class);
        Mockito.doAnswer(args -> {
            Node.Id to = args.getArgument(0);
            Request request = args.getArgument(1);
            untracked.add("tracked:" + request.getClass().getSimpleName() + "->" + to);
            if (request instanceof PreAccept)
            {
                PreAccept preAccept = (PreAccept) request;
                onSuccess(args, new PreAccept.PreAcceptOk(preAccept.txnId, preAccept.txnId, PartialDeps.NONE, ExecuteFlags.none()));
            }
            else if (request instanceof Accept)
                onSuccess(args, new Accept.AcceptReply(((Accept) request).scope, PartialDeps.NONE));
            else if (request instanceof Commit)
                onSuccess(args, new ReadData.ReadOk(null, null, 0));
            else if (request instanceof Apply)
            {
                ApplyReply reply = firstApplyReply.getOrDefault(to, ApplyReply.Applied);
                if (!reply.isFinal())
                    timeouts.add(() -> onFailure(args));
                onSuccess(args, reply);
            }
            else if (request instanceof WaitUntilApplied)
                onSuccess(args, new ReadData.ReadOk(null, null, 0));
            else
                throw new AssertionError("Unexpected request: " + request);
            return null;
        }).when(msg).send(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        // untracked sends: the Maximal Apply resend (the replica applies it; no callback exists) and InformDurable
        Mockito.doAnswer(args -> {
            Node.Id to = args.getArgument(0);
            Request request = args.getArgument(1);
            if (request instanceof Apply) untracked.add("Apply(" + ((Apply) request).kind + ")->" + to);
            else if (request instanceof InformDurable) untracked.add("InformDurable");
            else untracked.add(request.getClass().getSimpleName() + "->" + to);
            return null;
        }).when(msg).send(Mockito.any(), Mockito.any());
        return msg;
    }

    private static void onFailure(InvocationOnMock args)
    {
        Node.Id to = args.getArgument(0);
        AsyncExecutor executor = args.getArgument(2);
        Callback<?> cb = args.getArgument(3);
        executor.execute(() -> cb.onFailure(to, null));
    }

    private static <T> void onSuccess(InvocationOnMock args, T reply)
    {
        Node.Id to = args.getArgument(0);
        AsyncExecutor executor = args.getArgument(2);
        Callback<T> cb = args.getArgument(3);
        executor.execute(() -> cb.onSuccess(to, reply));
    }
}
