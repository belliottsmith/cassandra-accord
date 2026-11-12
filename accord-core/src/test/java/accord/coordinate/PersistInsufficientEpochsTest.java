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
import java.util.concurrent.ConcurrentHashMap;
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
 * PersistSyncPoint answers a non-success ApplyReply (Insufficient or InsufficientEpochs) with a Maximal Apply resend.
 * The reply that triggered the resend settles the original request, so the original callback is cancelled and the
 * resend is tracked in its place; the replica's reply to the resend is then counted, and Persist must reach a quorum
 * and inform durability. Previously the resend was untracked, so the replica was never counted: for InsufficientEpochs
 * the replica was dropped from the tracker entirely (Persist hit its own invariant failure), and for Insufficient the
 * replica was eventually counted as a timeout failure (Persist failed despite the replica applying).
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

    /** N2 RaceWithRecovery (counted failure), N3 InsufficientEpochs (maximal resend applies). */
    @Test
    void insufficientEpochsWithRaceWithRecovery() throws Exception
    {
        run(Map.of(N2, ApplyReply.RaceWithRecovery, N3, new ApplyReply(ApplyReply.Kind.InsufficientEpochs, 1)));
    }

    /** Both remote replicas lag an epoch: each answers InsufficientEpochs and then applies the maximal resend. */
    @Test
    void twoReplicasInsufficientEpochs() throws Exception
    {
        run(Map.of(N2, new ApplyReply(ApplyReply.Kind.InsufficientEpochs, 1), N3, new ApplyReply(ApplyReply.Kind.InsufficientEpochs, 1)));
    }

    /** N2 loses its ApplyReply, N3 is missing the txn and applies only the maximal resend. */
    @Test
    void insufficientWithLostReply() throws Exception
    {
        run(Map.of(N2, ApplyReply.Insufficient, N3, ApplyReply.Insufficient), N2);
    }

    private static void run(Map<Node.Id, ApplyReply> firstApplyReply) throws Exception
    {
        run(firstApplyReply, null);
    }

    /**
     * @param lostReply a node whose first ApplyReply is lost, so that a quorum can only be reached if the
     *                  other replica's reply to its maximal resend is counted
     */
    private static void run(Map<Node.Id, ApplyReply> firstApplyReply, Node.Id lostReply) throws Exception
    {
        Topology t1 = TopologyUtils.initialTopology(ALL, Ranges.single(IntKey.range(0, 100)), ALL.size());
        MockCluster.Clock clock = new MockCluster.Clock(0);
        RecordingAgent agent = new RecordingAgent(clock);
        List<String> sends = new CopyOnWriteArrayList<>();
        Node n1 = Utils.createNode(N1, t1, scriptedMessaging(firstApplyReply, lostReply, sends), clock, agent);
        for (Node.Id node : ALL)
            n1.topology().onReadyToCoordinate(node, t1.epoch());

        TxnId txnId = new TxnId(1, n1.now(), 0, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, n1.id());
        AsyncChainUtils.getUninterruptibly(CoordinateSyncPoint.exclusive(n1, txnId, Ranges.single(IntKey.range(0, 100))), 10, TimeUnit.SECONDS);

        // the replicas apply the maximal resend, so the sync point must be reported durable
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (agent.exceptions.isEmpty() && !sends.contains("InformDurable") && System.nanoTime() < deadline)
            Thread.sleep(10);

        Assertions.assertTrue(agent.exceptions.isEmpty(), "PersistSyncPoint must not throw: " + agent.exceptions);
        Assertions.assertTrue(sends.contains("InformDurable"), "replicas applied the maximal resend, yet InformDurable was never sent: " + sends);
        // every maximal resend must be tracked, so that its reply can be counted
        Assertions.assertEquals(0, untrackedMaximal(sends), "the maximal resend must be tracked: " + sends);
    }

    private static int untrackedMaximal(List<String> sends)
    {
        return (int) sends.stream().filter("untracked:Apply(Maximal)"::equals).count();
    }

    private static MessageSink scriptedMessaging(Map<Node.Id, ApplyReply> firstApplyReply, Node.Id lostReply, List<String> sends)
    {
        Map<Node.Id, Integer> applies = new ConcurrentHashMap<>();
        MessageSink msg = Mockito.mock(MessageSink.class);
        // tracked sends: the reply is delivered to the callback
        Mockito.doAnswer(args -> {
            Node.Id to = args.getArgument(0);
            Request request = args.getArgument(1);
            sends.add("tracked:" + request.getClass().getSimpleName() + "->" + to);
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
                boolean first = applies.merge(to, 1, Integer::sum) == 1;
                if (first && to.equals(lostReply))
                    return null; // the reply is lost
                ApplyReply reply = first ? firstApplyReply.getOrDefault(to, ApplyReply.Applied) : ApplyReply.Applied;
                sends.add("trackedReply:" + reply + "->" + to);
                onSuccess(args, reply);
            }
            else if (request instanceof WaitUntilApplied)
                onSuccess(args, new ReadData.ReadOk(null, null, 0));
            else
                throw new AssertionError("Unexpected request: " + request);
            return null;
        }).when(msg).send(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        // untracked sends: no callback exists, so the reply would be discarded
        Mockito.doAnswer(args -> {
            Node.Id to = args.getArgument(0);
            Request request = args.getArgument(1);
            if (request instanceof Apply) sends.add("untracked:Apply(" + ((Apply) request).kind + ")");
            else if (request instanceof InformDurable) sends.add("InformDurable");
            else sends.add("untracked:" + request.getClass().getSimpleName() + "->" + to);
            return null;
        }).when(msg).send(Mockito.any(), Mockito.any());
        return msg;
    }

    private static <T> void onSuccess(InvocationOnMock args, T reply)
    {
        Node.Id to = args.getArgument(0);
        AsyncExecutor executor = args.getArgument(2);
        Callback<T> cb = args.getArgument(3);
        executor.execute(() -> cb.onSuccess(to, reply));
    }
}
