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

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import accord.api.Result;
import accord.impl.IntKey;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.impl.mock.SimpleMessageSink;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.messages.Message;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Shard;

import static accord.Utils.id;
import static accord.Utils.ranges;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChainUtils.getUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A coordinator may ignore a REJECTED PreAccept vote only if it then takes the fast or medium path.
 *
 * 4 nodes, rf=3 (shard replicas R, A, B; fast-path electorate = all 3, fast quorum 3, slow quorum 2); coordinator C is
 * not a replica. Transaction T uses NoMediumPath (TxnId flags 0). Replica R has already witnessed an ExclusiveSyncPoint
 * RX &gt; T (rejectBefore=RX in its MaxConflicts), so it votes REJECTED on T's PreAccept; A and B vote fast. R's reply
 * arrives last. The tracker reaches medium-path criteria, so CoordinateTransaction.onPreAccepted skips the REJECTED
 * check (topologies.size()==1 &amp;&amp; hasMediumPathAccepted), but since T has no medium path it proposes SLOW at the
 * merged executeAt, which still carries REJECTED.
 *
 * Intended: [1] a transaction never commits at an executeAt flagged REJECTED; [2] a later transaction U
 * (RX &lt; U &lt; T.executeAt) that A/B never rejected on their own is not rejected because of T.
 */
public class CoordinateTransactionRejectedSlowPathTest
{
    private static TxnId txnIdOf(Message msg)
    {
        for (Class<?> c = msg.getClass(); c != null; c = c.getSuperclass())
        {
            try
            {
                Field f = c.getDeclaredField("txnId");
                f.setAccessible(true);
                Object v = f.get(msg);
                return v instanceof TxnId ? (TxnId) v : null;
            }
            catch (NoSuchFieldException ignore) {}
            catch (IllegalAccessException e) { return null; }
        }
        return null;
    }

    private static void await(String what, BooleanSupplier cond) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!cond.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                fail("timed out waiting for " + what);
            Thread.sleep(5);
        }
    }

    private static Timestamp executeAtOf(Node node, IntKey.Raw key, TxnId txnId) throws Exception
    {
        CommandStore store = node.unsafeForKey(key.toUnseekable());
        return getUninterruptibly(store.submit(ExecutionContext.unsequenced(txnId, "probe"), safeStore -> {
            SafeCommand c = safeStore.ifInitialised(txnId);
            return c == null ? null : c.current().executeAt();
        }), 5, TimeUnit.SECONDS);
    }

    @Test
    void slowPathAfterIgnoredRejectCommitsRejectedExecuteAt() throws Throwable
    {
        AtomicReference<TxnId> tRef = new AtomicReference<>();
        AtomicReference<Node.Id> rejecterRef = new AtomicReference<>();
        AtomicInteger otherPreAcceptOks = new AtomicInteger();
        AtomicReference<Runnable> heldReply = new AtomicReference<>();
        AtomicReference<Timestamp> rejecterVote = new AtomicReference<>();

        // the rejecting replica R is slower than A/B to answer PreAccept for T: hold its reply until both others replied,
        // then deliver it immediately (all three votes counted, R's last)
        MockCluster.Builder builder = MockCluster.builder().nodes(4).replication(3).messageSink((self, network) -> new SimpleMessageSink(self, network)
        {
            @Override
            public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply, Throwable failure)
            {
                TxnId t = tRef.get();
                if (t != null && reply instanceof PreAcceptOk && ((PreAcceptOk) reply).txnId.equals(t))
                {
                    if (self.equals(rejecterRef.get()))
                    {
                        rejecterVote.set(((PreAcceptOk) reply).witnessedAt);
                        heldReply.set(() -> super.reply(replyingToNode, replyContext, reply, failure));
                        maybeRelease();
                        return;
                    }
                    super.reply(replyingToNode, replyContext, reply, failure);
                    otherPreAcceptOks.incrementAndGet();
                    maybeRelease();
                    return;
                }
                super.reply(replyingToNode, replyContext, reply, failure);
            }

            private void maybeRelease()
            {
                if (otherPreAcceptOks.get() < 2) return;
                Runnable r = heldReply.getAndSet(null);
                if (r != null) r.run();
            }
        });

        try (MockCluster cluster = builder.build())
        {
            IntKey.Raw key = IntKey.key(10);
            Keys keys = Keys.of(key);
            Ranges rxRanges = ranges(range(0, 20));

            // rf=3 of 4 nodes: the coordinator C is NOT a replica of key 10, so every PreAccept vote is a remote reply
            Shard shard = cluster.get(1).topology().current().forKey(key.toUnseekable());
            Node.Id cId = null;
            for (int i = 1; i <= 4; i++)
                if (!shard.nodes.contains(id(i))) cId = id(i);
            Node.Id rId = shard.nodes.get(0), aId = shard.nodes.get(1), bId = shard.nodes.get(2);
            rejecterRef.set(rId);
            Node C = cluster.get(cId), R = cluster.get(rId), A = cluster.get(aId), B = cluster.get(bId);

            // T: NoMediumPath (flags 0), Unoptimised fast path, coordinated by C; its id is allocated first (T < RX)
            TxnId t0 = C.nextTxnIdWithDefaultFlags(keys, Write, Key);
            TxnId T = new TxnId(t0.epoch(), t0.hlc(), 0, Write, Key, t0.node);
            assertFalse(T.hasMediumPath());
            Thread.sleep(5);

            // RX (ExclusiveSyncPoint) coordinated by R; its messages to other nodes are lost, so only R witnesses it
            // (rejectBefore=RX in R's MaxConflicts, SafeCommandStore.updateExclusiveSyncPoint) and only R rejects T
            TxnId rx = R.nextTxnIdWithDefaultFlags(rxRanges, ExclusiveSyncPoint, Range);
            Node.Id rIdF = rId;
            cluster.networkFilter.addFilter(from -> true, to -> !to.equals(rIdF), msg -> rx.equals(txnIdOf(msg)));
            cluster.networkFilter.addFilter(from -> !from.equals(rIdF), to -> true, msg -> rx.equals(txnIdOf(msg)));
            CoordinateSyncPoint.exclusive(R, rx, rxRanges).begin((s, f) -> {});
            CommandStore rStore = R.unsafeForKey(key.toUnseekable());
            await("R to witness RX", () -> rStore.maxConflict(T, keys).is(REJECTED));
            assertFalse(A.unsafeForKey(key.toUnseekable()).maxConflict(T, keys).is(REJECTED));
            assertFalse(B.unsafeForKey(key.toUnseekable()).maxConflict(T, keys).is(REJECTED));
            Thread.sleep(5);

            // U: a later write on the same key, id allocated now (RX < U < T.executeAt), coordinated after T commits
            TxnId U0 = C.nextTxnIdWithDefaultFlags(keys, Write, Key);
            TxnId U = U0;
            assertTrue(U.compareTo(rx) > 0);
            Thread.sleep(5);

            tRef.set(T);
            StringBuilder errors = new StringBuilder();
            boolean tRejected = false;
            try
            {
                Result result = getUninterruptibly(CoordinateTransaction.coordinate(C, T, writeTxn(keys)), 10, TimeUnit.SECONDS);
                assertTrue(result == MockStore.RESULT);
            }
            catch (ExecutionException e)
            {
                if (!(e.getCause() instanceof Invalidated)) throw e;
                tRejected = true; // rejecting T is an acceptable outcome
            }

            if (!tRejected)
            {
                Timestamp executeAt = executeAtOf(A, key, T);
                await("B to learn T's executeAt", () -> {
                    try { Timestamp e = executeAtOf(B, key, T); return e != null && e.equals(executeAt); }
                    catch (Exception ex) { return false; }
                });
                assertTrue(U.compareTo(executeAt) < 0, "U must precede T.executeAt for the second check");
                for (Node n : new Node[]{ A, B })
                {
                    Timestamp mc = n.unsafeForKey(key.toUnseekable()).maxConflict(U, keys);
                }

                // Property 1 (committed_executeat_not_rejected)
                if (executeAt.is(REJECTED))
                    errors.append("[1] T committed on the SLOW path at an executeAt flagged REJECTED: ").append(executeAt).append('\n');
            }

            // Property 2 (no_reject_inherited_from_committed): U, coordinated by C over A+B (R cut off for U),
            // must not be rejected: A and B never witnessed RX, only T.
            cluster.networkFilter.addFilter(from -> true, to -> to.equals(rIdF), msg -> U.equals(txnIdOf(msg)));
            cluster.networkFilter.addFilter(from -> from.equals(rIdF), to -> true, msg -> U.equals(txnIdOf(msg)));
            try
            {
                getUninterruptibly(CoordinateTransaction.coordinate(C, U, writeTxn(keys)), 10, TimeUnit.SECONDS);
            }
            catch (ExecutionException e)
            {
                errors.append("[2] later transaction U=").append(U).append(" was rejected after T committed: ").append(e.getCause()).append('\n');
            }
            if (errors.length() > 0)
                fail(errors.toString());
        }
    }
}
