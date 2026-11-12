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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import accord.api.MessageSink;
import accord.api.TopologyService;
import accord.impl.DefaultTimeouts;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.Utils.createNode;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;

/**
 * Draining pending epochs on a TopologyRetiredException must stop once no pending epochs remain.
 *
 * A node with no active epoch awaits epoch E, which creates PendingEpochs 1..E and fetches each.
 * The fetch of E fails with TopologyRetiredException (what Cassandra's AccordTopologyService
 * returns when the epoch is below its minimum retained epoch). TopologyManager.fetch's drain loop
 *   while (pending.minEpoch() <= pending.epoch) pending.removeFirst(pending.minEpoch());
 * keeps going after PendingEpochs is empty because PendingEpochs.minEpoch() of an empty set is 0,
 * and removeFirst(0) trips Invariants.require(start < end).
 *
 * Intended behaviour: the retired epochs are dropped quietly (the code says "don't retry"),
 * with no exception escaping the fetch callback.
 */
public class TopologyRetiredDrainTest
{
    private static final Node.Id ID = new Node.Id(1);

    private static TopologyManager createTopologyManager(TopologyService service)
    {
        MockCluster.Clock time = new MockCluster.Clock(0);
        Node node = createNode(ID, Topology.EMPTY, new MessageSink.NoOpSink(), time, new TestAgent(time), null);
        return new TopologyManager(SUPPLIER, node, service, time, new DefaultTimeouts(time, Runnable::run));
    }

    private static PendingEpochs pendingOf(TopologyManager tm) throws Exception
    {
        Field f = TopologyManager.class.getDeclaredField("pending");
        f.setAccessible(true);
        return (PendingEpochs) f.get(tm);
    }

    /** Fetch completes asynchronously (the normal Cassandra path: a remote fetch that later fails). */
    @Test
    @Timeout(30)
    void retiredFetchOfHighestPendingEpochAsync() throws Exception
    {
        Map<Long, AsyncResult.Settable<Topology>> fetches = new HashMap<>();
        TopologyManager tm = createTopologyManager(epoch -> fetches.computeIfAbsent(epoch, e -> AsyncResults.settable()));
        Assertions.assertTrue(tm.active().isEmpty());

        long epoch = 3;
        tm.await(epoch, null);
        PendingEpochs pending = pendingOf(tm);
        Assertions.assertEquals(3, pending.size(), "await(3) on an empty node creates pending epochs 1..3");
        Assertions.assertEquals(1, pending.minEpoch());

        // Listener exceptions are routed to the thread's uncaught exception handler (AsyncResults.notify).
        List<Throwable> uncaught = new ArrayList<>();
        Thread thread = Thread.currentThread();
        Thread.UncaughtExceptionHandler prev = thread.getUncaughtExceptionHandler();
        thread.setUncaughtExceptionHandler((t, e) -> uncaught.add(e));
        try
        {
            fetches.get(epoch).setFailure(new TopologyRetiredException(epoch, epoch + 1));
        }
        finally
        {
            thread.setUncaughtExceptionHandler(prev);
        }

        if (!uncaught.isEmpty())
        {
            AssertionError err = new AssertionError("TopologyRetiredException drain loop threw from the fetch callback "
                                                    + "(pending.size()=" + pending.size() + ", minEpoch()=" + pending.minEpoch() + "): " + uncaught.get(0));
            err.initCause(uncaught.get(0));
            throw err;
        }
        Assertions.assertTrue(pending.isEmpty(), "retired epochs <= 3 should have been dropped");
    }

    /** Fetch fails immediately (an already-completed failed AsyncResult): the exception escapes await() itself. */
    @Test
    @Timeout(30)
    void retiredFetchImmediateFailureEscapesAwait() throws Exception
    {
        TopologyManager tm = createTopologyManager(epoch -> AsyncResults.failure(new TopologyRetiredException(epoch, epoch + 1)));
        Assertions.assertTrue(tm.active().isEmpty());
        try
        {
            tm.await(3, null);
        }
        catch (Throwable t)
        {
            AssertionError err = new AssertionError("await(3) threw from the TopologyRetiredException drain loop "
                                                    + "(pending.size()=" + pendingOf(tm).size() + "): " + t);
            err.initCause(t);
            throw err;
        }
    }
}
