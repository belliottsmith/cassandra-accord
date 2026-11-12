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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.MessageSink;
import accord.impl.DefaultTimeouts;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topology;
import static accord.impl.IntKey.range;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;

/**
 * Ready, closed and retired notifications for an epoch being activated must not be lost.
 *
 * TopologyManager.updateActive advances pending.minPermitted to E+1 under the lock
 *, then runs CommandStores.updateTopology WITHOUT the lock
 *, and only afterwards publishes ActiveEpoch(E), copying
 * pending.closed / pending.retired / pending.ready.
 * A remote notification for epoch E arriving on another thread inside that window
 * (onReadyToCoordinate / onEpochRetired / onEpochClosed are invoked by message
 * handlers on arbitrary threads) is neither recorded in PendingEpoch(E)
 * (getOrCreateIfPermitted(E) returns null, PendingEpochs) nor in
 * ActiveEpoch(E) (it does not exist yet; ActiveEpochs applies it to E-1 only).
 *
 * To land a notification deterministically inside the window, the test passes a
 * ShardDistributor (a real Node constructor parameter) that CommandStores.updateTopology
 * calls while splitting newly added local ranges. The
 * distributor parks the reporting thread there; the main thread then delivers the
 * notification exactly as a concurrent message handler would, and releases it.
 * Each baseline test delivers the same notification after publication and shows
 * the expected outcome; each race test shows the notification is lost.
 */
public class TopologyManagerPublishWindowTest
{
    private static final Node.Id ID = new Node.Id(1);
    private static final Range R1 = range(100, 200);
    private static final Range R2 = range(200, 300);

    /** Parks the thread that calls split() once armed, until released. */
    static class GatedDistributor implements ShardDistributor
    {
        final ShardDistributor delegate = new ShardDistributor.EvenSplit<>(8, ignore -> new IntKey.Splitter());
        volatile boolean armed;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public List<Ranges> split(Ranges ranges)
        {
            if (armed)
            {
                armed = false;
                entered.countDown();
                try
                {
                    if (!release.await(10, TimeUnit.SECONDS))
                        throw new IllegalStateException("gate never released");
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return delegate.split(ranges);
        }

        @Override
        public @Nullable Range splitRange(Range range, int from, int to, int totalSplits)
        {
            return delegate.splitRange(range, from, to, totalSplits);
        }

        @Override
        public Ranges selectFirstSubRanges(Range range, Ranges subRanges, int totalSplits)
        {
            return delegate.selectFirstSubRanges(range, subRanges, totalSplits);
        }

        @Override
        public int numberOfSplitsPossible(Range range)
        {
            return delegate.numberOfSplitsPossible(range);
        }
    }

    private static TopologyManager createTopologyManager(ShardDistributor distributor)
    {
        MockCluster.Clock time = new MockCluster.Clock(0);
        Node node = createNode(ID, Topology.EMPTY, new MessageSink.NoOpSink(), time, new TestAgent(time), distributor);
        return new TopologyManager(SUPPLIER, node, ignore -> { throw new UnsupportedOperationException(); }, time, new DefaultTimeouts(time, Runnable::run));
    }

    // epoch 1: node 1 replicates R1; epoch 2: node 1 additionally gains R2 (so CommandStores splits added ranges)
    private static Topology t1() { return topology(1, shard(R1, idList(1, 2, 3), idSet(1, 2, 3))); }
    private static Topology t2() { return topology(2, shard(R1, idList(1, 2, 3), idSet(1, 2, 3)),
                                                      shard(R2, idList(1, 2, 3), idSet(1, 2, 3))); }

    /** report t2 on another thread, deliver {@code notify} while that thread is inside the publish window */
    private static TopologyManager raceEpoch2(Consumer<TopologyManager> notify) throws Throwable
    {
        GatedDistributor gate = new GatedDistributor();
        TopologyManager tm = createTopologyManager(gate);
        tm.reportTopology(t1());
        gate.armed = true;

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reporter = new Thread(() -> {
            try { tm.reportTopology(t2()); }
            catch (Throwable t) { failure.set(t); }
        }, "topology-reporter");
        reporter.setDaemon(true);
        reporter.start();

        Assertions.assertTrue(gate.entered.await(10, TimeUnit.SECONDS), "reporter never reached CommandStores.updateTopology");
        // we are now between setMinPermitted(2) and publication of ActiveEpoch(2)
        Assertions.assertEquals(1, tm.epoch());
        notify.accept(tm);
        gate.release.countDown();
        reporter.join(10_000);
        Assertions.assertFalse(reporter.isAlive(), "reporter did not finish");
        if (failure.get() != null) throw failure.get();
        Assertions.assertEquals(2, tm.epoch());
        return tm;
    }

    private static TopologyManager afterPublish(Consumer<TopologyManager> notify)
    {
        TopologyManager tm = createTopologyManager(new GatedDistributor());
        tm.reportTopology(t1());
        tm.reportTopology(t2());
        Assertions.assertEquals(2, tm.epoch());
        notify.accept(tm);
        return tm;
    }

    private static final Consumer<TopologyManager> RETIRE_R1_AT_2 = tm -> tm.onEpochRetired(Ranges.of(R1), 2);
    private static final Consumer<TopologyManager> CLOSE_R1_AT_2 = tm -> tm.onEpochClosed(Ranges.of(R1), 2);
    private static final Consumer<TopologyManager> READY_2_3_AT_2 = tm -> { tm.onReadyToCoordinate(id(2), 2); tm.onReadyToCoordinate(id(3), 2); };

    // ---- TEB-P2 (representative): retired notification lost ----

    @Test
    void retiredBaselineAfterPublish()
    {
        TopologyManager tm = afterPublish(RETIRE_R1_AT_2);
        Assertions.assertTrue(tm.unsafeGetActiveEpoch(2).retired().containsAll(Ranges.of(R1)),
                              "baseline: epoch 2 retired=" + tm.unsafeGetActiveEpoch(2).retired());
    }

    @Test
    void retiredDuringPublishWindowIsLost() throws Throwable
    {
        TopologyManager tm = raceEpoch2(RETIRE_R1_AT_2);
        Ranges retired = tm.unsafeGetActiveEpoch(2).retired();
        Assertions.assertTrue(retired.containsAll(Ranges.of(R1)),
                              "onEpochRetired(" + R1 + ", 2) delivered during the publish window was lost: epoch 2 retired=" + retired);
    }

    // ---- TEB-P1: ready-to-coordinate notifications lost ----

    @Test
    void readyBaselineAfterPublish()
    {
        TopologyManager tm = afterPublish(READY_2_3_AT_2);
        Assertions.assertTrue(tm.unsafeGetActiveEpoch(2).quorumReady().containsAll(Ranges.of(R1)),
                              "baseline: epoch 2 quorumReady=" + tm.unsafeGetActiveEpoch(2).quorumReady());
    }

    @Test
    void readyDuringPublishWindowIsLost() throws Throwable
    {
        TopologyManager tm = raceEpoch2(READY_2_3_AT_2);
        Ranges quorumReady = tm.unsafeGetActiveEpoch(2).quorumReady();
        Assertions.assertTrue(quorumReady.containsAll(Ranges.of(R1)),
                              "onReadyToCoordinate(2|3, 2) delivered during the publish window was lost: epoch 2 quorumReady=" + quorumReady);
    }

    // ---- TEB-P3: closed notification reported to listeners but not recorded ----

    @Test
    void closedBaselineAfterPublish()
    {
        TopologyManager tm = afterPublish(CLOSE_R1_AT_2);
        Assertions.assertTrue(tm.unsafeGetActiveEpoch(2).closed().containsAll(Ranges.of(R1)),
                              "baseline: epoch 2 closed=" + tm.unsafeGetActiveEpoch(2).closed());
    }

    @Test
    void closedDuringPublishWindowIsLost() throws Throwable
    {
        TopologyManager tm = raceEpoch2(CLOSE_R1_AT_2);
        Ranges closed = tm.unsafeGetActiveEpoch(2).closed();
        Assertions.assertTrue(closed.containsAll(Ranges.of(R1)),
                              "onEpochClosed(" + R1 + ", 2) delivered during the publish window was lost: epoch 2 closed=" + closed);
    }
}
