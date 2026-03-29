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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import accord.api.MessageSink;
import accord.impl.DefaultTimeouts;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;

import static accord.Utils.createNode;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;
import static accord.utils.Property.qt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PendingEpochsTest
{
    private static final Node.Id ID = new Node.Id(1);

    @Test
    void addEpochsInRandomOrder()
    {
        qt().withExamples(100).check(rs -> {
            TopologyManager manager = createTopologyManager();
            PendingEpochs pendingEpochs = new PendingEpochs(manager);

            // Generate random epoch values
            int numEpochs = rs.nextInt(5, 20);
            List<Long> epochs = new ArrayList<>();
            for (int i = 0; i < numEpochs; i++)
            {
                epochs.add((long) rs.nextInt(1, 100));
            }

            Collections.shuffle(epochs, rs.asJdkRandom());

            for (long epoch : epochs)
            {
                PendingEpoch pendingEpoch = pendingEpochs.getOrCreate(epoch);
                assertNotNull(pendingEpoch, "PendingEpoch should not be null");
                assertEquals(epoch, pendingEpoch.epoch, "Epoch value should match");
            }

            // Verify that all epochs are accessible and in order
            long minEpoch = Collections.min(epochs);
            long maxEpoch = Collections.max(epochs);

            int expectedSize = (int) (maxEpoch - minEpoch + 1);
            assertEquals(expectedSize, pendingEpochs.size(), "Size should equal the range of epochs");
            assertEquals(maxEpoch, pendingEpochs.maxEpoch(), "Max epoch should match");

            for (long epoch = minEpoch; epoch <= maxEpoch; epoch++)
            {
                PendingEpoch pendingEpoch = pendingEpochs.getOrCreate(epoch);
                assertNotNull(pendingEpoch, "PendingEpoch should exist for epoch " + epoch);
                assertEquals(epoch, pendingEpoch.epoch, "Epoch value should match for epoch " + epoch);
            }

            for (int i = 0; i < pendingEpochs.size(); i++)
            {
                PendingEpoch pendingEpoch = pendingEpochs.atIndex(i);
                assertEquals(minEpoch + i, pendingEpoch.epoch, "Epoch at index " + i + " should be " + (minEpoch + i));
            }
        });
    }

    private static TopologyManager createTopologyManager()
    {
        MockCluster.Clock time = new MockCluster.Clock(0);
        Node node = createNode(ID, Topology.EMPTY, new MessageSink.NoOpSink(), time, new TestAgent(time), null);
        return new TopologyManager(SUPPLIER, node, ignore -> {throw new UnsupportedOperationException();}, time, new DefaultTimeouts(time, Runnable::run));
    }
}