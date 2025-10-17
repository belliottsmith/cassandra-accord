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

package accord.impl.mock;

import accord.api.TopologyListener;
import accord.api.TopologyService;
import accord.local.Node;
import accord.topology.ActiveEpoch;
import accord.topology.EpochReady;
import accord.topology.Topology;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import org.junit.jupiter.api.Assertions;

import java.util.*;
import java.util.function.LongFunction;

public class MockTopologyService implements TopologyService, TopologyListener
{
    private final List<Topology> epochs = new ArrayList<>();
    private final Map<Long, EpochReady> acks = new HashMap<>();
    private final LongFunction<Topology> fetch;

    public MockTopologyService(LongFunction<Topology> fetch, Topology initialTopology)
    {
        this.fetch = fetch;
        if (initialTopology.epoch() > 0)
            epochs.add(initialTopology);
    }

    @Override
    public void onStartup(Node node)
    {
        node.topology().addListener(this);
        for (Topology topology : epochs)
            node.topology().reportTopology(topology);
    }

    public synchronized Topology getTopologyForEpoch(long epoch)
    {
        return epoch >= epochs.size() ? null : epochs.get((int) epoch);
    }

    @Override
    public synchronized AsyncResult<Topology> fetchTopologyForEpoch(long epoch)
    {
        if (epoch < epochs.size())
            return AsyncResults.success(getTopologyForEpoch(epoch));

        Topology topology = fetch.apply(epoch);
        if (topology == null)
            throw new IllegalStateException();
        epochs.add(topology);
        return AsyncResults.success(topology);
    }

    @Override
    public void onActive(ActiveEpoch epoch)
    {
        Assertions.assertFalse(acks.containsKey(epoch.epoch()));
        acks.put(epoch.epoch(), epoch.epochReady());
    }

    public synchronized EpochReady ackFor(long epoch)
    {
        return acks.get(epoch);
    }
}
