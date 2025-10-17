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

package accord.maelstrom;

import accord.api.TopologyService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public class SimpleTopologyService implements TopologyService
{
    private final Topology topology;

    public SimpleTopologyService(Topology topology)
    {
        this.topology = topology;
    }

    @Override
    public AsyncResult<Topology> fetchTopologyForEpoch(long epoch)
    {
        Invariants.require(epoch == topology.epoch());
        return AsyncResults.success(topology);
    }

    @Override
    public void onStartup(Node node)
    {
        node.topology().reportTopology(topology);
    }
}

