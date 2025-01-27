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

package accord.burn;

import accord.api.TestableConfigurationService;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.MessageTask;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class TopologyUpdates
{
    private final Long2ObjectHashMap<Map<Node.Id, Ranges>> pendingSyncTopologies = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Map<Node.Id, Ranges>> pendingBootstrap = new Long2ObjectHashMap<>();

    Function<Node.Id, AgentExecutor> executors;
    public TopologyUpdates(Function<Node.Id, AgentExecutor> executors)
    {
        this.executors = executors;
    }

    public synchronized MessageTask notify(Node originator, Topology prev, Topology update)
    {
        Set<Node.Id> nodes = new TreeSet<>(prev.nodes());
        nodes.addAll(update.nodes());
        Map<Node.Id, Ranges> nodeToNewRanges = new HashMap<>();
        for (Node.Id node : nodes)
        {
            Ranges newRanges = update.rangesForNode(node).without(prev.rangesForNode(node));
            nodeToNewRanges.put(node, newRanges);
        }
        pendingSyncTopologies.put(update.epoch(), nodeToNewRanges);
        pendingBootstrap.put(update.epoch(), new HashMap<>(nodeToNewRanges));
        return MessageTask.begin(originator, nodes, executors.apply(originator.id()), "TopologyNotify:" + update.epoch(), (node, from, onDone) -> {
            long nodeEpoch = node.epoch();
            if (nodeEpoch + 1 < update.epoch())
                onDone.accept(false);
            ((TestableConfigurationService) node.configService()).reportTopology(update);
            onDone.accept(true);
        });
    }

    public synchronized void syncComplete(Node originator, Collection<Node.Id> cluster, long epoch)
    {
        if (pendingSyncTopologies.isEmpty())
            return;

        Map<Node.Id, Ranges> pending = pendingSyncTopologies.get(epoch);
        Invariants.require(pending != null && pending.remove(originator.id()) != null);

        if (pending.isEmpty())
            pendingSyncTopologies.remove(epoch);

        MessageTask.begin(originator, cluster, executors.apply(originator.id()), "SyncComplete:" + epoch, (node, from, onDone) -> {
            node.onRemoteSyncComplete(originator.id(), epoch);
            onDone.accept(true);
        });
    }

    public synchronized void bootstrapComplete(Node originator, long epoch)
    {
        if (pendingBootstrap.isEmpty())
            return;

        Map<Node.Id, Ranges> pending = pendingBootstrap.get(epoch);
        Invariants.require(pending != null && pending.remove(originator.id()) != null);

        if (pending.isEmpty())
            pendingBootstrap.remove(epoch);
    }

    public synchronized void epochClosed(Node originator, Collection<Node.Id> cluster, Ranges ranges, long epoch)
    {
        executors.apply(originator.id()).execute(() -> {
            MessageTask.begin(originator, cluster, executors.apply(originator.id()), "EpochClosed:" + epoch, (node, from, onDone) -> {
                node.onEpochClosed(ranges, epoch);
                onDone.accept(true);
            });
        });
    }

    public synchronized void epochRedundant(Node originator, Collection<Node.Id> cluster, Ranges ranges, long epoch)
    {
        executors.apply(originator.id()).execute(() -> {
            MessageTask.begin(originator, cluster, executors.apply(originator.id()), "EpochComplete:" + epoch, (node, from, onDone) -> {
                node.onEpochRetired(ranges, epoch);
                onDone.accept(true);
            });
        });
    }

    public boolean isPending(Range range, Node.Id id)
    {
        Predicate<Map<Node.Id, Ranges>> test = map -> {
            Ranges rs = map.get(id);
            return rs != null && rs.intersects(range);
        };
        return pendingSyncTopologies.values().stream().anyMatch(test)
               || pendingBootstrap.values().stream().anyMatch(test);
    }

    public int pendingTopologies()
    {
        return pendingSyncTopologies.size();
    }
}
