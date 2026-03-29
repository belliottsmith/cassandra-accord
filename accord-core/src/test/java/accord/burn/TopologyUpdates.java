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

import accord.api.AsyncExecutor;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.MessageTask;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.*;
import java.util.function.Function;

public class TopologyUpdates
{
    private final Long2ObjectHashMap<Map<Node.Id, Ranges>> pendingSyncTopologies = new Long2ObjectHashMap<>();

    Function<Node.Id, AsyncExecutor> executors;
    public TopologyUpdates(Function<Node.Id, AsyncExecutor> executors)
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
        return MessageTask.begin(originator, nodes, executors.apply(originator.id()), "TopologyNotify:" + update.epoch(), (node, from, onDone) -> {
            // used to callback with both false and true, but reports anyway... not clear what intention was, so just reporting success
            node.topology().reportTopology(update);
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
            node.topology().onReadyToCoordinate(originator.id(), epoch);
            onDone.accept(true);
        });
    }

    public synchronized void epochClosed(Node originator, Collection<Node.Id> cluster, Ranges ranges, long epoch)
    {
        executors.apply(originator.id()).execute(() -> {
            MessageTask.begin(originator, cluster, executors.apply(originator.id()), "EpochClosed:" + epoch, (node, from, onDone) -> {
                node.topology().onEpochClosed(ranges, epoch);
                onDone.accept(true);
            });
        });
    }

    public synchronized void epochRetired(Node originator, Collection<Node.Id> cluster, Ranges ranges, long epoch)
    {
        executors.apply(originator.id()).execute(() -> {
            MessageTask.begin(originator, cluster, executors.apply(originator.id()), "EpochComplete:" + epoch, (node, from, onDone) -> {
                node.topology().onEpochRetired(ranges, epoch);
                onDone.accept(true);
            });
        });
    }

    public int pendingTopologies()
    {
        return pendingSyncTopologies.size();
    }
}
