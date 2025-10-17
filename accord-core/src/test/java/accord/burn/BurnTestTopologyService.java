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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.AsyncExecutor;
import accord.api.TopologyListener;
import accord.api.TopologyService;
import accord.coordinate.Exhausted;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Ranges;
import accord.topology.ActiveEpoch;
import accord.topology.Topology;
import accord.utils.RandomSource;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public class BurnTestTopologyService implements TopologyService, TopologyListener
{
    private final Node.Id self;
    private final AsyncExecutor executor;
    private final Agent agent;
    private final Function<Node.Id, Node> lookup;
    private final Supplier<RandomSource> randomSupplier;
    private final TopologyUpdates topologyUpdates;
    private final Topology initialTopology;

    public BurnTestTopologyService(Node.Id self, AsyncExecutor executor, Agent agent, Supplier<RandomSource> randomSupplier, Topology topology, Function<Node.Id, Node> lookup, TopologyUpdates topologyUpdates)
    {
        this.self = self;
        this.executor = executor;
        this.agent = agent;
        this.randomSupplier = randomSupplier;
        this.lookup = lookup;
        this.topologyUpdates = topologyUpdates;
        this.initialTopology = topology;
    }

    @Override
    public void onStartup(Node node)
    {
        node.topology().reportTopology(initialTopology);
        node.topology().addListener(this);
    }

    private static class FetchTopologyRequest implements Request
    {
        private final long epoch;

        public FetchTopologyRequest(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            Topology topology = on.topology().active().maybeGlobalForEpoch(epoch);
            on.reply(from, replyContext, new FetchTopologyReply(topology), null);
        }

        @Override
        public MessageType type()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "FetchTopologyRequest{" + epoch + '}';
        }
    }

    private static class FetchTopologyReply implements Reply
    {
        public final Topology topology;

        public FetchTopologyReply(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public MessageType type()
        {
            return null;
        }

        @Override
        public String toString()
        {
            String epoch = topology == null ? "null" : Long.toString(topology.epoch());
            return "FetchTopologyReply{" + epoch + '}';
        }
    }

    private class FetchTopology extends AsyncResults.SettableResult<Topology> implements Callback<FetchTopologyReply>
    {
        private final FetchTopologyRequest request;
        private final List<Node.Id> candidates;

        public FetchTopology(long epoch, List<Node.Id> candidates)
        {
            this.request = new FetchTopologyRequest(epoch);
            this.candidates = new ArrayList<>(candidates);
            executor.execute(this::trySendNext);
        }

        void trySendNext()
        {
            if (candidates.isEmpty())
            {
                tryFailure(Exhausted.exhausted(agent, null, null, null));
                return;
            }

            int idx = randomSupplier.get().nextInt(candidates.size());
            Node.Id node = candidates.remove(idx);
            originator().send(node, request, executor, this);
        }

        @Override
        public void onSuccess(Node.Id from, FetchTopologyReply reply)
        {
            if (reply.topology != null) trySuccess(reply.topology);
            else trySendNext();
        }

        @Override
        public void onFailure(Node.Id from, Throwable failure)
        {
            trySendNext();
        }

        @Override
        public boolean onCallbackFailure(Node.Id from, Throwable failure)
        {
            return tryFailure(failure);
        }
    }

    public AsyncResult<Topology> fetchTopologyForEpoch(long epoch)
    {
        List<Node.Id> ids = lookup.apply(self).topology().current().nodes();
        if (ids.isEmpty())
            ids = initialTopology.nodes();
        return new FetchTopology(epoch, ids);
    }

    @Override
    public void onActive(ActiveEpoch active)
    {
        active.epochReady().coordinate.invokeIfSuccess(() -> {
            topologyUpdates.syncComplete(lookup.apply(self), active.global().nodes(), active.global().epoch());
        });
    }

    private Node originator()
    {
        return lookup.apply(self);
    }

    @Override
    public void onEpochClosed(Ranges ranges, long epoch, @Nullable Topology topology)
    {
        if (topology != null)
            topologyUpdates.epochClosed(lookup.apply(self), topology.nodes(), ranges, topology.epoch());
    }

    @Override
    public void onEpochRetired(Ranges ranges, long epoch, @Nullable Topology topology)
    {
        if (topology != null)
            topologyUpdates.epochRetired(lookup.apply(self), topology.nodes(), ranges, topology.epoch());
    }
}
