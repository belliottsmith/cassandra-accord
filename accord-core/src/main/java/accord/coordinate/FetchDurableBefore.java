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

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.messages.GetDurableBefore;
import accord.messages.GetDurableBefore.DurableBeforeReply;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

public class FetchDurableBefore extends AbstractCoordination<Ranges, FetchDurableBefore.Watermarks, DurableBeforeReply, FetchDurableBefore.Watermarks>
{
    /**
     * The watermarks we collect from a quorum of peers: the merged {@link DurableBefore}, and the highest locally
     * applied bound any of them reported for each range (see {@link GetDurableBefore}).
     */
    public static class Watermarks
    {
        public final DurableBefore durableBefore;
        public final ReducingRangeMap<TxnId> maxLocallyApplied;

        public Watermarks(DurableBefore durableBefore, ReducingRangeMap<TxnId> maxLocallyApplied)
        {
            this.durableBefore = durableBefore;
            this.maxLocallyApplied = maxLocallyApplied;
        }

        @Override
        public String toString()
        {
            return "durableBefore=" + durableBefore + ", maxLocallyApplied=" + maxLocallyApplied;
        }
    }

    final QuorumTracker tracker;

    public FetchDurableBefore(Node node, Topology topology, BiConsumer<? super Watermarks, Throwable> callback)
    {
        super(node, node.someExclusiveExecutor(), TxnId.NONE, topology.ranges(), topology.nodes(), callback);
        this.tracker = new QuorumTracker(new Topologies.Single(node.topology().sorter(), topology));
    }

    void start()
    {
        super.start();
        contact((i1, i2) -> new GetDurableBefore(), id -> !node.id().equals(id));
        executor.executeMaybeImmediately(() -> {
            markSelfContacted();
            onSuccess(node.id(), new DurableBeforeReply(node.durableBefore(), new ReducingRangeMap<>()));
        });
    }

    public static AsyncChain<Watermarks> catchup(Node node)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            public @Nullable Cancellable start(BiConsumer<? super Watermarks, Throwable> callback)
            {
                catchup(node, callback);
                return null;
            }
        };
    }

    public static void catchup(Node node, BiConsumer<? super Watermarks, Throwable> callback)
    {
        Topology topology = node.topology().currentLocal();
        if (topology.ranges().isEmpty())
            callback.accept(new Watermarks(DurableBefore.EMPTY, new ReducingRangeMap<>()), null);
        else
            new FetchDurableBefore(node, topology, callback).start();
    }

    @Override
    void onSuccessInternal(Node.Id from, int fromIndex, DurableBeforeReply reply)
    {
        recordOk(fromIndex, new Watermarks(reply.durableBefore, reply.maxLocallyApplied));
        handle(tracker.recordSuccess(from));
    }

    @Override
    void onFailureInternal(Node.Id from, int fromIndex, Throwable fail)
    {
        recordFailure(fail);
        handle(tracker.recordFailure(from));
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.FetchDurableBefore;
    }

    @Nonnull
    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    private void handle(RequestStatus status)
    {
        switch (status)
        {
            case Success:
                SortedListMap<Node.Id, Watermarks> oks = finishOks();
                DurableBefore durableBefore = oks.foldlNonNullValues((w, prev) -> DurableBefore.merge(prev, w.durableBefore), DurableBefore.EMPTY);
                ReducingRangeMap<TxnId> maxLocallyApplied = oks.foldlNonNullValues((w, prev) -> ReducingRangeMap.merge(prev, w.maxLocallyApplied, Timestamp::nonNullOrMax), new ReducingRangeMap<>());
                finishWithSuccess(new Watermarks(durableBefore, maxLocallyApplied));
                break;
            case Failed:
                finishOnFailure();
                break;
            case NoChange:
                break;
        }
    }
}
