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

import java.util.Collection;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Callback;
import accord.primitives.FullRoute;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.WrappableException;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

/**
 * Abstract parent class for implementing preaccept-like operations where we may need to fetch additional replies
 * from future epochs.
 */
abstract class AbstractCoordinatePreAccept<T, R> implements Callback<R>
{
    final Node node;
    final SequentialAsyncExecutor executor;
    final TxnId txnId;
    final FullRoute<?> route;

    final Topologies topologies;
    final BiConsumer<T, Throwable> callback;
    private boolean isDone;

    AbstractCoordinatePreAccept(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, @Nonnull TxnId txnId, BiConsumer<T, Throwable> callback)
    {
        this(node, executor, route, txnId, node.topology().select(route, txnId, txnId, SHARE, QuorumEpochIntersections.preaccept.include), callback);
    }

    AbstractCoordinatePreAccept(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, @Nonnull TxnId txnId, Topologies topologies, BiConsumer<T, Throwable> callback)
    {
        this.node = node;
        this.executor = executor;
        this.txnId = txnId;
        this.route = route;
        this.topologies = topologies;
        this.callback = callback;
    }

    void start()
    {
        contact(topologies.nodes(), topologies, this);
    }

    abstract void contact(Collection<Id> nodes, Topologies topologies, Callback<R> callback);
    abstract void onSuccessInternal(Id from, R reply);
    void onSlowResponseInternal(Id from) {}
    abstract void onFailureInternal(Id from, Throwable failure);
    abstract void onNewEpochTopologyMismatch(TopologyMismatch mismatch);
    abstract void onPreAccepted(Topologies topologies);
    abstract long executeAtEpoch();

    @Override
    public final void onFailure(Id from, Throwable failure)
    {
        if (!isDone)
            onFailureInternal(from, failure);
    }

    @Override
    public final boolean onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone) return false;
        isDone = true;
        callback.accept(null, failure);
        return true;
    }

    @Override
    public final void onSuccess(Id from, R reply)
    {
        if (!isDone)
            onSuccessInternal(from, reply);
    }

    @Override
    public final void onSlowResponse(Id from)
    {
        if (!isDone)
            onSlowResponseInternal(from);
    }

    void setFailure(Throwable failure)
    {
        Invariants.require(!isDone);
        // we may already be complete, as we may receive a failure from a later phase; but it's fine to redundantly mark done
        isDone = true;
        callback.accept(null, failure);
    }

    final void onPreAcceptedOrNewEpoch()
    {
        Invariants.require(!isDone);
        isDone = true;
        long latestEpoch = executeAtEpoch();
        if (latestEpoch > topologies.currentEpoch()) node.withEpochExact(latestEpoch, executor, callback, t -> WrappableException.wrap(t), () -> onPreAcceptedInNewEpoch(topologies, latestEpoch));
        else onPreAccepted(topologies);
    }

    final void onPreAcceptedInNewEpoch(Topologies topologies, long latestEpoch)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(latestEpoch), txnId, route.homeKey(), route);
        if (mismatch == null) onPreAccepted(topologies);
        else onNewEpochTopologyMismatch(mismatch);
    }
}
