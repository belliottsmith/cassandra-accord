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
import accord.messages.Callback;
import accord.primitives.FullRoute;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.WrappableException;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;

/**
 * Abstract parent class for implementing preaccept-like operations where we may need to fetch additional replies
 * from future epochs.
 */
abstract class AbstractCoordinatePreAccept<T, R> extends SettableResult<T> implements Callback<R>, BiConsumer<T, Throwable>
{
    final Node node;
    final TxnId txnId;
    final FullRoute<?> route;

    final Topologies topologies;
    private boolean isDone;

    AbstractCoordinatePreAccept(Node node, FullRoute<?> route, @Nonnull TxnId txnId)
    {
        this(node, route, txnId, node.topology().select(route, txnId, txnId, QuorumEpochIntersections.preaccept.include));
    }

    AbstractCoordinatePreAccept(Node node, FullRoute<?> route, @Nonnull TxnId txnId, Topologies topologies)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.topologies = topologies;
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
    public synchronized final void onFailure(Id from, Throwable failure)
    {
        if (!isDone)
            onFailureInternal(from, failure);
    }

    @Override
    public final synchronized boolean onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone) return false;
        isDone = true;
        return tryFailure(failure);
    }

    @Override
    // TODO (expected): shouldn't need synchronized
    public final synchronized void onSuccess(Id from, R reply)
    {
        if (!isDone)
            onSuccessInternal(from, reply);
    }

    @Override
    public final synchronized void onSlowResponse(Id from)
    {
        if (!isDone)
            onSlowResponseInternal(from);
    }

    @Override
    public final boolean tryFailure(Throwable failure)
    {
        if (!super.tryFailure(failure))
            return false;
        onFailure(failure);
        return true;
    }

    private void onFailure(Throwable failure)
    {
        // we may already be complete, as we may receive a failure from a later phase; but it's fine to redundantly mark done
        isDone = true;
        if (failure instanceof CoordinationFailed)
        {
            ((CoordinationFailed) failure).set(txnId, route.homeKey());
            if (failure instanceof Timeout)
                node.agent().metricsEventsListener().onTimeout(txnId);
            else if (failure instanceof Preempted)
                node.agent().metricsEventsListener().onPreempted(txnId);
            else if (failure instanceof Invalidated)
                node.agent().metricsEventsListener().onInvalidated(txnId);
        }
    }

    final void onPreAcceptedOrNewEpoch()
    {
        Invariants.checkState(!isDone);
        isDone = true;
        long latestEpoch = executeAtEpoch();
        if (latestEpoch > topologies.currentEpoch()) node.withEpoch(latestEpoch, this, () -> onPreAcceptedInNewEpoch(topologies, latestEpoch));
        else onPreAccepted(topologies);
    }

    final void onPreAcceptedInNewEpoch(Topologies topologies, long latestEpoch)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(latestEpoch), txnId, route.homeKey(), route);
        if (mismatch == null) onPreAccepted(topologies);
        else onNewEpochTopologyMismatch(mismatch);
    }

    @Override
    public final void accept(T success, Throwable failure)
    {
        if (success != null) trySuccess(success);
        else tryFailure(WrappableException.wrap(failure));
    }
}
