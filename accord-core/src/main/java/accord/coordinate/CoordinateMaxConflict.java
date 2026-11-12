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

import accord.api.VisibleForImplementation;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.GetMaxConflict;
import accord.messages.GetMaxConflict.GetMaxConflictOk;
import accord.primitives.FullRoute;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.ActiveEpoch;
import accord.topology.ActiveEpochs;
import accord.topology.Topologies;
import accord.topology.TopologyMismatch;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.topology.SelectShards.ALL;

/**
 * Calculate the maximum TxnId that could have been agreed before this operation started
 */
@VisibleForImplementation
public class CoordinateMaxConflict extends AbstractCoordinatePreAccept<Timestamp, GetMaxConflictOk, Void>
{
    final QuorumTracker tracker;
    Timestamp maxConflict;
    long executionEpoch;

    private CoordinateMaxConflict(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, long executionEpoch, Timestamp maxConflict, BiConsumer<? super Timestamp, Throwable> callback)
    {
        super(node, executor, topologies, route, TxnId.NONE, callback);
        this.maxConflict = maxConflict;
        this.executionEpoch = executionEpoch;
        this.tracker = new QuorumTracker(topologies);
    }

    public static AsyncChain<Timestamp> maxConflict(Node node, Routables<?> keysOrRanges)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super Timestamp, Throwable> callback)
            {
                maxConflict(node, keysOrRanges, callback);
                return null;
            }
        };
    }

    public static void maxConflict(Node node, Routables<?> keysOrRanges, BiConsumer<? super Timestamp, Throwable> callback)
    {
        CoordinateMaxConflict coordinate;
        try
        {
            ActiveEpochs active = node.topology().active();
            long epoch = active.maxEpoch(Long.MIN_VALUE, ActiveEpoch::all, keysOrRanges);
            FullRoute<?> route = node.computeRoute(epoch, keysOrRanges, active);
            Topologies topologies = active.withUnsyncedEpochs(route, epoch, epoch, ALL);
            coordinate = new CoordinateMaxConflict(node, node.someExclusiveExecutor(), topologies, route, epoch, Timestamp.NONE, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        coordinate.start();
    }

    @Override
    void start()
    {
        super.start();
        contact((to, nodeStatus) -> nodeStatus.isUnavailable() ? null : new GetMaxConflict(to, topologies, scope, executionEpoch));
    }

    @Override
    void onSuccessInternal(Node.Id from, int fromIndex, GetMaxConflictOk reply)
    {
        maxConflict = Timestamp.mergeMax(reply.maxConflict, maxConflict);
        executionEpoch = Math.max(executionEpoch, reply.latestEpoch);

        if (tracker.recordSuccess(from) == Success)
            onPreAcceptedOrNewEpoch();
    }

    @Override
    void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        finishOnFailure(mismatch);
    }

    @Override
    long executeAtEpoch()
    {
        return executionEpoch;
    }

    @Override
    void onPreAccepted()
    {
        if (executionEpoch > topologies.currentEpoch())
        {
            queryNewEpoch(executionEpoch);
            return;
        }
        finishWithSuccess(maxConflict);
    }

    /**
     * A replica told us a newer epoch has been established. Any transaction we must respect may have been agreed by a
     * quorum of a newer epoch that does not intersect the quorum of the epoch we queried, so query the quorums of every
     * epoch in between too, carrying forward what we have found so far.
     */
    private void queryNewEpoch(long epoch)
    {
        BiConsumer<? super Timestamp, Throwable> callback = finishAndTakeCallback();
        CoordinateMaxConflict coordinate;
        try
        {
            Topologies next = node.topology().active().withUnsyncedEpochs(scope, topologies.currentEpoch() + 1, epoch, ALL);
            coordinate = new CoordinateMaxConflict(node, executor, next, scope, epoch, maxConflict, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        coordinate.start();
    }

    @Nonnull
    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}
