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

import accord.api.VisibleForImplementation;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.GetMaxConflict;
import accord.messages.GetMaxConflict.GetMaxConflictOk;
import accord.primitives.FullRoute;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableByCallback;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Calculate the maximum TxnId that could have been agreed before this operation started
 */
@VisibleForImplementation
public class CoordinateMaxConflict extends AbstractCoordinatePreAccept<Timestamp, GetMaxConflictOk>
{
    final QuorumTracker tracker;
    Timestamp maxConflict;
    long executionEpoch;

    private CoordinateMaxConflict(Node node, FullRoute<?> route, long executionEpoch, BiConsumer<Timestamp, Throwable> callback)
    {
        this(node, route, executionEpoch, node.topology().withUnsyncedEpochs(route, executionEpoch, executionEpoch), callback);
    }

    private CoordinateMaxConflict(Node node, FullRoute<?> route, long executionEpoch, Topologies topologies, BiConsumer<Timestamp, Throwable> callback)
    {
        super(node, route, null, topologies, callback);
        this.maxConflict = Timestamp.NONE;
        this.executionEpoch = executionEpoch;
        this.tracker = new QuorumTracker(topologies);
    }

    public static AsyncResult<Timestamp> maxConflict(Node node, Routables<?> keysOrRanges)
    {
        long epoch = node.epoch();
        FullRoute<?> route = node.computeRoute(epoch, keysOrRanges);
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(epoch), null, route.homeKey(), keysOrRanges);
        if (mismatch != null)
            return AsyncResults.failure(mismatch);

        SettableByCallback<Timestamp> result = new SettableByCallback<>();
        CoordinateMaxConflict coordinate = new CoordinateMaxConflict(node, route, epoch, result);
        coordinate.start();
        return result;
    }

    @Override
    void contact(Collection<Node.Id> nodes, Topologies topologies, Callback<GetMaxConflictOk> callback)
    {
        node.send(nodes, to -> new GetMaxConflict(to, topologies, route, executionEpoch), callback);
    }

    @Override
    void onSuccessInternal(Node.Id from, GetMaxConflictOk reply)
    {
        maxConflict = Timestamp.max(reply.maxConflict, maxConflict);
        executionEpoch = Math.max(executionEpoch, reply.latestEpoch);

        if (tracker.recordSuccess(from) == Success)
            onPreAcceptedOrNewEpoch();
    }

    @Override
    void onFailureInternal(Node.Id from, Throwable failure)
    {
        if (tracker.recordFailure(from) == Failed)
            setFailure(failure);
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        setFailure(mismatch);
    }

    @Override
    long executeAtEpoch()
    {
        return executionEpoch;
    }

    @Override
    void onPreAccepted(Topologies topologies)
    {
        callback.accept(maxConflict, null);
    }
}
