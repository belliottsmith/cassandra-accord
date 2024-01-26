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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.primitives.Timestamp.mergeMax;
import static accord.utils.Functions.foldl;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
abstract class AbstractCoordinatePreAccept<T, R> extends SettableResult<T> implements Callback<R>, BiConsumer<T, Throwable>
{
    class ExtraEpochs implements Callback<R>
    {
        final QuorumTracker tracker;
        private boolean extraRoundIsDone;

        ExtraEpochs(long fromEpoch, long toEpoch)
        {
            Topologies topologies = node.topology().preciseEpochs(route, fromEpoch, toEpoch);
            this.tracker = new QuorumTracker(topologies);
        }

        void start()
        {
            // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
            // note that we must send to all replicas of old topology, as electorate may not be reachable
            contact(tracker.topologies().nodes(), this);
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            synchronized (AbstractCoordinatePreAccept.this)
            {
                if (!extraRoundIsDone && tracker.recordFailure(from) == Failed)
                    setFailure(failure);
            }
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            AbstractCoordinatePreAccept.this.onCallbackFailure(from, failure);
        }

        @Override
        public void onSuccess(Id from, R reply)
        {
            synchronized (AbstractCoordinatePreAccept.this)
            {
                if (!extraRoundIsDone)
                    onExtraSuccessInternal(from, reply);
            }
        }
    }

    final Node node;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;

    private Topologies topologies;
    final List<R> oks;
    private boolean initialRoundIsDone;
    private ExtraEpochs extraEpochs;
    private Map<Id, Object> debug = Invariants.debug() ? new LinkedHashMap<>() : null;

    AbstractCoordinatePreAccept(Node node, FullRoute<?> route, TxnId txnId, Txn txn)
    {
        this(node, txnId, txn, route, node.topology().withUnsyncedEpochs(route, txnId, txnId));
    }

    AbstractCoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Topologies topologies)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.topologies = topologies;
        this.oks = new ArrayList<>(topologies.estimateUniqueNodes());
    }

    void start()
    {
        contact(topologies.nodes(), this);
    }

    abstract void contact(Set<Id> nodes, Callback<R> callback);
    abstract void onSuccessInternal(Id from, R reply);
    abstract void onExtraSuccessInternal(Id from, R reply);
    abstract void onFailureInternal(Id from, Throwable failure);
    abstract void onNewEpochTopologyMismatch();
    abstract void onPreAccepted(Topologies topologies, Timestamp executeAt, List<R> oks);

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (debug != null) debug.putIfAbsent(from, failure);
        if (!initialRoundIsDone)
            onFailureInternal(from, failure);
    }

    @Override
    public final synchronized void onCallbackFailure(Id from, Throwable failure)
    {
        initialRoundIsDone = true;
        if (extraEpochs != null)
            extraEpochs.extraRoundIsDone = true;

        tryFailure(failure);
    }

    @Override
    public final synchronized void onSuccess(Id from, R reply)
    {
        if (debug != null) debug.putIfAbsent(from, reply);
        if (!initialRoundIsDone)
            onSuccessInternal(from, reply);

    }

    @Override
    public void setFailure(Throwable failure)
    {
        Invariants.checkState(!initialRoundIsDone || (extraEpochs != null && !extraEpochs.extraRoundIsDone));
        initialRoundIsDone = true;
        if (extraEpochs != null)
            extraEpochs.extraRoundIsDone = true;

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
        super.setFailure(failure);
    }

    void onPreAcceptedOrNewEpoch()
    {
        Invariants.checkState(!initialRoundIsDone || (extraEpochs != null && !extraEpochs.extraRoundIsDone));
        initialRoundIsDone = true;
        if (extraEpochs != null)
            extraEpochs.extraRoundIsDone = true;

        // if the epoch we are accepting in is later, we *must* contact the later epoch for pre-accept, as this epoch
        // could have moved ahead, and the timestamp we may propose may be stale.
        // Note that these future epochs are non-voting, they only serve to inform the timestamp we decide
        Timestamp executeAt = foldl(oks, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (executeAt.epoch() <= topologies.currentEpoch())
            onPreAccepted(topologies, executeAt, oks);
        else
            onNewEpoch(topologies, executeAt, oks);
    }

    void onNewEpoch(Topologies prevTopologies, Timestamp executeAt, List<R> successes)
    {
        // TODO (desired, efficiency): check if we have already have a valid quorum for the future epoch
        //  (noting that nodes may have adopted new ranges, in which case they should be discounted, and quorums may have changed shape)
        node.withEpoch(executeAt.epoch(), () -> {
            TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(executeAt.epoch()), txnId, route.homeKey(), txn.keys());
            if (mismatch != null)
            {
                initialRoundIsDone = true;
                onNewEpochTopologyMismatch();
                return;
            }
            topologies = node.topology().withUnsyncedEpochs(route, txnId.epoch(), executeAt.epoch());
            boolean equivalent = topologies.oldestEpoch() <= prevTopologies.currentEpoch();
            for (long epoch = topologies.currentEpoch() ; equivalent && epoch > prevTopologies.currentEpoch() ; --epoch)
                equivalent = topologies.forEpoch(epoch).shards().equals(prevTopologies.current().shards());

            if (equivalent)
            {
                onPreAccepted(topologies, executeAt, oks);
            }
            else
            {
                extraEpochs = new ExtraEpochs(prevTopologies.currentEpoch() + 1, executeAt.epoch());
                extraEpochs.start();
            }
        });
    }

    @Override
    public void accept(T success, Throwable failure)
    {
        if (success != null)
        {
            trySuccess(success);
        }
        else
        {
            if (failure instanceof CoordinationFailed)
            {
                ((CoordinationFailed) failure).set(txnId, route.homeKey());
                if (failure instanceof Preempted)
                    node.agent().metricsEventsListener().onPreempted(txnId);
                else if (failure instanceof Timeout)
                    node.agent().metricsEventsListener().onTimeout(txnId);
                else if (failure instanceof Invalidated)
                    node.agent().metricsEventsListener().onInvalidated(txnId);
            }
            tryFailure(failure);
        }
    }
}
