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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.ConfigurationService;
import accord.api.VisibleForImplementation;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public abstract class AbstractConfigurationService<EpochState extends AbstractConfigurationService.AbstractEpochState,
                                                   EpochHistory extends AbstractConfigurationService.AbstractEpochHistory<EpochState>>
                      implements ConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfigurationService.class);

    protected final Node.Id localId;
    protected final Agent agent;

    protected final EpochHistory epochs = createEpochHistory();

    protected final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public abstract static class AbstractEpochState
    {
        protected final long epoch;
        protected final AsyncResult.Settable<Topology> received = AsyncResults.settable();
        protected final AsyncResult.Settable<Void> acknowledged = AsyncResults.settable();
        @GuardedBy("AbstractEpochHistory.this")
        protected EpochReady ready = null;
        @GuardedBy("AbstractEpochHistory.this")
        protected Topology topology = null;

        public AbstractEpochState(long epoch)
        {
            this.epoch = epoch;
        }

        public long epoch()
        {
            return epoch;
        }

        public EpochReady ready()
        {
            return ready;
        }

        @Override
        public String toString()
        {
            return "EpochState{" + epoch + '}';
        }

        public String toDebugString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("EpochState{").append(epoch);
            addDebugString(sb);
            sb.append('}');
            return sb.toString();
        }

        protected void addDebugString(StringBuilder sb)
        {
            sb.append(" received ")
              .append(received)
              .append(" acknowledged ")
              .append(acknowledged)
              .append(" ready ")
              .append(ready);
        }

        @VisibleForTesting
        public synchronized void setReadyForTesting(Topology topology)
        {
            this.topology = topology;
            received.setSuccess(topology);
            acknowledged.setSuccess(null);
            ready = EpochReady.done(topology.epoch());
        }
    }

    /**
     * Thread safety is managed by a synchronized lock on this object, and extending classes can use the same lock when needed.
     *
     * There is a special case when the last recieved/acknowleged epochs are needed, they can be accessed without a lock
     * and provide a happens-before relationship (if lastReceived=42, epoch 42 exists and is set up)
     */
    @VisibleForTesting
    public abstract static class AbstractEpochHistory<EpochState extends AbstractEpochState>
    {
        // TODO (low priority): move pendingEpochs / FetchTopology into here?
        private List<EpochState> epochs = new ArrayList<>();

        /**
         * {@link #lastReceived} and {@link #lastAcknowledged} help define the visibility by making sure that the update is the very last thing done in their respected logic.
         * This means that the read of these fields can be done without a lock and can be used to tell the caller that the given epoch has reached a given step.
         *
         * The reason that these fields are private are to help enforce the thread safety of this class.
         */
        private volatile long lastReceived = 0;
        private volatile long lastAcknowledged = 0;
        private volatile long lastTruncated = -1;

        protected abstract EpochState createEpochState(long epoch);

        public long lastReceived()
        {
            return lastReceived;
        }

        public long lastAcknowledged()
        {
            return lastAcknowledged;
        }

        public synchronized long minEpoch()
        {
            return epochs.isEmpty() ? 0L : epochs.get(0).epoch;
        }

        public synchronized long maxEpoch()
        {
            int size = epochs.size();
            return size == 0 ? 0L : epochs.get(size - 1).epoch;
        }

        @VisibleForTesting
        synchronized EpochState atIndex(int idx)
        {
            return epochs.get(idx);
        }

        @VisibleForTesting
        synchronized int size()
        {
            return epochs.size();
        }

        public boolean wasTruncated(long epoch)
        {
            return lastTruncated > 0 && lastTruncated >= epoch;
        }

        public boolean isEmpty()
        {
            return lastReceived == 0;
        }

        @Nullable
        synchronized EpochState getIfExists(long epoch)
        {
            if (epoch < minEpoch() || epoch > lastReceived)
                return null;

            return getOrCreate(epoch);
        }

        protected synchronized EpochState getOrCreate(long epoch)
        {
            Invariants.requireArgument(!wasTruncated(epoch), "Can not re-create truncated epoch %d. Last truncated: %d", epoch, lastTruncated);
            Invariants.requireArgument(epoch >= 0, "Epoch must be non-negative but given %d", epoch);
            Invariants.requireArgument(epoch > 0 || (lastReceived == 0 && epochs.isEmpty()), "Received epoch 0 after initialization. Last received %d, epochs; %s", lastReceived, epochs);
            if (epochs.isEmpty())
            {
                EpochState state = createEpochState(epoch);
                epochs.add(state);
                return state;
            }

            long minEpoch = minEpoch();
            if (epoch < minEpoch)
            {
                int prepend = Ints.checkedCast(minEpoch - epoch);
                List<EpochState> next = new ArrayList<>(epochs.size() + prepend);
                for (long addEpoch=epoch; addEpoch<minEpoch; addEpoch++)
                    next.add(createEpochState(addEpoch));
                next.addAll(epochs);
                epochs = next;
                minEpoch = minEpoch();
                Invariants.require(minEpoch == epoch, "Epoch %d != %d", epoch, minEpoch);
            }
            long maxEpoch = maxEpoch();
            int idx = Ints.checkedCast(epoch - minEpoch);

            // add any missing epochs
            for (long addEpoch = maxEpoch + 1; addEpoch <= epoch; addEpoch++)
                epochs.add(createEpochState(addEpoch));

            return epochs.get(idx);
        }

        public void receive(Topology topology)
        {
            EpochState state;
            synchronized (this)
            {
                long epoch = topology.epoch();
                logger.debug("Receiving epoch {}", epoch);
                if (lastReceived >= epoch)
                {
                    // If we have already seen the epoch
                    for (int i = epochs.size() - 1; i >= 0; i--)
                    {
                        EpochState expected = epochs.get(i);
                        if (epoch == expected.epoch)
                        {
                            Invariants.require(topology.equals(expected.topology),
                                               "Expected existing topology to match upsert, but %s != %s", topology, expected.topology);
                            return;
                        }
                    }
                }
                state = getOrCreate(epoch);
                state.topology = topology;
                lastReceived = epoch;
            }
            // avoid resolving the future while holding the lock, as the callbacks get called in-line
            state.received.setSuccess(topology);
        }

        AsyncResult<Topology> receiveFuture(long epoch)
        {
            return getOrCreate(epoch).received;
        }

        Topology topologyFor(long epoch)
        {
            return getOrCreate(epoch).topology;
        }

        Topology topologyForLastReceived()
        {
            return getOrCreate(lastReceived).topology;
        }

        public void acknowledge(EpochReady ready)
        {
            EpochState state;
            synchronized (this)
            {
                long epoch = ready.epoch;
                logger.debug("Acknowledging epoch {}", epoch);
                Invariants.require(lastAcknowledged == epoch - 1 || epoch == 0 || lastAcknowledged == 0,
                                   "Epoch %d != %d + 1", epoch, lastAcknowledged);
                state = getOrCreate(epoch);
                Invariants.require(state.ready == null, "Ready result was already set for epoch", epoch);
                state.ready = ready;
                lastAcknowledged = epoch;
            }
            // avoid resolving the future while holding the lock, as the callbacks get called in-line
            state.acknowledged.setSuccess(null);
        }

        public AsyncResult<Void> acknowledgeFuture(long epoch)
        {
            return getOrCreate(epoch).acknowledged;
        }

        public synchronized void unsafeMarkTruncated()
        {
            long minEpoch = minEpoch();
            if (minEpoch > 0)
                lastTruncated = minEpoch - 1;
        }

        public synchronized void truncateUntil(long epoch)
        {
            Invariants.requireArgument(epoch <= maxEpoch(), "epoch %d > %d", epoch, maxEpoch());
            long minEpoch = minEpoch();
            int trimFrom = Ints.checkedCast(epoch - minEpoch);

            final int count = epochs.size();
            int highestReadyIdx = -1;
            for (int i = trimFrom; i >= 0; i--)
            {
                EpochReady ready = epochs.get(i).ready;
                if (ready != null && ready.reads.isDone())
                {
                    highestReadyIdx = i;
                    break;
                }
            }

            // Always leave least 1 ready epoch after truncation
            if (highestReadyIdx <= 0)
                return;

            List<EpochState> next = new ArrayList<>(epochs.subList(highestReadyIdx, count));
            Invariants.require(next.get(0).ready.reads.isDone());
            epochs = next;
            lastTruncated = next.get(0).epoch - 1;
        }
    }

    public AbstractConfigurationService(Node.Id localId, Agent agent)
    {
        this.localId = localId;
        this.agent = agent;
    }

    protected abstract EpochHistory createEpochHistory();

    protected EpochState getOrCreateEpochState(long epoch)
    {
        return epochs.getOrCreate(epoch);
    }

    @Override
    public void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    public boolean isEmpty()
    {
        return epochs.isEmpty();
    }

    @Override
    public Topology currentTopology()
    {
        if (isEmpty()) return null;
        return epochs.topologyForLastReceived();
    }

    @Override
    public Topology getTopologyForEpoch(long epoch)
    {
        return epochs.topologyFor(epoch);
    }

    protected Topology getTopologyIfExists(long epoch)
    {
        EpochState state = epochs.getIfExists(epoch);
        if (state != null)
            return state.topology;
        return null;
    }

    @Override
    public abstract void fetchTopologyForEpoch(long epoch);

    protected abstract void onReadyToCoordinate(Topology topology);

    // TODO (expected): startSync should not be necessary if we handle pending propagation work better
    @Override
    public boolean acknowledgeEpoch(EpochReady ready)
    {
        if (epochs.wasTruncated(ready.epoch))
            return false;

        ready.metadata.invokeIfSuccess(() -> epochs.acknowledge(ready)).invoke(agent);
        ready.coordinate.invokeIfSuccess(() -> onReadyToCoordinate(epochs.getOrCreate(ready.epoch).topology)).invoke(agent);
        return true;
    }

    protected void topologyUpdatePostListenerNotify(Topology topology) {}

    protected Executor executor()
    {
        return Runnable::run;
    }

    public void reportTopology(Topology topology)
    {
        long lastReceived = epochs.lastReceived();
        if (topology.epoch() <= lastReceived)
            return;

        if (lastReceived > 0 && topology.epoch() > lastReceived + 1)
        {
            logger.debug("Epoch {} received; waiting to receive {} before reporting", topology.epoch(), lastReceived + 1);
            epochs.receiveFuture(lastReceived + 1)
                  .invokeIfSuccess(() -> reportTopology(topology), executor())
                  .invoke(agent);
            fetchTopologyForEpoch(lastReceived + 1);
            return;
        }

        long lastAcked = epochs.lastAcknowledged();
        if (lastAcked == 0 && lastReceived > 0)
        {
            logger.debug("Epoch {} received; waiting for {} to ack before reporting", topology.epoch(), epochs.minEpoch());
            epochs.acknowledgeFuture(epochs.minEpoch())
                  .invokeIfSuccess(() -> reportTopology(topology), executor())
                  .invoke(agent);
            return;
        }

        if (lastAcked > 0 && topology.epoch() > lastAcked + 1)
        {
            logger.debug("Epoch {} received; waiting for {} to ack before reporting", topology.epoch(), lastAcked + 1);
            epochs.acknowledgeFuture(lastAcked + 1)
                  .invokeIfSuccess(() -> reportTopology(topology), executor())
                  .invoke(agent);
            return;
        }

        logger.debug("Epoch {} received", topology.epoch());
        epochs.receive(topology);
        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);
        topologyUpdatePostListenerNotify(topology);
    }

    public void unsafeMarkTruncated()
    {
        epochs.unsafeMarkTruncated();
    }

    protected void receiveRemoteSyncCompletePreListenerNotify(Node.Id node, long epoch) {}

    public void receiveRemoteSyncComplete(Node.Id node, long epoch)
    {
        receiveRemoteSyncCompletePreListenerNotify(node, epoch);
        for (Listener listener : listeners)
            listener.onRemoteSyncComplete(node, epoch);
    }

    public void receiveClosed(Ranges ranges, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochClosed(ranges, epoch);
    }

    public void receiveRetired(Ranges ranges, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochRetired(ranges, epoch);
    }

    public String getDebugStr()
    {
        StringBuilder sb = new StringBuilder();
        synchronized (this)
        {
            for (long i = epochs.minEpoch(); i <= epochs.maxEpoch(); i++)
            {
                if (i > epochs.minEpoch())
                    sb.append(", ");
                sb.append(i).append(": ").append(epochs.getOrCreate(i).toDebugString());
            }
        }
        return sb.toString();
    }

    @VisibleForImplementation
    public AsyncResult<Void> epochReady(long epoch, Function<EpochReady, AsyncResult<Void>> get)
    {
        // synchronized for state.ready visibility
        synchronized (this)
        {
            EpochState state = epochs.getOrCreate(epoch);
            if (state.ready != null)
                return get.apply(state.ready);

            return state.acknowledged.flatMap(r -> get.apply(state.ready));
        }
    }
}