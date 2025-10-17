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

package accord.topology;

import java.util.Arrays;

import accord.local.Node;
import accord.primitives.Ranges;
import accord.utils.Invariants;

/**
 * Thread safety is managed by a synchronized lock on the TopologyManager.
 */
class PendingEpochs
{
    final TopologyManager manager;
    private PendingEpoch[] epochs = new PendingEpoch[16];
    private int start, end;

    PendingEpochs(TopologyManager manager)
    {
        this.manager = manager;
    }

    int size()
    {
        return end - start;
    }

    boolean isEmpty()
    {
        return end == start;
    }

    private void append(PendingEpoch append)
    {
        if (end == epochs.length)
        {
            int capacity = Math.max(epochs.length, size() * 2);
            resize(capacity, 0);
        }
        epochs[end++] = append;
    }

    private void prepend(PendingEpoch append)
    {
        if (start == 0)
        {
            int size = size();
            int capacity = Math.max(epochs.length, size * 2);
            resize(capacity, Math.max(1, capacity / 4));
        }
        epochs[--start] = append;
    }

    private void resize(int newCapacity, int newStart)
    {
        int size = size();
        PendingEpoch[] nextEpochs = epochs.length == newCapacity ? epochs : new PendingEpoch[newCapacity];
        System.arraycopy(epochs, start, nextEpochs, newStart, size);
        if (epochs == nextEpochs)
        {
            if (newStart < start) Arrays.fill(nextEpochs, newStart + size, end, null);
            else Arrays.fill(nextEpochs, start, newStart, null);
        }
        else
        {
            epochs = nextEpochs;
        }
        start = newStart;
        end = start + size;
    }

    /**
     * Mark sync complete for the given node/epoch, and if this epoch
     * is now synced, update the prevSynced flag on superseding epochs
     */
    void remoteReadyToCoordinate(Node.Id node, long epoch)
    {
        Invariants.requireArgument(epoch > 0);
        getOrCreate(epoch).remoteReadyToCoordinate(node);
    }

    /**
     * Mark the epoch as "closed" for the provided ranges; this means that no new transactions
     * that intersect with this range may be proposed in the epoch (they will be rejected).
     */
    Ranges closed(Ranges ranges, long epoch)
    {
        return getOrCreate(epoch).closed(ranges);
    }

    /**
     * Mark the epoch as "retired" for the provided ranges; this means that all transactions that can be
     * proposed for this epoch have now been executed globally.
     */
    Ranges retired(Ranges ranges, long epoch)
    {
        return getOrCreate(epoch).retired(ranges);
    }

    PendingEpoch atIndex(int i)
    {
        return epochs[start + i];
    }

    // TODO (low priority): move pendingEpochs / FetchTopology into here?

    long maxEpoch()
    {
        return isEmpty() ? 0 : epochs[end - 1].epoch;
    }

    PendingEpoch getOrCreate(long epoch)
    {
        if (isEmpty())
        {
            append(new PendingEpoch(epoch, manager));
            return epochs[start];
        }

        long minEpoch = atIndex(0).epoch;
        if (epoch < minEpoch)
        {
            for (long addEpoch = minEpoch - 1; addEpoch >= epoch; --addEpoch)
                prepend(new PendingEpoch(addEpoch, manager));
            return epochs[start];
        }

        int i = (int) (epoch - minEpoch);
        if (i < size())
            return atIndex(i);

        long maxEpoch = maxEpoch();
        for (long addEpoch = maxEpoch + 1; addEpoch <= epoch; addEpoch++)
            append(new PendingEpoch(addEpoch, manager));

        return epochs[end - 1];
    }

    void removeFirst(long epoch)
    {
        Invariants.require(start < end);
        Invariants.require(epochs[start].epoch == epoch);
        epochs[start++] = null;
    }
}
