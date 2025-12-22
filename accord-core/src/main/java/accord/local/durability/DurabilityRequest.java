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

package accord.local.durability;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Timeouts;
import accord.local.Node;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.MinimalSyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResults;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;

public class DurabilityRequest
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityRequest.class);

    static class DurableEvents
    {
        private final long requestedAt;
        private long lastAttemptAt;
        private long durableAt;
        private int attempts;

        DurableEvents(long requestedAt)
        {
            this.requestedAt = requestedAt;
        }

        public long requestedAt() { return requestedAt; }
        public int attempts() { return attempts; }
        public long durableAt() { return durableAt; }
        public long lastAttemptAt() { return lastAttemptAt; }
    }

    final AsyncResults.SettableResult<Void> result = new AsyncResults.SettableResult<>();
    final Object requestedBy;
    final Txn.Kind kind;
    final Timestamp min;
    final Ranges ranges;
    final DurabilityLevel require;
    final long startedAt, timeoutAt;
    Timeouts.RegisteredTimeout timeout;

    private Ranges agreed = Ranges.EMPTY;
    private Ranges achieved = Ranges.EMPTY;

    private LinkedHashMap<TxnId, DurableEvents> events;

    DurabilityRequest(Object requestedBy, Txn.Kind kind, Timestamp min, Ranges ranges, DurabilityLevel require, long startedAt, long timeoutAt)
    {
        this.requestedBy = requestedBy;
        this.kind = kind;
        this.min = min == null ? TxnId.NONE : min;
        this.ranges = ranges;
        this.require = require;
        this.startedAt = startedAt;
        this.timeoutAt = timeoutAt;
    }

    public synchronized void reportAttempt(TxnId txnId, long now)
    {
        DurableEvents e = events.get(txnId);
        e.lastAttemptAt = now;
        e.attempts++;
    }

    synchronized void register(TxnId txnId, long now)
    {
        if (events == null)
            events = new LinkedHashMap<>();
        events.put(txnId, new DurableEvents(now));
    }

    private DurableEvents ensure(TxnId txnId)
    {
        if (events == null)
            events = new LinkedHashMap<>();
        return events.computeIfAbsent(txnId, k -> new DurableEvents(Long.MIN_VALUE));
    }

    synchronized DurableEvents get(TxnId txnId)
    {
        if (events == null)
            return null;
        return events.get(txnId);
    }

    boolean isDone()
    {
        return result.isDone();
    }

    synchronized Ranges achieved()
    {
        return achieved;
    }

    synchronized boolean isDone(AbstractRanges ranges)
    {
        if (isDone())
            return true;
        return achieved.containsAll(ranges);
    }

    void timeout()
    {
        if (result.tryFailure(new TimeoutException()))
            logger.info("Durability request timeout {}", this);
    }

    void reportSuccess()
    {
        result.trySuccess(null);
        Timeouts.RegisteredTimeout cancel = timeout;
        if (cancel != null) cancel.cancel();
    }

    synchronized boolean report(DurabilityResult durability, long finishedAt)
    {
        MinimalSyncPoint syncPoint = durability.syncPoint;
        Route<Range> route = syncPoint.route;
        if (kind == VisibilitySyncPoint && !syncPoint.syncId.is(VisibilitySyncPoint))
            return false;

        Ranges intersecting = ranges.intersecting(route, Minimal);
        if (intersecting.isEmpty())
            return false;

        DurableEvents e = get(syncPoint.syncId);
        if (min.compareTo(syncPoint.syncId) > 0)
        {
            if (e != null) logger.error("{}: too early to satisfy {}, but the request was submitted on its behalf.", syncPoint.syncId, this);
            else if (logger.isDebugEnabled()) logger.debug("{}: too early to satisfy {}", syncPoint.syncId, this);
            return false;
        }

        agreed = agreed.union(MERGE_ADJACENT, intersecting);
        Ranges waitingOn = ranges.without(achieved);

        Ranges expect = waitingOn.slice(intersecting, Minimal);
        Ranges satisfies = expect.slice(durability.satisfies(require), Minimal);
        Ranges success = satisfies.slice(waitingOn, Minimal);
        Ranges failed = expect.without(success);

        if (!failed.isEmpty())
            logFailure(success, failed, e, durability);

        Ranges newAchieved = this.achieved.union(MERGE_ADJACENT, success);
        if (this.achieved != newAchieved && e == null)
            e = ensure(syncPoint.syncId);

        if (e != null && e.durableAt == 0)
            e.durableAt = finishedAt;

        if (newAchieved == this.achieved)
            return false;

        this.achieved = newAchieved;
        if (e != null) logger.info("{}: Successfully achieved durability for {} requested by {}. Remaining: {}.", syncPoint.syncId, ranges, this, ranges.without(this.achieved));
        else if (logger.isDebugEnabled()) logger.debug("{}: partially satisfies {}. Remaining: {}.", syncPoint.syncId, this, ranges.without(this.achieved));
        return this.achieved.containsAll(ranges);
    }

    private void logFailure(Ranges success, Ranges failed, DurableEvents e, DurabilityResult durability)
    {
        if (require.local.compareTo(durability.min.local) > 0 || require.remote.compareTo(durability.min.remote) > 0)
        {
            if (e != null || logger.isDebugEnabled())
            {
                String successString = success.isEmpty() ? "achieved" : String.format("achieved %s/%s for %s but only", require.local, require.remote, success);
                String message = String.format("%s: %s %s/%s for %s; insufficient to satisfy %s/%s requested by %s", durability.syncPoint.syncId, successString, durability.min.local, durability.min.remote, failed, require.local, require.remote, this);
                if (e != null) logger.info(message);
                else logger.debug(message);
            }
            return;
        }

        if (require.including != null && (durability.min.including == null || !durability.min.including.containsAll(require.including)))
        {
            if (e != null || logger.isDebugEnabled())
            {
                String message = String.format("%s: missing nodes %s for ranges %s requested by %s", durability.syncPoint.syncId, missingIds(require.including, durability.min.including), failed, this);
                if (e != null) logger.info(message);
                else logger.debug(message);
            }
        }
    }

    @Override
    public String toString()
    {
        return "[" + requestedBy + " requires >= " + min + " for "
               + ranges + " with local:" + require.local + " and remote:" + require.remote
               + (require.including == null ? "" : " including:" + require.including) + ']';
    }

    private static String missingIds(Collection<Node.Id> require, @Nullable Collection<Node.Id> actual)
    {
        if (actual == null)
            return require.toString();

        StringBuilder sb = new StringBuilder("[");
        for (Node.Id id : require)
        {
            if (!actual.contains(id))
            {
                sb.append(id);
                sb.append(',');
            }
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    synchronized Ranges stillWaiting(Route<Range> intersecting)
    {
        return ranges.without(achieved).intersecting(intersecting, Minimal);
    }
}
