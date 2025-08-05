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
import java.util.Set;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Timeouts;
import accord.coordinate.ExecuteSyncPoint;
import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.FullRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncResults;

import static accord.local.durability.DurabilityService.SyncRemote.All;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;

public class DurabilityRequest
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityRequest.class);

    static class DurableEvents
    {
        final long requestedAt;
        long lastAttemptAt;
        ExecuteSyncPoint lastAttempt;
        long durableAt;
        int attempts;

        DurableEvents(long requestedAt)
        {
            this.requestedAt = requestedAt;
        }
    }

    final AsyncResults.SettableResult<Void> result = new AsyncResults.SettableResult<>();
    final Object requestedBy;
    final Txn.Kind kind;
    final Timestamp min;
    final Ranges ranges;
    final SyncLocal local;
    final SyncRemote remote;
    final @Nullable Collection<Node.Id> including;
    final long startedAt, timeoutAt;
    Timeouts.RegisteredTimeout timeout;

    private Ranges agreed = Ranges.EMPTY;
    private Ranges achieved = Ranges.EMPTY;

    private LinkedHashMap<TxnId, DurableEvents> events;

    DurabilityRequest(Object requestedBy, Txn.Kind kind, Timestamp min, Ranges ranges, SyncLocal local, SyncRemote remote, @Nullable Collection<Node.Id> including, long startedAt, long timeoutAt)
    {
        this.requestedBy = requestedBy;
        this.kind = kind;
        this.min = min == null ? TxnId.NONE : min;
        this.ranges = ranges;
        this.local = local;
        this.remote = remote;
        this.including = including;
        this.startedAt = startedAt;
        this.timeoutAt = timeoutAt;
    }

    public synchronized void reportAttempt(TxnId txnId, long now, ExecuteSyncPoint attempt)
    {
        DurableEvents e = events.get(txnId);
        if (Invariants.debug())
            e.lastAttempt = attempt;
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

    private DurableEvents get(TxnId txnId)
    {
        if (events == null)
            return null;
        return events.get(txnId);
    }

    boolean isDone()
    {
        return result.isDone();
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
        SyncPoint<Range> syncPoint = durability.syncPoint;
        if (kind == VisibilitySyncPoint && !syncPoint.syncId.is(VisibilitySyncPoint))
            return false;

        Ranges intersecting = ranges.intersecting(syncPoint.route, Minimal);
        if (intersecting.isEmpty())
            return false;

        agreed = agreed.union(MERGE_ADJACENT, intersecting);
        if (local.compareTo(durability.achievedLocal) > 0 || remote.compareTo(durability.achievedRemote) > 0)
            return false;

        if (min.compareTo(syncPoint.syncId) > 0)
        {
            DurableEvents e = get(syncPoint.syncId);
            if (e != null && e.durableAt >= 0) logger.error("{}: too early to satisfy {}, but the request was submitted on its behalf.", syncPoint.syncId, this);
            else logger.debug("{}: too early to satisfy {}", syncPoint.syncId, this);
            return false;
        }

        if (remote == All && including != null && (durability.excluding == null || intersects(durability.excluding, including)))
        {
            logger.debug("{}: missing nodes {} for ranges {} to satisfy {}", syncPoint.syncId, missingIds(including, durability.excluding),
                        ranges.intersecting(syncPoint.route, Minimal), this);
            return false;
        }

        Ranges newAchieved = achieved.union(MERGE_ADJACENT, intersecting);
        DurableEvents e = get(syncPoint.syncId);
        if (achieved != newAchieved && e == null)
            e = ensure(syncPoint.syncId);

        if (e != null && e.durableAt == 0)
            e.durableAt = finishedAt;

        if (newAchieved == achieved)
            return false;

        achieved = newAchieved;
        logger.debug("{}: partially satisfies {}. Remaining: {}.", syncPoint.syncId, this, ranges.without(achieved));
        return achieved.containsAll(ranges);
    }

    @Override
    public String toString()
    {
        return "[" + requestedBy + " requires >= " + min + " for "
               + ranges + " with local:" + local + " and remote:" + remote
               + (including == null ? "" : " including:" + including) + ']';
    }


    private static String missingIds(Collection<Node.Id> including, Collection<Node.Id> excluding)
    {
        if (excluding == null)
            return including.toString();

        StringBuilder sb = new StringBuilder("[");
        for (Node.Id id : excluding)
        {
            if (including.contains(id))
            {
                sb.append(id);
                sb.append(',');
            }
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    private static boolean intersects(Collection<Node.Id> a, Collection<Node.Id> b)
    {
        if (a.isEmpty() || b.isEmpty())
            return false;

        boolean swap = b instanceof Set ? (a instanceof Set && a.size() > b.size()) : (a instanceof Set || a.size() > b.size());
        if (swap) { Collection<Node.Id> tmp = a; a = b; b = tmp; }

        for (Node.Id id : a)
        {
            if (b.contains(id))
                return true;
        }
        return false;
    }

    synchronized Ranges stillWaiting(FullRoute<Range> intersecting)
    {
        return ranges.without(achieved).intersecting(intersecting, Minimal);
    }
}
