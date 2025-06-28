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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.AsyncExecutor;
import accord.api.RoutingKey;
import accord.coordinate.ExecuteSyncPoint;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
import accord.coordinate.Exhausted;
import accord.coordinate.Timeout;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import org.agrona.collections.ObjectHashSet;

import static accord.coordinate.ExecuteSyncPoint.coordinateIncluding;
import static accord.local.durability.DurabilityService.SyncRemote.All;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Tracks and schedules durability requests, executing a sync point after it has been agreed.
 *
 * Up to {@code maxConcurrency} tasks are allowed to be in flight. All subsequent tasks are put into the {@code pending}
 * queue, which new tasks are pulled from after the current in-progress ones complete. To prevent unbounded growth of the
 * pending queue, pending tasks are periodically pruned.
 */
public class DurabilityQueue
{
    private static final Logger logger = LoggerFactory.getLogger(DurabilityQueue.class);
    private static final ConcurrentSkipListMap<Long, Collection<Node.Id>> WARNINGS_LOGGED = new ConcurrentSkipListMap<>();
    private static final long EXHAUSTED_LOG_INTERVAL_MINUTES = 5;
    private static final int PRUNE_SIZE_THRESHOLD = 128;

    private final Node node;
    private int maxConcurrency = 16;

    private final ObjectHashSet<ExecuteSyncPoint> inProgress = new ObjectHashSet<>();
    private final TreeMap<RoutingKey, RoutingKey> inProgressRanges = new TreeMap<>();
    // TODO (desired): prioritise by least recently updated range
    private final Deque<Pending> pending = new ArrayDeque<>();
    private int pendingCounter, prunedAt;

    static class Pending
    {
        final SyncPoint<Range> syncPoint;
        final @Nullable DurabilityRequest request;
        final int attempt;

        Pending(SyncPoint<Range> syncPoint, DurabilityRequest request, int attempt)
        {
            this.syncPoint = syncPoint;
            this.request = request;
            this.attempt = attempt;
        }
    }

    public DurabilityQueue(Node node)
    {
        this.node = node;
    }

    synchronized void submit(SyncPoint<Range> syncPoint, @Nullable DurabilityRequest request)
    {
        if (request != null)
            request.register(syncPoint.syncId, node.elapsed(MICROSECONDS));

        submit(syncPoint, request, 1);
    }

    private synchronized void submit(SyncPoint<Range> syncPoint, @Nullable DurabilityRequest request, int attempt)
    {
        AsyncExecutor executor = node.someExecutor();
        if (executor != null && inProgress.size() < maxConcurrency && !isInProgress(syncPoint.route))
        {
            start(syncPoint, request, attempt, executor);
        }
        else
        {
            ++pendingCounter;
            pending.add(new Pending(syncPoint, request, attempt));
            if (pending.size() >= PRUNE_SIZE_THRESHOLD && pendingCounter > prunedAt + pending.size())
                prune();
        }
    }

    private boolean isInProgress(Route<Range> route)
    {
        for (Range range : route)
        {
            Map.Entry<RoutingKey, RoutingKey> e = inProgressRanges.floorEntry(range.start());
            if (e != null && e.getValue().compareTo(range.start()) > 0)
                return true;
        }
        return false;
    }

    private void registerInProgress(SyncPoint<Range> syncPoint, ExecuteSyncPoint submitted)
    {
        inProgress.add(submitted);
        for (Range range : syncPoint.route)
            inProgressRanges.put(range.start(), range.end());
    }

    private void unregisterInProgress(SyncPoint<Range> syncPoint, ExecuteSyncPoint submitted)
    {
        inProgress.remove(submitted);
        for (Range range : syncPoint.route)
        {
            RoutingKey end = inProgressRanges.remove(range.start());
            Invariants.require(range.end().equals(end), "Expected exact range to be in progress, but found different end (%s vs %s)", range.end(), end);
        }
    }

    private static class SortForPruning implements Comparable<SortForPruning>
    {
        final Range range;
        final SyncPoint<Range> syncPoint;

        SortForPruning(Range range, SyncPoint<Range> syncPoint)
        {
            this.range = range;
            this.syncPoint = syncPoint;
        }

        @Override
        public int compareTo(@Nonnull SortForPruning that)
        {
            int c = this.range.start().compareTo(that.range.start());
            if (c == 0) c = -this.range.end().compareTo(that.range.end());
            if (c == 0) return this.syncPoint.syncId.compareTo(that.syncPoint.syncId);
            return c;
        }
    }

    private static class OverlapsForPruning
    {
        final SyncPoint<Range> syncPoint;
        final NavigableMap<TxnId, SyncPoint<Range>> overlaps = new TreeMap<>();

        private OverlapsForPruning(SyncPoint<Range> syncPoint)
        {
            this.syncPoint = syncPoint;
        }

        void add(SyncPoint<Range> overlap)
        {
            overlaps.put(overlap.syncId, overlap);
        }
    }

    private synchronized void prune()
    {
        prunedAt = pendingCounter;
        List<SortForPruning> sorted = new ArrayList<>();
        for (Pending p : pending)
        {
            for (Range range : p.syncPoint.route)
                sorted.add(new SortForPruning(range, p.syncPoint));
        }
        sorted.sort(SortForPruning::compareTo);
        Map<TxnId, OverlapsForPruning> overlaps = new TreeMap<>();
        int i = 0;
        while (i < sorted.size())
        {
            SortForPruning entry = sorted.get(i);
            for (int j = i + 1; j < sorted.size() ; ++j)
            {
                SortForPruning next = sorted.get(j);
                if (next.range.start().compareTo(entry.range.end()) >= 0)
                    break;

                overlaps.computeIfAbsent(next.syncPoint.syncId, ignore -> new OverlapsForPruning(next.syncPoint)).add(entry.syncPoint);
                overlaps.computeIfAbsent(entry.syncPoint.syncId, ignore -> new OverlapsForPruning(entry.syncPoint)).add(next.syncPoint);
            }
            ++i;
        }

        Set<TxnId> remove = new HashSet<>();
        for (OverlapsForPruning e : overlaps.values())
        {
            SyncPoint<Range> syncPoint = e.syncPoint;
            Ranges supersedes = e.overlaps.tailMap(syncPoint.syncId).values().stream().map(s -> s.route.toRanges()).reduce(Ranges.EMPTY, Ranges::with);
            Ranges superseding = e.overlaps.headMap(syncPoint.syncId).values().stream().map(s -> s.route.toRanges()).reduce(Ranges.EMPTY, Ranges::with);
            if (superseding.containsAll(syncPoint.route) && supersedes.containsAll(syncPoint.route))
                remove.add(syncPoint.syncId);
        }


        logger.info("Pruned {} sync points awaiting durability to {}", pending.size(), pending.size() - remove.size());
        if (!remove.isEmpty())
        {
            List<Pending> newPending = new ArrayList<>(pending.size());
            for (Pending p : pending)
            {
                if (!remove.contains(p.syncPoint.syncId))
                    newPending.add(p);
            }
            pending.clear();
            pending.addAll(newPending);
        }
    }

    private void start(SyncPoint<Range> exclusiveSyncPoint, @Nullable DurabilityRequest request, int attempt, AsyncExecutor executor)
    {
        logger.debug("{}: Awaiting durability for {}", exclusiveSyncPoint.syncId, exclusiveSyncPoint.route.toRanges());
        ExecuteSyncPoint coordinate = coordinateIncluding(node, exclusiveSyncPoint, request == null ? null : request.including, executor, attempt);
        registerInProgress(exclusiveSyncPoint, coordinate);
        if (request != null)
            request.reportAttempt(exclusiveSyncPoint.syncId, node.elapsed(MICROSECONDS), coordinate);

        coordinate.onQuorum().invoke((success, fail) -> {
            synchronized (this)
            {
                unregisterInProgress(exclusiveSyncPoint, coordinate);
                maybeSubmitPending();
            }
        });
        coordinate.invoke((success, fail) -> {
            TxnId txnId = exclusiveSyncPoint.syncId;
            Ranges ranges = exclusiveSyncPoint.route.toRanges();
            String requestor = request != null ? " requested by " + request.requestedBy : "";
            if (fail != null)
            {
                if (logger.isTraceEnabled()) logger.trace("{}: failed awaiting durability for {}{}.", txnId, ranges, requestor, fail);
                if (fail instanceof SyncPointErased || fail instanceof TopologyManager.TopologyRetiredException)
                {
                    // we can't succeed. if this was requested, and the request is still waiting, submit another coordination request
                    // TODO (required): expand this to all unknown exception outcomes
                    if (request != null)
                        restart(exclusiveSyncPoint, request, attempt + 1);
                    return;
                }
            }
            if (success == null || (success.achievedRemote.compareTo(request == null ? All : request.remote) < 0))
            {
                if (success != null)
                    fail = success.failure;

                if (fail instanceof Exhausted || success != null)
                {
                    Collection<Node.Id> failedNodes = success != null ? success.excluding : ((Exhausted)fail).failedNodes();
                    Ranges failedRanges = success != null ? exclusiveSyncPoint.route().toRanges() : ((Exhausted)fail).failedRanges();
                    boolean log = failedNodes == null;
                    if (!log)
                    {
                        Set<Node.Id> unlogged = new HashSet<>(failedNodes);
                        for (Collection<Node.Id> logged : WARNINGS_LOGGED.tailMap(System.nanoTime() - MINUTES.toNanos(EXHAUSTED_LOG_INTERVAL_MINUTES)).values())
                            unlogged.removeAll(logged);
                        log = !unlogged.isEmpty();
                    }
                    if (log)
                    {
                        logger.info("{}: Incomplete durability for {}{}. {} were unsuccessful.", txnId, failedRanges, requestor, failedNodes == null ? "some nodes" : failedNodes);
                        WARNINGS_LOGGED.headMap(System.nanoTime() - MINUTES.toNanos(EXHAUSTED_LOG_INTERVAL_MINUTES)).clear();
                        WARNINGS_LOGGED.put(System.nanoTime(), failedNodes == null ? Collections.emptyList() : failedNodes);
                    }
                }
                else
                {
                    if (fail instanceof Timeout) logger.info("{}: Timeout awaiting durability for {}{}", txnId, ranges, requestor, fail);
                    else if (fail != null) logger.info("{}: Failed awaiting durability for {}{}; will retry", txnId, ranges, requestor, fail);
                }
                retry(exclusiveSyncPoint, request, attempt + 1);
            }
            else
            {
                if (request != null) logger.info("{}: Successfully achieved durability for {}{}.", txnId, ranges, requestor);
                else logger.debug("{}: Successfully achieved durability for {}.", txnId, ranges);
            }
        });
    }

    void retry(SyncPoint<Range> syncPoint, DurabilityRequest request, int attempt)
    {
        long retryDelay = node.agent().retryDurabilityDelay(node, attempt, MICROSECONDS);
        Invariants.require(retryDelay > 0);
        if (request != null) logger.info("{}: Retrying durability for {} requested by {} in {}s.", syncPoint.syncId, syncPoint.route.toRanges(), request.requestedBy, String.format("%.3f", retryDelay/1000_000.0));
        else logger.debug("{}: Retrying durability for {} in {}s.", syncPoint.syncId, syncPoint.route.toRanges(), String.format("%.3f", retryDelay/1000_000.0));
        node.scheduler().selfRecurring(() -> submit(syncPoint, request, attempt), retryDelay, MICROSECONDS);
    }

    void restart(SyncPoint<Range> syncPoint, DurabilityRequest request, int attempt)
    {
        long retryDelay = node.agent().retryDurabilityDelay(node, attempt, MICROSECONDS);
        Invariants.require(retryDelay > 0);
        logger.debug("{}: Restarting durability for {} in {}s.", syncPoint.syncId, syncPoint.route.toRanges(), String.format("%.3f", retryDelay/1000_000.0));
        node.scheduler().selfRecurring(() -> node.durability().shards().request(request, request.stillWaiting(syncPoint.route)), retryDelay, MICROSECONDS);
    }

    synchronized void maybeSubmitPending()
    {
        if (inProgress.size() < maxConcurrency)
            submitPending();
    }

    private synchronized void submitPending()
    {
        AsyncExecutor executor = node.someExecutor();
        List<Pending> couldNotSubmit = null;
        Pending next;
        while (null != (next = pending.poll()))
        {
            if (!isInProgress(next.syncPoint.route))
            {
                start(next.syncPoint, next.request, next.attempt, executor);
                if (inProgress.size() >= maxConcurrency) break;
                else continue;
            }

            if (couldNotSubmit == null)
                couldNotSubmit = new ArrayList<>();
            couldNotSubmit.add(next);
        }

        if (couldNotSubmit != null)
        {
            for (int i = couldNotSubmit.size() - 1 ; i >= 0 ; --i)
                pending.push(couldNotSubmit.get(i));
        }
    }

    synchronized void setMaxConcurrency(int newMaxConcurrency)
    {
        this.maxConcurrency = newMaxConcurrency;
    }
}