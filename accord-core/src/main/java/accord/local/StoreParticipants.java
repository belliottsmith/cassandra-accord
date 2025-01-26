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

package accord.local;

import java.util.Objects;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.CommandStores.RangesForEpoch;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.local.StoreParticipants.Filter.QUERY;
import static accord.local.StoreParticipants.Filter.UPDATE;
import static accord.primitives.Known.KnownRoute.MaybeRoute;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Route.tryCastToRoute;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

// TODO (desired): split into a single sub-class to save memory in typical case of owns==touches==hasTouched
// TODO (required): add invariants confirming owns contains AT LEAST anything in hasTouched() that intersects txnId.epoch()
public class StoreParticipants
{
    public enum Filter { QUERY, LOAD, UPDATE }

    static class FullStoreParticipants extends StoreParticipants
    {
        @Nullable private final Participants<?> executes;
        private final Participants<?> touches;
        private final Participants<?> hasTouched;

        FullStoreParticipants(@Nullable Route<?> route, Participants<?> owns, @Nullable Participants<?> executes, Participants<?> touches, Participants<?> hasTouched)
        {
            super(route, owns, executes == null);
            this.executes = executes;
            this.touches = touches;
            this.hasTouched = hasTouched;
            Invariants.requireArgument(route != null || (!Route.isRoute(owns) && !Route.isRoute(executes) && !Route.isRoute(touches) && !Route.isRoute(hasTouched)));
            Routable.Domain domain = owns.domain();
            Invariants.requireArgument(touches.containsAll(owns));
            Invariants.requireArgument(route == null || domain == route.domain());
            Invariants.requireArgument(domain == touches.domain());
            Invariants.requireArgument(domain == hasTouched.domain());
        }

        @Nullable public Participants<?> executes()
        {
            return executes;
        }

        @Nullable public Participants<?> stillExecutes()
        {
            return executes;
        }

        public Participants<?> touches()
        {
            return touches;
        }

        public final Participants<?> hasTouched()
        {
            return hasTouched;
        }

        public boolean touchesOnlyOwned()
        {
            return touches.equals(owns());
        }
    }

    public static class FilteredStoreParticipants extends FullStoreParticipants
    {
        final Participants<?> stillOwns, stillTouches, stillExecutes;

        FilteredStoreParticipants(@Nullable Route<?> route, Participants<?> owns, @Nullable Participants<?> executes, Participants<?> touches, Participants<?> hasTouched, Participants<?> stillOwns, Participants<?> stillTouches, Participants<?> stillExecutes)
        {
            super(route, owns, executes, touches, hasTouched);
            this.stillOwns = stillOwns;
            this.stillTouches = stillTouches;
            this.stillExecutes = stillExecutes;
        }

        @Override
        public Participants<?> stillOwns()
        {
            return stillOwns;
        }

        @Override
        public Participants<?> stillTouches()
        {
            return stillTouches;
        }

        @Override
        public Participants<?> stillExecutes()
        {
            return stillExecutes;
        }

        StoreParticipants update(Route<?> route, Participants<?> hasTouched)
        {
            return new FilteredStoreParticipants(route, owns(), executes(), touches(), hasTouched, stillOwns, stillTouches, stillExecutes);
        }

        StoreParticipants update(Route<?> route, Participants<?> owns, Participants<?> executes, Participants<?> touches, Participants<?> hasTouched)
        {
            return new FilteredStoreParticipants(route, owns, executes, touches, hasTouched, stillOwns, stillTouches, stillExecutes);
        }
    }

    private static final StoreParticipants EMPTY_KEYS = new StoreParticipants(null, RoutingKeys.EMPTY);
    private static final StoreParticipants EMPTY_RANGES = new StoreParticipants(null, Ranges.EMPTY);

    @Nullable private final Route<?> route;
    private final Participants<?> owns;
    private final boolean executesIsNull;

    StoreParticipants(@Nullable Route<?> route, Participants<?> owns)
    {
        this(route, owns, true);
    }

    StoreParticipants(@Nullable Route<?> route, Participants<?> owns, boolean executesIsNull)
    {
        this.route = route;
        this.owns = owns;
        this.executesIsNull = executesIsNull;
        Invariants.requireArgument(route != null || !Route.isRoute(owns));
        Routable.Domain domain = owns.domain();
        Invariants.requireArgument(route == null || domain == route.domain());
    }

    public static StoreParticipants create(@Nullable Route<?> route, Participants<?> owns, @Nullable Participants<?> executes, Participants<?> touches, Participants<?> hasTouched)
    {
        return create(route, owns, executes, touches, hasTouched, true);
    }

    public static StoreParticipants create(@Nullable Route<?> route, Participants<?> owns, @Nullable Participants<?> executes, Participants<?> touches, Participants<?> hasTouched, boolean dedup)
    {
        if ((executes == null || owns == executes) && owns == touches && owns == hasTouched)
            return new StoreParticipants(route, owns, executes == null);
        return new FullStoreParticipants(route, owns, executes, touches, hasTouched);
    }

    public final boolean hasFullRoute()
    {
        return Route.isFullRoute(route());
    }

    /**
     * The maximum known route
     */
    public final @Nullable Route<?> route()
    {
        return route;
    }

    /**
     * Everything that the replica is known by all other replicas to own and
     * always participate in the coordination or execution of.
     *
     * That is, keys owned on txnId.epoch() when uncommitted, then between txnId.epoch and executeAt.epoch once committed
     */
    public final Participants<?> owns()
    {
        return owns;
    }

    /**
     * Everything that the replica is known by all other replicas to own and participate in the coordination or execution of,
     * plus any additional keys that we expect to execute locally.
     *
     * This distinction only matters for ExclusiveSyncPoints which close out and retire old epochs, and so execute on
     * all un-retired epochs they intersect with.
     */
    public final Participants<?> ownsOrExecutes(TxnId txnId)
    {
        return txnId.is(ExclusiveSyncPoint) ? touches() : owns;
    }

    /**
     * Owns, but excludes any stale or pre-bootstrap ranges that are also shard redundant,
     * as we do not need to do anything with these keys locally
     * @return
     */
    public Participants<?> stillOwns()
    {
        return owns;
    }

    /**
     * Ranges that we currently interact with, including those we don't own. This includes keys for which we were
     * asked to compute dependencies for, e.g. for a decision in a future epoch we no longer own.
     *
     * If our status includes dependencies of any kind then the dependencies should ordinarily cover exactly this set,
     * except if we are stale or pre-bootstrap, in which case we should have no more keys than this set,
     * but include all keys in {@link #stillTouches}.
     */
    public Participants<?> touches()
    {
        return owns;
    }

    public boolean touchesOnlyOwned()
    {
        return true;
    }

    /**
     * touches, but excluding any stale, pre-bootstrap or retired ranges that are also shard redundant,
     * as we do not need to do anything with these keys locally.
     */
    public Participants<?> stillTouches()
    {
        return touches();
    }

    /**
     * All keys we have ever interacted with
     */
    public Participants<?> hasTouched()
    {
        return owns;
    }

    /**
     * If set, the keys we are known to execute (i.e. excluding any that are pre-bootstrap or stale)
     * @return
     */
    public @Nullable Participants<?> executes()
    {
        return executesIsNull ? null : owns;
    }

    /**
     * If set, the keys we are known to execute (i.e. excluding any that are pre-bootstrap or stale)
     * @return
     */
    public @Nullable Participants<?> stillExecutes()
    {
        return executesIsNull ? null : owns;
    }

    public Participants<?> stillOwnsOrMayExecute(TxnId txnId)
    {
        return txnId.is(ExclusiveSyncPoint) ? stillTouches() : stillOwns();
    }

    /**
     * Do not invoke this method on a participants we will use to query esp. e.g. for calculateDeps.
     * TODO (required): create separate Query object that cannot be filtered or used to update a Command
     */
    public StoreParticipants filter(Filter filter, SafeCommandStore safeStore, TxnId txnId, @Nullable EpochSupplier executeAt)
    {
        return filter(filter, safeStore.redundantBefore(), txnId, executeAt);
    }

    /**
     * Do not invoke this method on a participants we will use to query esp. e.g. for calculateDeps.
     * TODO (desired): create separate Query object that cannot be filtered or used to update a Command
     */
    public StoreParticipants filter(Filter filter, RedundantBefore redundantBefore, TxnId txnId, @Nullable EpochSupplier executeAt)
    {
        if (filter == QUERY)
            return this;

        if (!redundantBefore.mayFilter(txnId, stillTouches()))
            return this;

        Participants<?> curStillOwns = stillOwns();
        Participants<?> stillOwns = redundantBefore.expectToOwn(txnId, executeAt, curStillOwns);

        Participants<?> curStillExecutes = stillExecutes(), stillExecutes = curStillExecutes;
        if (stillExecutes != null)
            stillExecutes = redundantBefore.expectToExecute(txnId, executeAt, stillExecutes);

        Participants<?> curTouches = touches();
        Participants<?> touches = curTouches;
        if (filter == UPDATE)
            touches = redundantBefore.expectToCalculateDependenciesOrConsultOnRecovery(txnId, curTouches);

        Participants<?> curStillTouches = stillTouches();
        Participants<?> stillTouches = redundantBefore.expectToOwnOrExecuteOrConsultOnRecovery(txnId, curStillTouches);
        if (stillTouches != curStillTouches && owns() == curStillTouches && stillTouches.equals(owns))
            stillTouches = owns;

        if (curStillOwns != stillOwns || curStillTouches != stillTouches || curStillExecutes != stillExecutes)
            return new FilteredStoreParticipants(route, owns, executes(), touches, hasTouched(), stillOwns, stillTouches, stillExecutes);

        if (curTouches != touches)
            return update(route, owns, executes(), touches, hasTouched());

        return this;
    }

    public final boolean touches(RoutingKey key)
    {
        return touches().contains(key);
    }

    public final boolean hasTouched(RoutingKey key)
    {
        return hasTouched().contains(key);
    }

    public final boolean stillTouches(RoutingKey key)
    {
        return stillTouches().contains(key);
    }

    public final StoreParticipants supplementHasTouched(Participants<?> addTouched)
    {
        Participants<?> hasTouched = hasTouched();
        Participants<?> newHasTouched = Participants.merge(hasTouched, (Participants)addTouched);
        if (hasTouched == newHasTouched) return this;
        return update(route, owns(), executes(), touches(), newHasTouched);
    }

    public final StoreParticipants supplement(Route<?> route)
    {
        route = Route.merge(this.route(), (Route)route);
        if (route == this.route()) return this;
        return create(route, owns(), executes(), touches(), hasTouched());
    }

    public final StoreParticipants supplement(@Nullable StoreParticipants that)
    {
        if (that == null) return this;
        if (this == that) return this;
        return supplement(that.route(), that.hasTouched());
    }

    public final StoreParticipants supplementOrMerge(SaveStatus currentSaveStatus, @Nullable StoreParticipants that)
    {
        if (currentSaveStatus.compareTo(Erased) >= 0)
            return this;

        if (currentSaveStatus.known.has(MaybeRoute))
            return merge(that);

        return supplement(that);
    }

    public final StoreParticipants merge(@Nullable StoreParticipants that)
    {
        if (that == null) return this;
        if (this == that) return this;
        return merge(that.route(), that.owns(), that.touches(), that.hasTouched());
    }

    /**
     * Route, owns and hasTouched are merged
     * touches is left unchanged, as this varies and cannot safely be supplemented
     * (e.g. if one update touches more epochs than another, it will have suitable data for those epochs)
     */
    public final StoreParticipants supplement(@Nullable Route<?> route, @Nullable Participants<?> hasTouched)
    {
        route = Route.merge(route(), (Route)route);
        hasTouched = Participants.merge(hasTouched(), (Participants) hasTouched);
        return this.route == route && hasTouched() == hasTouched ? this : update(route, hasTouched);
    }

    /**
     * Route, owns and hasTouched are merged
     * touches is left unchanged, as this varies and cannot safely be supplemented
     * (e.g. if one update touches more epochs than another, it will have suitable data for those epochs)
     */
    public final StoreParticipants merge(@Nullable Route<?> route, @Nullable Participants<?> owns, @Nullable Participants<?> touches, @Nullable Participants<?> hasTouched)
    {
        route = Route.merge(this.route, (Route)route);
        owns = Participants.merge(this.owns, (Participants) owns);
        Participants<?> curTouches = touches(), curHasTouched = hasTouched();
        touches = Participants.merge(curTouches, (Participants) touches);
        hasTouched = Participants.merge(curHasTouched, (Participants) hasTouched);
        return this.route == route && this.owns == owns && touches == curTouches && hasTouched == curHasTouched ? this : update(route, owns, executes(), touches, hasTouched);
    }

    StoreParticipants update(Route<?> route, Participants<?> hasTouched)
    {
        return new FullStoreParticipants(route, owns, executes(), touches(), hasTouched);
    }

    StoreParticipants update(Route<?> route, Participants<?> owns, Participants<?> executes, Participants<?> touches, Participants<?> hasTouched)
    {
        return new FullStoreParticipants(route, owns, executes, touches, hasTouched);
    }

    public StoreParticipants withExecutes(Participants<?> executes, Participants<?> stillExecutes)
    {
        if (executes == stillExecutes)
            return update(route, owns, executes, touches(), hasTouched());

        return new FilteredStoreParticipants(route, owns, executes, touches(), hasTouched(), stillOwns(), stillTouches(), stillExecutes);
    }

    // TODO (required): retire this method, merge with executes()
    public Ranges executeRanges(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt)
    {
        Ranges ranges = txnId.is(ExclusiveSyncPoint)
                        ? safeStore.ranges().all()
                        : safeStore.ranges().allAt(executeAt.epoch());

        // TODO (required): otherwise, remove stale?
        return safeStore.redundantBefore().removePreBootstrap(txnId, ranges);
    }

    // TODO (required): synchronise with latest standard logic
    public static Route<?> touches(SafeCommandStore safeStore, long fromEpoch, TxnId txnId,long toEpoch, Route<?> route)
    {
        // TODO (required): remove pre-bootstrap?
        if (txnId.is(ExclusiveSyncPoint))
            return route.slice(safeStore.ranges().all(), Minimal);

        return route.slice(safeStore.ranges().allBetween(fromEpoch, toEpoch), Minimal);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof StoreParticipants)) return false;
        if (obj == this) return true;
        StoreParticipants that = (StoreParticipants) obj;
        return Objects.equals(route(), that.route())
               && owns().equals(that.owns())
               && Objects.equals(executes(), that.executes())
               && touches().equals(that.touches())
               && hasTouched().equals(that.hasTouched());
    }

    public String toString()
    {
        return owns().toString() + (owns() != touches() ? "(" + touches().toString() + ")" : "");
    }

    public static StoreParticipants read(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId)
    {
        Participants<?> owns = participants.slice(safeStore.ranges().allAt(txnId.epoch()), Minimal);
        return create(tryCastToRoute(participants), owns, null, owns, owns);
    }

    public static StoreParticipants read(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long epoch)
    {
        long txnIdEpoch = txnId.epoch();
        return read(safeStore, participants, txnId, Math.min(txnIdEpoch, epoch), Math.max(txnIdEpoch, epoch));
    }

    public static StoreParticipants execute(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long executeAtEpoch)
    {
        return execute(safeStore, participants, txnId, txnId.epoch(), executeAtEpoch);
    }

    public static StoreParticipants execute(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long minEpoch, long executeAtEpoch)
    {
        return update(safeStore, participants, minEpoch, txnId, executeAtEpoch, executeAtEpoch, true);
    }

    public static StoreParticipants read(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId, long minEpoch, long maxEpoch)
    {
        long txnIdEpoch = txnId.epoch();
        RangesForEpoch storeRanges = safeStore.ranges();
        Ranges ownedRanges = storeRanges.allAt(txnIdEpoch);
        Participants<?> owns = participants.slice(ownedRanges, Minimal);
        if (owns == participants)
            return new StoreParticipants(Route.tryCastToRoute(participants), owns);

        Ranges touchesRanges = storeRanges.extend(ownedRanges, txnIdEpoch, txnIdEpoch, minEpoch, maxEpoch);
        if (ownedRanges == touchesRanges)
            return new StoreParticipants(Route.tryCastToRoute(participants), owns);

        Participants<?> touches = participants.slice(touchesRanges, Minimal);
        return create(tryCastToRoute(participants), owns, null, touches, touches);
    }

    public static StoreParticipants update(SafeCommandStore safeStore, Participants<?> participants, long minEpoch, TxnId txnId, long executeAtEpoch)
    {
        return update(safeStore, participants, minEpoch, txnId, executeAtEpoch, executeAtEpoch);
    }

    public static StoreParticipants execute(SafeCommandStore safeStore, Participants<?> participants, long minEpoch, TxnId txnId, long executeAtEpoch)
    {
        return update(safeStore, participants, minEpoch, txnId, executeAtEpoch, executeAtEpoch, true);
    }

    public static StoreParticipants update(SafeCommandStore safeStore, Participants<?> participants, long minEpoch, TxnId txnId, long executeAtEpoch, long highEpoch)
    {
        return update(safeStore, participants, minEpoch, txnId, executeAtEpoch, highEpoch, false);
    }

    public static StoreParticipants update(SafeCommandStore safeStore, Participants<?> participants, long minEpoch, TxnId txnId, long executeAtEpoch, long highEpoch, boolean execute)
    {
        RangesForEpoch storeRanges = safeStore.ranges();
        Ranges ownedRanges = storeRanges.allBetween(txnId.epoch(), executeAtEpoch);
        Participants<?> owns = participants.slice(ownedRanges, Minimal);
        Ranges touchesRanges = storeRanges.extend(ownedRanges, txnId.epoch(), executeAtEpoch, minEpoch, highEpoch);
        Participants<?> touches = ownedRanges == touchesRanges || owns == participants ? owns : participants.slice(touchesRanges, Minimal);
        Participants<?> executes = null;
        if (execute)
        {
            if (txnId.is(ExclusiveSyncPoint)) executes = touches;
            else
            {
                Ranges executeRanges = storeRanges.allAt(executeAtEpoch);
                executes = executeRanges == ownedRanges ? owns : participants.slice(executeRanges, Minimal);
            }
        }
        return create(tryCastToRoute(participants), owns, executes, touches, touches);
    }

    public static StoreParticipants notAccept(SafeCommandStore safeStore, Participants<?> participants, TxnId txnId)
    {
        RangesForEpoch storeRanges = safeStore.ranges();
        Ranges ownedRanges = storeRanges.allAt(txnId.epoch());
        // TODO (expected): do we need to extend touches here? Feels likely this harks back to before we saved hasTouched
        Ranges touchesRanges = storeRanges.all();
        Participants<?> owns = participants.slice(ownedRanges, Minimal);
        Participants<?> touches = ownedRanges == touchesRanges || owns == participants ? owns : participants.slice(touchesRanges, Minimal);
        return create(tryCastToRoute(participants), owns, null, touches, touches);
    }

    public static StoreParticipants empty(Routable.Domain domain)
    {
        return domain == Key ? EMPTY_KEYS : EMPTY_RANGES;
    }

    public static StoreParticipants empty(TxnId txnId)
    {
        return txnId.is(Key) ? EMPTY_KEYS : EMPTY_RANGES;
    }

    public static StoreParticipants empty(TxnId txnId, Route<?> route)
    {
        Participants<?> empty = txnId.is(Key) ? RoutingKeys.EMPTY : Ranges.EMPTY;
        return new StoreParticipants(route, empty);
    }

    public static StoreParticipants empty(TxnId txnId, Route<?> route, boolean executesIsNull)
    {
        Participants<?> empty = txnId.is(Key) ? RoutingKeys.EMPTY : Ranges.EMPTY;
        return new StoreParticipants(route, empty, executesIsNull);
    }

    public static StoreParticipants all(Route<?> route)
    {
        return new StoreParticipants(route, route, false);
    }
}
