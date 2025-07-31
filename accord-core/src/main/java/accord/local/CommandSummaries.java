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

import java.util.NavigableMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.local.RedundantBefore.Bounds;
import accord.local.cfk.CommandsForKey;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.LoadKeysFor.RECOVERY;
import static accord.local.MaxDecidedRX.minDecidedDependencyId;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Nothing;

public interface CommandSummaries
{
    enum SummaryStatus
    {
        NOT_DIRECTLY_WITNESSED,
        PREACCEPTED,
        NOTACCEPTED,
        ACCEPTED,
        COMMITTED,
        STABLE,
        APPLIED,
        INVALIDATED;

        public static final SummaryStatus NONE = null;

        private static final SummaryStatus[] SUMMARY_STATUSES = values();
    }

    enum IsDep
    {
        IS_COORD_DEP, IS_NOT_COORD_DEP, NOT_ELIGIBLE, IS_STABLE_DEP, IS_NOT_STABLE_DEP;
        private static final IsDep[] IS_DEPS = values();
    }

    class Summary extends TxnId
    {
        private static final int SUMMARY_STATUS_MASK = 0x7;
        private static final int IS_DEP_SHIFT = 3;
        final @Nonnull Timestamp executeAt;
        final int encoded;
        final Unseekables<?> participants;

        public Summary slice(Ranges ranges)
        {
            return new Summary(this, this.executeAt, encoded, participants.slice(ranges, Minimal));
        }

        @VisibleForTesting
        public Summary(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull SummaryStatus status, IsDep dep, Unseekables<?> participants)
        {
            super(txnId);
            this.participants = participants;
            this.executeAt = executeAt.equals(txnId) ? this : executeAt;
            this.encoded = status.ordinal() | (dep == null ? Integer.MIN_VALUE : (dep.ordinal() << IS_DEP_SHIFT));
        }

        private Summary(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, int encoded, Unseekables<?> participants)
        {
            super(txnId);
            this.participants = participants;
            this.executeAt = executeAt == txnId || executeAt.equals(txnId) ? this : executeAt;
            this.encoded = encoded;
        }

        public boolean is(IsDep isDep)
        {
            return (encoded >> IS_DEP_SHIFT) == isDep.ordinal();
        }

        public IsDep isDep()
        {
            int ordinal = encoded >> IS_DEP_SHIFT;
            return ordinal < 0 ? null : IsDep.IS_DEPS[ordinal];
        }

        public boolean is(SummaryStatus summaryStatus)
        {
            return (encoded & SUMMARY_STATUS_MASK) == summaryStatus.ordinal();
        }

        public SummaryStatus status()
        {
            int ordinal = encoded & SUMMARY_STATUS_MASK;
            return SummaryStatus.SUMMARY_STATUSES[ordinal];
        }

        public TxnId plainTxnId()
        {
            return new TxnId(this);
        }

        public Timestamp plainExecuteAt()
        {
            return executeAt == this ? new Timestamp(this) : executeAt;
        }

        @Override
        public String toString()
        {
            return "Summary{" +
                   "txnId=" + plainTxnId() +
                   ", executeAt=" + plainExecuteAt() +
                   ", saveStatus=" + status() +
                   ", isDep=" + isDep() +
                   '}';
        }
    }

    class SummaryLoader
    {
        public interface Factory<L extends SummaryLoader, P>
        {
            L create(P param, RedundantBefore redundantBefore, @Nullable MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep);
        }

        protected final RedundantBefore redundantBefore;
        protected final MaxDecidedRX maxDecidedRX;
        protected final Unseekables<?> searchKeysOrRanges;
        // TODO (expected): separate out Kinds we need before/after primaryTxnId/executeAt
        protected final Kinds testKind;
        protected final TxnId primaryTxnId, findAsDep, minTxnId, minDecidedId;
        protected final Timestamp maxTxnId;
//        protected final TxnId minDecidedId;

        // TODO (expected): provide executeAt to PreLoadContext so we can more aggressively filter what we load, esp. by Kind
        public static SummaryLoader loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, PreLoadContext context)
        {
            return loader(redundantBefore, maxDecidedRX, context.primaryTxnId(), context.loadKeysFor(), context.keys());
        }

        public static SummaryLoader loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges)
        {
            return loader(null, redundantBefore, maxDecidedRX, primaryTxnId, loadKeysFor, keysOrRanges, SummaryLoader::new);
        }

        public static <L extends SummaryLoader, P> L loader(P param, RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, PreLoadContext context, Factory<L, P> factory)
        {
            return loader(param, redundantBefore, maxDecidedRX, context.primaryTxnId(), context.loadKeysFor(), context.keys(), factory);
        }

        public static <L extends SummaryLoader, P> L loader(P param, RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges, Factory<L, P> factory)
        {
            Invariants.require(primaryTxnId != null);
            TxnId minTxnId = redundantBefore.min(keysOrRanges, Bounds::gcBefore);
            Timestamp maxTxnId = loadKeysFor == RECOVERY || !primaryTxnId.is(ExclusiveSyncPoint) ? Timestamp.MAX : primaryTxnId;
            TxnId findAsDep = loadKeysFor == RECOVERY ? primaryTxnId : null;
            Kinds kinds = primaryTxnId.witnesses().or(loadKeysFor == RECOVERY ? primaryTxnId.witnessedBy() : Nothing);
            if (!primaryTxnId.is(Txn.Kind.ExclusiveSyncPoint))
                maxDecidedRX = null;
            return factory.create(param, redundantBefore, maxDecidedRX, primaryTxnId, keysOrRanges, kinds, minTxnId, maxTxnId, findAsDep);
        }

        public SummaryLoader(Object ignore, RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            this.redundantBefore = redundantBefore;
            this.maxDecidedRX = maxDecidedRX;
            this.primaryTxnId = primaryTxnId;
            this.searchKeysOrRanges = searchKeysOrRanges;
            this.testKind = testKind;
            this.minTxnId = minTxnId;
            this.maxTxnId = maxTxnId;
            this.findAsDep = findAsDep;
            this.minDecidedId = minDecidedDependencyId(maxDecidedRX, searchKeysOrRanges, primaryTxnId);
        }

        public boolean isRelevant(CommandsForKey cfk)
        {
            if (cfk == null || cfk.size() == 0)
                return false;

            // NOTE: we CANNOT safely filter on first element, as we may have pruned dependencies we need to witness
            //  and that will be populated on the receiving replicas as necessary - that is,
            //  we must permit adopting future dependencies
            CommandsForKey.TxnInfo last = cfk.get(cfk.size() - 1);
            if (last.compareTo(minTxnId) < 0)
                return false;

            if (maxDecidedRX == null)
                return true;

            CommandsForKey.TxnInfo minUndecided = cfk.minUndecided();
            if (minUndecided != null)
                return true;

            if (minDecidedId != null && last.compareTo(minDecidedId) < 0)
                return false;

            TxnId minDecidedId = maxDecidedRX.minDecidedDependencyId(cfk.key(), primaryTxnId);
            return minDecidedId == null || last.compareTo(minDecidedId) >= 0;
        }

        public final Summary ifRelevant(Command cmd)
        {
            return ifRelevant(cmd.txnId(), cmd.executeAtOrTxnId(), cmd.saveStatus(), cmd.participants(), cmd.partialDeps());
        }

        final boolean isEligibleDep(SummaryStatus status, TxnId findAsDep, TxnId txnId, Timestamp executeAt)
        {
            switch (status)
            {
                default: throw new UnhandledEnum(status);
                case NOT_DIRECTLY_WITNESSED:
                case INVALIDATED:
                    return false;
                case NOTACCEPTED:
                case PREACCEPTED:
                    if (!txnId.is(TxnId.FastPath.PrivilegedCoordinatorWithDeps))
                        return false;
                case ACCEPTED:
                    return txnId.compareTo(findAsDep) > 0;
                case COMMITTED:
                case APPLIED:
                case STABLE:
                    return executeAt.compareTo(findAsDep) > 0;
            }
        }

        public final Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, StoreParticipants participants, @Nullable PartialDeps partialDeps)
        {
            if (participants == null)
                return null;

            return ifRelevant(txnId, executeAt, saveStatus, participants.touches(), partialDeps);
        }

        public final Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Participants<?> touches, @Nullable PartialDeps partialDeps)
        {
            SummaryStatus summaryStatus = saveStatus.summary;
            if (summaryStatus == null)
                return null;

            if (!txnId.is(testKind))
                return null;

            boolean mayFilterAsDecided = maxDecidedRX != null && (saveStatus.compareTo(SaveStatus.PreCommitted) >= 0 || txnId.is(ExclusiveSyncPoint));
            if (mayFilterAsDecided && minDecidedId != null && txnId.compareTo(minDecidedId) < 0)
                return null;

            // start in search key domain, since this is what we consult to decide if can be recovered
            Unseekables<?> intersecting = searchKeysOrRanges.intersecting(touches, Minimal);
            if (intersecting.isEmpty())
                return null;

            if (redundantBefore != null)
            {
                // TODO (expected): consider whether this is necessary (and document it).
                Unseekables<?> newIntersecting = redundantBefore.foldlWithBounds(intersecting, (e, accum, start, end) -> {
                    if (e.gcBefore.compareTo(txnId) <= 0)
                        return accum;
                    return accum.without(Ranges.of(start.rangeFactory().newRange(start, end)));
                }, intersecting, ignore -> false);

                if (newIntersecting.isEmpty())
                    return null;

                intersecting = newIntersecting;
            }

            if (mayFilterAsDecided)
            {
                TxnId minDecidedId = minDecidedDependencyId(maxDecidedRX, intersecting, primaryTxnId);
                if (minDecidedId != null && txnId.compareTo(minDecidedId) < 0)
                    return null;
            }

            Invariants.require(partialDeps != null || findAsDep == null || !saveStatus.known.deps().hasProposedOrDecidedDeps());
            IsDep isDep = null;
            if (findAsDep != null)
            {
                if (partialDeps == null || !isEligibleDep(summaryStatus, findAsDep, txnId, executeAt))
                {
                    isDep = IsDep.NOT_ELIGIBLE;
                }
                else
                {
                    boolean isCoordDeps = summaryStatus.compareTo(ACCEPTED) < 0;
                    int index = partialDeps.indexOf(findAsDep);
                    // TODO (desired): don't construct participants, pass intersecting as parameter
                    boolean isAnyDep = index >= 0 && partialDeps.isStable(index)
                                       && partialDeps.participants(index).containsAll(intersecting);

                    isDep = isAnyDep ? (isCoordDeps ? IsDep.IS_COORD_DEP     : IsDep.IS_STABLE_DEP)
                                     : (isCoordDeps ? IsDep.IS_NOT_COORD_DEP : IsDep.IS_NOT_STABLE_DEP);
                }
            }

            // convert to the domain of the command we're loading
            intersecting = touches.intersecting(intersecting, Minimal);
            return construct(txnId, executeAt, summaryStatus, isDep, intersecting);
        }

        protected Summary construct(TxnId txnId, Timestamp executeAt, SummaryStatus summaryStatus, IsDep isDep, Unseekables<?> participants)
        {
            return new Summary(txnId, executeAt, summaryStatus, isDep, participants);
        }
    }
    
    enum TestStartedAt { STARTED_BEFORE, STARTED_AFTER, ANY }
    enum ComputeIsDep
    {
        // don't test deps
        IGNORE,

        // calculate but don't filter
        EITHER
    }

    interface ActiveCommandVisitor<P1, P2>
    {
        void visit(P1 p1, P2 p2, SummaryStatus status, Unseekable keyOrRange, TxnId txnId);
        default void visitMaxAppliedHlc(long maxAppliedHlc) {}
    }

    interface AllCommandVisitor
    {
        /**
         * Note: Durability is not guaranteed to return anything besides NotDurable; implementation is free to return more information if easily available.
         */
        boolean visit(Unseekable keyOrRange, TxnId txnId, Timestamp executeAt, SummaryStatus status, @Nullable IsDep dep, Status.Durability minDurability);
    }

    boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, Timestamp testStartAtTimestamp, ComputeIsDep computeIsDep, AllCommandVisitor visit);

    /**
     * Visits keys first in ascending order, with equal keys visiting TxnId is ascending order.
     * Visits range transactions in ascending order by TxnId, then visiting each Range in ascending order
     */
    <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visit, P1 p1, P2 p2);

    // TODO (expected): ByRangeSnapshot based on IntervalBTree so we can elide superseded dependencies as we do with CommandsForKey
    interface ByTxnIdSnapshot extends CommandSummaries
    {
        NavigableMap<Timestamp, Summary> byTxnId();
        class Helper
        {
            static <S extends Summary> NavigableMap<Timestamp, S> slice(TestStartedAt testStartedAt, Timestamp testStartedAtTimestamp, NavigableMap<Timestamp, S> map)
            {
                switch (testStartedAt)
                {
                    default: throw new UnhandledEnum(testStartedAt);
                    case STARTED_AFTER: return map.tailMap(testStartedAtTimestamp, false);
                    case STARTED_BEFORE: return map.headMap(testStartedAtTimestamp, false);
                    case ANY: return map;
                }
            }
        }

        default boolean visit(Unseekables<?> keysOrRanges,
                              TxnId testTxnId,
                              Kinds testKind,
                              TestStartedAt testStartedAt,
                              Timestamp testStartedAtTimestamp,
                              ComputeIsDep computeIsDep,
                              AllCommandVisitor visit)
        {
            NavigableMap<Timestamp, Summary> map = Helper.slice(testStartedAt, testStartedAtTimestamp, byTxnId());

            for (Summary value : map.values())
            {
                if (!testKind.test(value))
                    continue;

                Unseekables<?> participants = value.participants;
                Unseekables<?> intersecting = participants.intersecting(keysOrRanges);
                if (!intersecting.isEmpty())
                {
                    Timestamp executeAt = value.plainExecuteAt();
                    SummaryStatus status = value.status();
                    IsDep dep = value.isDep();
                    for (Unseekable participant : intersecting)
                    {
                        if (!visit.visit(participant, value.plainTxnId(), executeAt, status, dep, NotDurable))
                            return false;
                    }
                }
            }

            return true;
        }

        @Override
        default <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visit, P1 p1, P2 p2)
        {
            NavigableMap<Timestamp, Summary> map = byTxnId();
            for (Summary value : map.headMap(startedBefore, false).values())
            {
                if (!testKind.test(value))
                    continue;

                if (value.is(SummaryStatus.INVALIDATED))
                    continue;

                for (Unseekable keyOrRange : value.participants.intersecting(keysOrRanges, Minimal))
                    visit.visit(p1, p2, value.status(), keyOrRange, value.plainTxnId());
            }
        }
    }
}
