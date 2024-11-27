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

import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;

public interface CommandSummaries
{
    enum SummaryStatus
    {
        // preaccepted or accepted invalidated
        NOT_ACCEPTED,
        ACCEPTED,
        COMMITTED,
        STABLE,
        APPLIED,
        INVALIDATED
    }

    enum IsDep { IS_DEP, NOT_ELIGIBLE, IS_NOT_DEP }

    class Summary
    {
        public final @Nonnull TxnId txnId;
        public final @Nonnull Timestamp executeAt;
        public final @Nonnull SummaryStatus status;
        public final @Nonnull Unseekables<?> participants;

        public final IsDep dep;
        public final TxnId findAsDep;

        @VisibleForTesting
        public Summary(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull SummaryStatus status, @Nonnull Unseekables<?> participants, IsDep dep, TxnId findAsDep)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.status = status;
            this.participants = participants;
            this.findAsDep = findAsDep;
            this.dep = dep;
        }

        public Summary slice(Ranges slice)
        {
            return new Summary(txnId, executeAt, status, participants.slice(slice, Minimal), dep, findAsDep);
        }

        @Override
        public String toString()
        {
            return "Summary{" +
                   "txnId=" + txnId +
                   ", executeAt=" + executeAt +
                   ", saveStatus=" + status +
                   ", participants=" + participants +
                   ", maybeDep=" + dep +
                   ", findAsDep=" + findAsDep +
                   '}';
        }

        public static class Loader
        {
            public interface Factory<L extends Loader>
            {
                L create(Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep);
            }

            protected final Unseekables<?> searchKeysOrRanges;
            protected final RedundantBefore redundantBefore;
            // TODO (desired): separate out Kinds we need before/after primaryTxnId/executeAt
            protected final Kinds testKind;
            protected final TxnId minTxnId;
            protected final Timestamp maxTxnId;
            @Nullable protected final TxnId findAsDep;

            // TODO (desired): provide executeAt to PreLoadContext so we can more aggressively filter what we load, esp. by Kind
            public static Loader loader(RedundantBefore redundantBefore, PreLoadContext context)
            {
                return loader(redundantBefore, context.primaryTxnId(), context.keyHistory(), context.keys());
            }

            public static Loader loader(RedundantBefore redundantBefore, @Nullable TxnId primaryTxnId, KeyHistory keyHistory, Unseekables<?> keysOrRanges)
            {
                return loader(redundantBefore, primaryTxnId, keyHistory, keysOrRanges, Loader::new);
            }

            public static <L extends Loader> L loader(RedundantBefore redundantBefore, @Nullable TxnId primaryTxnId, KeyHistory keyHistory, Unseekables<?> keysOrRanges, Factory<L> factory)
            {
                TxnId minTxnId = redundantBefore.min(keysOrRanges, e -> e.gcBefore);
                Timestamp maxTxnId = primaryTxnId == null || keyHistory == KeyHistory.RECOVER || !primaryTxnId.is(ExclusiveSyncPoint) ? Timestamp.MAX : primaryTxnId;
                TxnId findAsDep = primaryTxnId != null && keyHistory == KeyHistory.RECOVER ? primaryTxnId : null;
                Kinds kinds = primaryTxnId == null ? AnyGloballyVisible : primaryTxnId.witnesses().or(keyHistory == KeyHistory.RECOVER ? primaryTxnId.witnessedBy() : Kinds.Nothing);
                return factory.create(keysOrRanges, redundantBefore, kinds, minTxnId, maxTxnId, findAsDep);
            }

            public Loader(Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
            {
                this.searchKeysOrRanges = searchKeysOrRanges;
                this.redundantBefore = redundantBefore;
                this.testKind = testKind;
                this.minTxnId = minTxnId;
                this.maxTxnId = maxTxnId;
                this.findAsDep = findAsDep;
            }

            public Summary ifRelevant(Command cmd)
            {
                return ifRelevant(cmd.txnId(), cmd.executeAtOrTxnId(), cmd.saveStatus(), cmd.participants(), cmd.partialDeps());
            }

            private boolean isEligibleDep(SummaryStatus status, TxnId findAsDep, TxnId txnId, Timestamp executeAt)
            {
                switch (status)
                {
                    default: throw new AssertionError("Unhandled SummaryStatus: " + status);
                    case NOT_ACCEPTED:
                    case INVALIDATED:
                        return false;
                    case ACCEPTED:
                        return txnId.compareTo(findAsDep) > 0;
                    case COMMITTED:
                    case APPLIED:
                    case STABLE:
                        return executeAt.compareTo(findAsDep) > 0;
                }
            }

            public Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, StoreParticipants participants, @Nullable PartialDeps partialDeps)
            {
                if (participants == null)
                    return null;

                return ifRelevant(txnId, executeAt, saveStatus, participants.touches(), partialDeps);
            }

            public Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Participants<?> touches, @Nullable PartialDeps partialDeps)
            {
                SummaryStatus summaryStatus = saveStatus.summary;
                if (summaryStatus == null)
                    return null;

                if (!txnId.is(testKind))
                    return null;

                // TODO (desired): generalise this better for key loading
                Ranges keysOrRanges = touches.toRanges();
                if (keysOrRanges.domain() != Routable.Domain.Range)
                    throw new AssertionError(String.format("Txn keys are not range for %s", touches));
                Ranges ranges = keysOrRanges;

                ranges = ranges.intersecting(searchKeysOrRanges, Minimal);
                if (ranges.isEmpty())
                    return null;

                if (redundantBefore != null)
                {
                    Ranges newRanges = redundantBefore.foldlWithBounds(ranges, (e, accum, start, end) -> {
                        if (e.shardAppliedOrInvalidatedBefore.compareTo(txnId) < 0)
                            return accum;
                        return accum.without(Ranges.of(start.rangeFactory().newRange(start, end)));
                    }, ranges, ignore -> false);

                    if (newRanges.isEmpty())
                        return null;

                    ranges = newRanges;
                }

                Invariants.checkState(partialDeps != null || findAsDep == null || !saveStatus.known.deps.hasProposedOrDecidedDeps());
                IsDep isDep = null;
                if (findAsDep != null)
                {
                    if (!isEligibleDep(summaryStatus, findAsDep, txnId, executeAt))
                    {
                        isDep = IsDep.NOT_ELIGIBLE;
                    }
                    else
                    {
                        Unseekables<?> participants = partialDeps.participants(findAsDep);
                        isDep = participants != null && participants.containsAll(ranges) ? IsDep.IS_DEP : IsDep.IS_NOT_DEP;
                    }
                }

                return new Summary(txnId, executeAt, summaryStatus, ranges, isDep, findAsDep);
            }
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
        void visit(P1 p1, P2 p2, Unseekable keyOrRange, TxnId txnId);
    }

    interface AllCommandVisitor
    {
        boolean visit(Unseekable keyOrRange, TxnId txnId, Timestamp executeAt, SummaryStatus status, @Nullable IsDep dep);
    }

    boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, Timestamp testStartAtTimestamp, ComputeIsDep computeIsDep, AllCommandVisitor visit);

    /**
     * Visits keys first in ascending order, with equal keys visiting TxnId is ascending order.
     * Visits range transactions in ascending order by TxnId, then visiting each Range in ascending order
     */
    <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visit, P1 p1, P2 p2);

    interface Snapshot extends CommandSummaries
    {
        NavigableMap<Timestamp, Summary> byTxnId();

        default boolean visit(Unseekables<?> keysOrRanges,
                              TxnId testTxnId,
                              Kinds testKind,
                              TestStartedAt testStartedAt,
                              Timestamp testStartedAtTimestamp,
                              ComputeIsDep computeIsDep,
                              AllCommandVisitor visit)
        {
            NavigableMap<Timestamp, Summary> map = byTxnId();
            switch (testStartedAt)
            {
                default: throw new AssertionError("Unknown started at: " + testStartedAt);
                case STARTED_AFTER:
                    map = map.tailMap(testStartedAtTimestamp, false);
                    break;
                case STARTED_BEFORE:
                    map = map.headMap(testStartedAtTimestamp, false);
                    break;
                case ANY:
                    break;
            }

            for (Summary value : map.values())
            {
                TxnId txnId = value.txnId;
                if (!testKind.test(txnId))
                    continue;

                Unseekables<?> participants = value.participants;
                Unseekables<?> intersecting = participants.intersecting(keysOrRanges);
                if (!intersecting.isEmpty())
                {
                    Timestamp executeAt = value.executeAt;
                    SummaryStatus status = value.status;
                    IsDep dep = value.dep;
                    for (Unseekable participant : intersecting)
                    {
                        if (!visit.visit(participant, txnId, executeAt, status, dep))
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
                TxnId txnId = value.txnId;
                if (!testKind.test(txnId))
                    continue;

                SummaryStatus status = value.status;
                if (status == SummaryStatus.INVALIDATED)
                    continue;

                for (Unseekable keyOrRange : value.participants.intersecting(keysOrRanges, Minimal))
                    visit.visit(p1, p2, keyOrRange, txnId);
            }
        }
    }
}
