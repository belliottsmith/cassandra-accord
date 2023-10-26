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
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.RedundantBefore.PreBootstrapOrStale.POST_BOOTSTRAP;
import static accord.local.RedundantBefore.PreBootstrapOrStale.PARTIALLY;
import static accord.local.RedundantStatus.LIVE;
import static accord.local.RedundantStatus.NOT_OWNED;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.SHARD_REDUNDANT;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Entry>
{
    public static class SerializerSupport
    {
        public static RedundantBefore create(boolean inclusiveEnds, RoutingKey[] ends, Entry[] values)
        {
            return new RedundantBefore(inclusiveEnds, ends, values);
        }
    }

    public enum PreBootstrapOrStale { NOT_OWNED, FULLY, PARTIALLY, POST_BOOTSTRAP }

    public static class Entry
    {
        public final Range range;
        public final long startEpoch, endEpoch;
        public final @Nonnull TxnId redundantBefore, bootstrappedAt;
        public final @Nullable Timestamp staleUntilAtLeast;

        public Entry(Range range, long startEpoch, long endEpoch, @Nonnull TxnId redundantBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
        {
            this.range = range;
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.redundantBefore = redundantBefore;
            this.bootstrappedAt = bootstrappedAt;
            this.staleUntilAtLeast = staleUntilAtLeast;
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry a, Entry b)
        {
            if (a.startEpoch > b.endEpoch)
                return a;

            if (b.startEpoch > a.endEpoch)
                return b;

            long startEpoch = Long.max(a.startEpoch, b.startEpoch);
            long endEpoch = Long.min(a.endEpoch, b.endEpoch);
            int cr = a.redundantBefore.compareTo(b.redundantBefore);
            int cb = a.bootstrappedAt.compareTo(b.bootstrappedAt);
            int csu = compareStaleUntilAtLeast(a.staleUntilAtLeast, b.staleUntilAtLeast);

            if (range.equals(a.range) && startEpoch == a.startEpoch && endEpoch == a.endEpoch && cr >= 0 && cb >= 0 && csu >= 0)
                return a;
            if (range.equals(b.range) && startEpoch == b.startEpoch && endEpoch == b.endEpoch && cr <= 0 && cb <= 0 && csu <= 0)
                return b;

            TxnId redundantBefore = cr >= 0 ? a.redundantBefore : b.redundantBefore;
            TxnId bootstrappedAt = cb >= 0 ? a.bootstrappedAt : b.bootstrappedAt;
            Timestamp staleUntilAtLeast = csu >= 0 ? a.staleUntilAtLeast : b.staleUntilAtLeast;

            // if our redundantBefore predates bootstrappedAt, we should clear it to avoid erroneously treating
            // transactions prior as locally redundant when they may simply have not applied yet, since we may
            // permit the sync point that defines redundancy to apply locally without waiting for these earlier
            // transactions, since we now consider them to be bootstrapping
            // TODO (desired): revisit later as semantics here evolve
            if (bootstrappedAt.compareTo(redundantBefore) >= 0)
                redundantBefore = TxnId.NONE;
            if (staleUntilAtLeast != null && bootstrappedAt.compareTo(staleUntilAtLeast) >= 0)
                staleUntilAtLeast = null;

            return new Entry(range, startEpoch, endEpoch, redundantBefore, bootstrappedAt, staleUntilAtLeast);
        }

        static @Nonnull RedundantStatus getAndMerge(Entry entry, @Nonnull RedundantStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            return prev.merge(entry.get(txnId));
        }

        static RedundantStatus get(Entry entry, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return NOT_OWNED;
            return entry.get(txnId);
        }

        static PreBootstrapOrStale getAndMerge(Entry entry, @Nonnull PreBootstrapOrStale prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (prev == PARTIALLY || entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;

            boolean isPreBootstrapOrStale = entry.staleUntilAtLeast != null || entry.bootstrappedAt.compareTo(txnId) > 0;
            return isPreBootstrapOrStale ? prev == POST_BOOTSTRAP ? PARTIALLY : FULLY
                                         : prev == FULLY          ? PARTIALLY : POST_BOOTSTRAP;
        }

        static Ranges validateSafeToRead(Entry entry, @Nonnull Ranges safeToRead, Timestamp bootstrapAt, Object ignore)
        {
            if (entry == null)
                return safeToRead;

            if (bootstrapAt.compareTo(entry.bootstrappedAt) < 0 || (entry.staleUntilAtLeast != null && bootstrapAt.compareTo(entry.staleUntilAtLeast) < 0))
                return safeToRead.subtract(Ranges.of(entry.range));

            return safeToRead;
        }

        static Ranges expectToExecute(Entry entry, @Nonnull Ranges executeRanges, TxnId txnId, Timestamp executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return executeRanges;

            if (txnId.compareTo(entry.bootstrappedAt) < 0 || entry.staleUntilAtLeast != null)
                return executeRanges.subtract(Ranges.of(entry.range));

            return executeRanges;
        }

        RedundantStatus get(TxnId txnId)
        {
            if (staleUntilAtLeast != null || bootstrappedAt.compareTo(txnId) > 0)
                return PRE_BOOTSTRAP_OR_STALE;
            if (redundantBefore.compareTo(txnId) > 0)
                return SHARD_REDUNDANT;
            return LIVE;
        }

        private static int compareStaleUntilAtLeast(@Nullable Timestamp a, @Nullable Timestamp b)
        {
            boolean aIsNull = a == null, bIsNull = b == null;
            if (aIsNull != bIsNull) return aIsNull ? -1 : 1;
            return aIsNull ? 0 : a.compareTo(b);
        }

        public final boolean isLocalRedundancyViaBootstrap()
        {
            return redundantBefore.compareTo(bootstrappedAt) < 0;
        }

        public final TxnId locallyRedundantBefore()
        {
            return TxnId.min(bootstrappedAt, redundantBefore);
        }

        // TODO (required, consider): this admits the range of epochs that cross the two timestamps, which matches our
        //   behaviour elsewhere but we probably want to only interact with the two point epochs in which we participate,
        //   but note hasRedundantDependencies which really does want to scan a range
        private boolean outOfBounds(Timestamp lb, EpochSupplier ub)
        {
            return ub.epoch() < startEpoch || lb.epoch() >= endEpoch;
        }

        Entry withRange(Range range)
        {
            return new Entry(range, startEpoch, endEpoch, redundantBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return this.range.equals(that.range) && equalsIgnoreRange(that);
        }

        public boolean equalsIgnoreRange(Entry that)
        {
            return this.startEpoch == that.startEpoch
                   && this.endEpoch == that.endEpoch
                   && this.redundantBefore.equals(that.redundantBefore)
                   && this.bootstrappedAt.equals(that.bootstrappedAt)
                   && Objects.equals(this.staleUntilAtLeast, that.staleUntilAtLeast);
        }

        @Override
        public String toString()
        {
            return "("
                   + (startEpoch == Long.MIN_VALUE ? "-\u221E" : Long.toString(startEpoch)) + ","
                   + (endEpoch == Long.MAX_VALUE ? "\u221E" : Long.toString(endEpoch)) + ","
                   + (redundantBefore.compareTo(bootstrappedAt) >= 0 ? redundantBefore + ")" : bootstrappedAt + "*)");
        }
    }

    public static RedundantBefore EMPTY = new RedundantBefore();

    private final Ranges staleRanges;

    private RedundantBefore()
    {
        staleRanges = Ranges.EMPTY;
    }

    RedundantBefore(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values)
    {
        super(inclusiveEnds, starts, values);
        staleRanges = extractStaleRanges(values);
    }

    private static Ranges extractStaleRanges(Entry[] values)
    {
        int countStaleRanges = 0;
        for (Entry entry : values)
        {
            if (entry != null && entry.staleUntilAtLeast != null)
                ++countStaleRanges;
        }

        if (countStaleRanges == 0)
            return Ranges.EMPTY;

        Range[] staleRanges = new Range[countStaleRanges];
        countStaleRanges = 0;
        for (Entry entry : values)
        {
            if (entry != null && entry.staleUntilAtLeast != null)
                staleRanges[countStaleRanges++] = entry.range;
        }
        return Ranges.ofSortedAndDeoverlapped(staleRanges).mergeTouching();
    }

    public static RedundantBefore create(Ranges ranges, long startEpoch, long endEpoch, @Nonnull TxnId redundantBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, startEpoch, endEpoch, redundantBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(Ranges ranges, long startEpoch, long endEpoch, @Nonnull TxnId redundantBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        if (ranges.isEmpty())
            return new RedundantBefore();

        Entry entry = new Entry(null, startEpoch, endEpoch, redundantBefore, bootstrappedAt, staleUntilAtLeast);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), entry.withRange(cur), (a, b) -> { throw new IllegalStateException(); });
            builder.append(cur.end(), null, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.merge(a, b, RedundantBefore.Entry::reduce, Builder::new);
    }

    public RedundantStatus get(TxnId txnId, @Nullable EpochSupplier executeAt, RoutingKey participant)
    {
        if (executeAt == null) executeAt = txnId;
        Entry entry = get(participant);
        return Entry.get(entry, txnId, executeAt);
    }

    public RedundantStatus status(TxnId txnId, EpochSupplier executeAt, Participants<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return foldl(participants, Entry::getAndMerge, NOT_OWNED, txnId, executeAt, ignore -> false);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public PreBootstrapOrStale preBootstrapOrStale(TxnId txnId, @Nullable EpochSupplier executeAt, Participants<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return foldl(participants, Entry::getAndMerge, PreBootstrapOrStale.NOT_OWNED, txnId, executeAt, r -> r == PARTIALLY);
    }

    public Ranges validateSafeToRead(Timestamp forBootstrapAt, Ranges ranges)
    {
        return foldl(ranges, Entry::validateSafeToRead, ranges, forBootstrapAt, null, r -> false);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges expectToExecute(TxnId txnId, Timestamp executeAt, Ranges ranges)
    {
        return foldl(ranges, Entry::expectToExecute, ranges, txnId, executeAt, r -> false);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public Ranges staleRanges()
    {
        return staleRanges;
    }

    static class Builder extends ReducingIntervalMap.Builder<RoutingKey, Entry, RedundantBefore>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected boolean equals(Entry a, Entry b)
        {
            return a.equalsIgnoreRange(b);
        }

        @Override
        public void append(RoutingKey start, @Nullable Entry value, BiFunction<Entry, Entry, Entry> reduce)
        {
            if (value != null && value.range.start().compareTo(start) < 0)
                value = value.withRange(value.range.newRange(start, value.range.end()));

            int tailIdx = values.size() - 1;
            super.append(start, value, reduce);

            Entry tailValue; // TODO (desired): clean up maintenance of accurate range bounds
            if (values.size() - 2 == tailIdx && tailIdx >= 0 && (tailValue = values.get(tailIdx)) != null && tailValue.range.end().compareTo(start) > 0)
                values.set(tailIdx, tailValue.withRange(tailValue.range.newRange(tailValue.range.start(), start)));
        }

        @Override
        protected Entry mergeEqual(Entry a, Entry b)
        {
            Invariants.checkState(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Entry(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startEpoch, a.endEpoch, a.redundantBefore, a.bootstrappedAt, a.staleUntilAtLeast);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }
}
