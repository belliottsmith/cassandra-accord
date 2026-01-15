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

import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.primitives.Status.Durability.HasOutcome.None;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.utils.Functions.alwaysFalse;

public class DurableBefore extends ReducingRangeMap<DurableBefore.Entry>
{
    public static class SerializerSupport
    {
        public static DurableBefore create(RoutingKey[] ends, Entry[] values)
        {
            if (values.length == 0)
                return DurableBefore.EMPTY;
            return new DurableBefore(ends, values);
        }
    }

    public static class Entry
    {
        public static final Entry MAX = new Entry(TxnId.MAX, TxnId.MAX);
        public static final Entry NONE = new Entry(TxnId.NONE, TxnId.NONE);

        public final @Nonnull TxnId quorumBefore, universalBefore;

        public Entry(@Nonnull TxnId quorum, @Nonnull TxnId universal)
        {
            Invariants.requireArgument(quorum.compareTo(universal) >= 0, "quorum %s < universal %s", quorum, universal);
            this.quorumBefore = quorum;
            this.universalBefore = universal;
        }

        public static Entry max(Entry a, Entry b)
        {
            return reduce(a, b, TxnId::max);
        }

        public static Entry min(Entry a, Entry b)
        {
            return reduce(a, b, TxnId::min);
        }

        private static Entry reduce(Entry a, Entry b, BiFunction<TxnId, TxnId, TxnId> reduce)
        {
            TxnId majority = reduce.apply(a.quorumBefore, b.quorumBefore);
            TxnId universal = reduce.apply(a.universalBefore, b.universalBefore);

            if (majority == a.quorumBefore && universal == a.universalBefore)
                return a;
            if (majority.equals(b.quorumBefore) && universal.equals(b.universalBefore))
                return b;

            return new Entry(majority, universal);
        }

        public HasOutcome get(TxnId txnId)
        {
            if (txnId.compareTo(quorumBefore) < 0)
                return txnId.compareTo(universalBefore) < 0 ? Universal : Quorum;
            return None;
        }

        static HasOutcome mergeMin(Entry entry, @Nullable HasOutcome prev, TxnId txnId)
        {
            HasOutcome next = entry.get(txnId);
            return prev != null && prev.compareTo(next) <= 0 ? prev : next;
        }

        static HasOutcome mergeMax(Entry entry, HasOutcome prev, TxnId txnId)
        {
            HasOutcome next = entry.get(txnId);
            return prev != null && prev.compareTo(next) >= 0 ? prev : next;
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return    this.quorumBefore.equals(that.quorumBefore)
                   && this.universalBefore.equals(that.universalBefore);
        }

        @Override
        public String toString()
        {
            return "(majority=" + quorumBefore + ",universal=" + universalBefore + ")";
        }
    }

    public static final DurableBefore EMPTY = new DurableBefore();

    final Entry min;
    private DurableBefore()
    {
        this.min = new Entry(TxnId.NONE, TxnId.NONE);
    }

    DurableBefore(RoutingKey[] starts, Entry[] values)
    {
        super(starts, values);
        if (values.length == 0)
        {
            min = new Entry(TxnId.NONE, TxnId.NONE);
        }
        else
        {
            Entry min = null;
            for (Entry value : values)
            {
                if (value == null)
                    continue;

                if (min == null) min = value;
                else min = Entry.min(min, value);
            }
            this.min = min;
        }
    }

    public static DurableBefore create(AbstractRanges ranges, @Nonnull TxnId majority, @Nonnull TxnId universal)
    {
        if (ranges.isEmpty())
            return DurableBefore.EMPTY;

        Entry entry = new Entry(majority, universal);
        return create(ranges, entry, Builder::new);
    }

    public static DurableBefore merge(DurableBefore a, DurableBefore b)
    {
        return ReducingIntervalMap.merge(a, b, DurableBefore.Entry::max, Builder::new);
    }

    public static DurableBefore mergeIfDifferent(DurableBefore prev, DurableBefore add)
    {
        DurableBefore next = DurableBefore.merge(prev, add);
        if (next.equals(prev))
            return prev;
        return next.equals(prev) ? prev : next;
    }

    public HasOutcome min(TxnId txnId, Unseekables<?> unseekables)
    {
        return notDurableIfNull(foldlWithDefault(unseekables, Entry::mergeMin, Entry.NONE, null, txnId, test -> test == None));
    }

    public Entry minEntry(Unseekables<?> unseekables)
    {
        return foldlWithDefault(unseekables, Entry::min, Entry.NONE, Entry.MAX);
    }

    public HasOutcome max(TxnId txnId, Unseekables<?> unseekables)
    {
        return notDurableIfNull(foldl(unseekables, Entry::mergeMax, null, txnId, test -> test == Universal));
    }

    public HasOutcome get(TxnId txnId, RoutingKey participant)
    {
        DurableBefore.Entry entry = get(participant);
        return entry == null ? None : entry.get(txnId);
    }

    public boolean isUniversal(TxnId txnId, RoutingKey participant)
    {
        return get(txnId, participant) == Universal;
    }

    public HasOutcome min(TxnId txnId)
    {
        if (min.universalBefore.compareTo(txnId) > 0)
            return Universal;
        if (min.quorumBefore.compareTo(txnId) > 0)
            return Quorum;
        return None;
    }

    public long maxEpoch()
    {
        return foldl((e, v) -> TxnId.max(v, TxnId.max(e.quorumBefore, e.universalBefore)), TxnId.NONE, alwaysFalse()).epoch();
    }

    private static HasOutcome notDurableIfNull(HasOutcome status)
    {
        return status == null ? None : status;
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, Entry, DurableBefore>
    {
        protected Builder(int capacity)
        {
            super(capacity);
        }

        @Override
        protected DurableBefore buildInternal()
        {
            return new DurableBefore(starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }

    public static final PersistentField.Persister<DurableBefore, DurableBefore> NOOP_PERSISTER = new PersistentField.Persister<>()
    {
        @Override public AsyncResult<?> persist(DurableBefore addValue, DurableBefore newValue) { return AsyncResults.success(null); }
        @Override public DurableBefore load() { return DurableBefore.EMPTY; }
    };
}
