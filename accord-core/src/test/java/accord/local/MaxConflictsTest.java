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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.PrefixedIntHashKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomTestRunner;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.primitives.Status.Durability.HasOutcome.None;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.utils.BTreeReducingRangeMap.isWellFormed;
import static accord.utils.Functions.alwaysFalse;

public class MaxConflictsTest
{
    @Test
    public void test()
    {
        test(1L, 1000);
        for (int i = 0 ; i < 10000 ; ++i)
            test(ThreadLocalRandom.current().nextLong(), 10);
        for (int i = 0 ; i < 1000 ; ++i)
            test(ThreadLocalRandom.current().nextLong(), 100);
        for (int i = 0 ; i < 100 ; ++i)
            test(ThreadLocalRandom.current().nextLong(), 1000);
    }

    private void test(long seed, int actionCount)
    {
        System.out.println("Seed: " + seed + ", count: " + actionCount);
        int MAX_HASH = 1 << 20;
        RandomTestRunner.test().withSeed(seed).check(rs -> {
            int maxSyncHashRange = 1 + rs.nextInt(MAX_HASH - 1);
            int maxPruneHashRange = (MAX_HASH-1) / (1 + rs.nextInt(15));
            int prefixCount = rs.nextInt(1, 3);
            int kwrxRatio = 1 + rs.nextInt(255);
            int pruneCount = rs.nextInt(1, 32);
            AtomicLong epoch = new AtomicLong(1), minHlc = new AtomicLong(), maxHlc = new AtomicLong(10000);
            Gen.LongGen hlcs = rs2 -> rs2.nextLong(minHlc.get(), maxHlc.get());
            Gen<TxnId> genSyncIds = AccordGens.txnIds(rs2 -> epoch.get(), hlcs, rs2 -> rs2.nextInt(10), Gens.pick(Txn.Kind.VisibilitySyncPoint, Txn.Kind.ExclusiveSyncPoint));
            Gen<TxnId> genWriteIds = AccordGens.txnIds(rs2 -> epoch.get(), hlcs, rs2 -> rs2.nextInt(10), Gens.pick(Txn.Kind.Write));
            Gen<RoutingKeys> genKeys = rs2 -> {
                int size = 1 + rs.nextInt(2);
                int prefix = 1 + rs.nextInt(prefixCount);
                RoutingKey[] keys = new RoutingKey[size];
                for (int i = 0 ; i < size ; ++i)
                    keys[i] = PrefixedIntHashKey.forHash(prefix, rs.nextInt(MAX_HASH));
                return RoutingKeys.of(keys);
            };
            Gen<Ranges> genRanges = genRanges(MAX_HASH, maxSyncHashRange, prefixCount);
            Gen<Ranges> genPruneRanges = genRanges(MAX_HASH, maxPruneHashRange, prefixCount);

            MaxConflicts conflicts = MaxConflicts.EMPTY;
            for (int actionCounter = actionCount ; actionCounter > 0 ; --actionCounter)
            {
                MaxConflicts next;
                if (rs.nextInt(kwrxRatio) == 0)
                {
                    TxnId syncId = genSyncIds.next(rs);
                    Ranges ranges = genRanges.next(rs);
                    next = conflicts.update(ranges, syncId, syncId);
                    Assertions.assertTrue(syncId.compareTo(next.get(syncId, ranges)) <= 0);
                }
                else
                {
                    TxnId txnId = genWriteIds.next(rs);
                    RoutingKeys keys = genKeys.next(rs);
                    next = conflicts.update(keys, txnId, txnId);
                    Assertions.assertTrue(txnId.compareTo(next.get(txnId, keys)) <= 0);
                }

                Assertions.assertTrue(isWellFormed(next));
                conflicts = next;

                if (pruneCount > 0 && (pruneCount >= actionCounter || rs.nextInt((actionCounter + pruneCount - 1) / pruneCount) == 0))
                {
                    Ranges ranges = genPruneRanges.next(rs);
                    if (rs.decide(0.5f))
                    {
                        Timestamp ts = Timestamp.minForEpoch(epoch.incrementAndGet());
                        next = next.update(ranges, ts, ts);
                    }
                    else
                    {
                        long hlc = rs.nextLong(minHlc.get(), minHlc.get() + maxHlc.get() / 2);
                        long newMinHlc = rs.nextLong(minHlc.get(), hlc + 1);
                        maxHlc.addAndGet(newMinHlc - minHlc.get());
                        minHlc.set(newMinHlc);
                        Timestamp ts = Timestamp.fromValues(epoch.get(), hlc, Node.Id.NONE);
                        next = next.update(ranges, ts, ts);
                    }

                    for (MaxConflicts.Entry e : conflicts)
                    {
                        Assertions.assertTrue(next.getMax(Ranges.of(e.toPlainRange())).compareTo(e.any) >= 0);
                        Assertions.assertTrue(next.getMaxWrite(Ranges.of(e.toPlainRange())).compareTo(e.write) >= 0);
                    }
                    Assertions.assertTrue(isWellFormed(next));
                    conflicts = next;
                    --pruneCount;
                }
            }
        });
    }

    private Gen<Ranges> genRanges(int maxHash, int maxHashRange, int prefixCount)
    {
        if (maxHashRange + 1 >= maxHash)
        {
            return rs ->
            {
                int prefix = 1 + rs.nextInt(prefixCount);
                return Ranges.of(Range.of(PrefixedIntHashKey.forHash(prefix, 0), PrefixedIntHashKey.forHash(prefix, maxHash)));
            };
        }

        return rs -> {
            int size = 1 + rs.nextInt(2);
            int prefix = 1 + rs.nextInt(3);
            Range[] ranges = new Range[size];
            for (int i = 0 ; i < size ; ++i)
            {
                int startHash = rs.nextInt(maxHash - (1 + maxHashRange));
                int endHash = startHash + 1 + rs.nextInt(maxHashRange);
                ranges[i] = Range.of(PrefixedIntHashKey.forHash(prefix, startHash), PrefixedIntHashKey.forHash(prefix, endHash));
            }
            return Ranges.of(ranges);
        };
    }

    static boolean equals(DurableBeforeLinear as, DurableBefore bs)
    {
        if (as.isEmpty())
        {
            if (!bs.isEmpty() || !as.min.equals(bs.min))
                return false;

            return true;
        }

        int i = 0;
        for (DurableBefore.Entry b : bs)
        {
            if (i == as.size())
                return false;

            if (as.valueAt(i) == null)
            {
                if (++i == as.size())
                    return false;
            }
            if (!b.start().equals(as.startAt(i)))
                return false;
            if (!b.end().equals(as.startAt(i + 1)))
                return false;
            if (!Objects.equals(b.quorum, as.valueAt(i).quorum))
                return false;
            if (!Objects.equals(b.universal, as.valueAt(i).universal))
                return false;
            ++i;
        }

        if (i != as.size())
            return false;

        if (!as.min.equals(bs.min))
            return false;

        return true;
    }

    public static class DurableBeforeLinear extends ReducingRangeMap<DurableBefore.Entry>
    {
        public static final DurableBeforeLinear EMPTY = new DurableBeforeLinear();

        final DurableBefore.Entry min;
        DurableBeforeLinear()
        {
            this.min = new DurableBefore.Entry(TxnId.NONE, TxnId.NONE);
        }

        DurableBeforeLinear(RoutingKey[] starts, DurableBefore.Entry[] values)
        {
            super(starts, values);
            if (values.length == 0)
            {
                min = new DurableBefore.Entry(TxnId.NONE, TxnId.NONE);
            }
            else
            {
                DurableBefore.Entry min = null;
                for (DurableBefore.Entry value : values)
                {
                    if (value == null)
                        continue;

                    if (min == null) min = value;
                    else min = DurableBefore.Entry.min(min, value);
                }
                this.min = min;
            }
        }

        public static DurableBeforeLinear create(AbstractRanges ranges, @Nonnull TxnId majority, @Nonnull TxnId universal)
        {
            return create(ranges, new DurableBefore.Entry(majority, universal));
        }

        public static DurableBeforeLinear create(AbstractRanges ranges, DurableBefore.Entry entry)
        {
            if (ranges.isEmpty())
                return DurableBeforeLinear.EMPTY;

            return create(ranges, entry, Builder::new);
        }

        public static DurableBeforeLinear merge(DurableBeforeLinear a, DurableBeforeLinear b)
        {
            return ReducingIntervalMap.merge(a, b, DurableBefore.Entry::max, Builder::new);
        }

        public Status.Durability.HasOutcome min(TxnId txnId, Unseekables<?> unseekables)
        {
            return notDurableIfNull(foldlWithDefault(unseekables, DurableBefore.Entry::mergeMin, DurableBefore.Entry.NONE, null, txnId, test -> test == None));
        }

        public DurableBefore.Entry minEntry(Unseekables<?> unseekables)
        {
            return foldlWithDefault(unseekables, DurableBefore.Entry::min, DurableBefore.Entry.NONE, DurableBefore.Entry.MAX);
        }

        public Status.Durability.HasOutcome max(TxnId txnId, Unseekables<?> unseekables)
        {
            return notDurableIfNull(foldl(unseekables, DurableBefore.Entry::mergeMax, null, txnId, test -> test == Universal));
        }

        public Status.Durability.HasOutcome get(TxnId txnId, RoutingKey participant)
        {
            DurableBefore.Entry entry = get(participant);
            return entry == null ? None : entry.get(txnId);
        }

        public boolean isUniversal(TxnId txnId, RoutingKey participant)
        {
            return get(txnId, participant) == Universal;
        }

        public Status.Durability.HasOutcome min(TxnId txnId)
        {
            if (min.universal.compareTo(txnId) > 0)
                return Universal;
            if (min.quorum.compareTo(txnId) > 0)
                return Quorum;
            return None;
        }

        public long maxEpoch()
        {
            return foldl((e, v) -> TxnId.max(v, TxnId.max(e.quorum, e.universal)), TxnId.NONE, alwaysFalse()).epoch();
        }

        private static Status.Durability.HasOutcome notDurableIfNull(Status.Durability.HasOutcome status)
        {
            return status == null ? None : status;
        }

        static class Builder extends AbstractBoundariesBuilder<RoutingKey, DurableBefore.Entry, DurableBeforeLinear>
        {
            protected Builder(int capacity)
            {
                super(capacity);
            }

            @Override
            protected DurableBeforeLinear buildInternal()
            {
                return new DurableBeforeLinear(starts.toArray(new RoutingKey[0]), values.toArray(new DurableBefore.Entry[0]));
            }
        }
    }
}
