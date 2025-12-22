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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import accord.api.RoutingKey;
import accord.impl.PrefixedIntHashKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;

import static accord.utils.BTreeReducingRangeMap.isWellFormed;

public abstract class AbstractMaxConflictsTest
{
    protected abstract void assertTrue(boolean isTrue);

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

    protected RoutingKey key(int prefix, int hash)
    {
        return PrefixedIntHashKey.forHash(prefix, hash);
    }

    protected void check(RandomSource rs, MaxConflicts prev, MaxConflicts next)
    {
        assertTrue(isWellFormed(next));
        for (MaxConflicts.Entry e : prev)
        {
            assertTrue(next.getMax(Ranges.of(e.toPlainRange())).compareTo(e.any) >= 0);
            assertTrue(next.getMaxWrite(Ranges.of(e.toPlainRange())).compareTo(e.write) >= 0);
        }
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
                int prefix = rs.nextInt(prefixCount);
                RoutingKey[] keys = new RoutingKey[size];
                for (int i = 0 ; i < size ; ++i)
                    keys[i] = key(prefix, rs.nextInt(MAX_HASH));
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
                    assertTrue(syncId.compareTo(next.get(syncId, ranges)) <= 0);
                }
                else
                {
                    TxnId txnId = genWriteIds.next(rs);
                    RoutingKeys keys = genKeys.next(rs);
                    next = conflicts.update(keys, txnId, txnId);
                    assertTrue(txnId.compareTo(next.get(txnId, keys)) <= 0);
                }

                assertTrue(isWellFormed(next));
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
                    check(rs, conflicts, next);
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
                int prefix = rs.nextInt(prefixCount);
                return Ranges.of(Range.of(key(prefix, 0), key(prefix, maxHash)));
            };
        }

        return rs -> {
            int size = 1 + rs.nextInt(2);
            int prefix = rs.nextInt(prefixCount);
            Range[] ranges = new Range[size];
            for (int i = 0 ; i < size ; ++i)
            {
                int startHash = rs.nextInt(maxHash - (1 + maxHashRange));
                int endHash = startHash + 1 + rs.nextInt(maxHashRange);
                ranges[i] = Range.of(key(prefix, startHash), key(prefix, endHash));
            }
            return Ranges.of(ranges);
        };
    }
}
