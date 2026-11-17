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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import accord.local.Cleanup;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.RedundantStatus.SomeStatus;
import accord.local.StoreParticipants;
import accord.primitives.FullKeyRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.api.Journal.Load.ALL;
import static accord.impl.CommandChange.Field.DURABILITY;
import static accord.impl.CommandChange.Field.PARTICIPANTS;
import static accord.impl.CommandChange.Field.SAVE_STATUS;
import static accord.impl.CommandChange.setChanged;
import static accord.impl.IntKey.range;
import static accord.impl.IntKey.routing;
import static accord.local.Cleanup.EXPUNGE;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.Cleanup.NO;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_DURABLE;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Journal compaction may see only some of a command's records, and decides how to clean them up from that PARTIAL
 * input. The cleanup it decides is written back with those records, so applies to the whole command when it is next
 * reconstructed; it must therefore never be more aggressive than the cleanup FULL input decides for the whole command.
 * <p>
 * The records of an applied write are merged exactly as the journal merges them, for every subset of the records.
 * PARTIAL EXPUNGE is not checked: it removes the records it saw rather than writing a cleanup back, and is decided
 * from global bounds.
 */
public class CommandChangePartialCleanupTest
{
    private static final Range LEFT = range(0, 50), RIGHT = range(50, 150);
    private static final Ranges RANGES = Ranges.of(LEFT, RIGHT);
    private static final FullKeyRoute ROUTE = RoutingKeys.of(routing(10), routing(100)).toRoute(routing(10));

    private static final TxnId TXN_ID = new TxnId(1, 100, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
    private static final Timestamp EXECUTE_AT = Timestamp.fromValues(1, 150, new Node.Id(1));
    private static final TxnId BEFORE_EXECUTE_AT = new TxnId(1, 120, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, Node.Id.NONE);
    private static final TxnId AFTER_EXECUTE_AT = new TxnId(1, 200, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, Node.Id.NONE);

    /** A single record of the command, setting only the fields it changes. */
    static final class Record extends CommandChange.Builder
    {
        Record()
        {
            super(TXN_ID, ALL);
        }

        Record participants(StoreParticipants participants)
        {
            this.participants = participants;
            flags = setChanged(PARTICIPANTS, flags);
            return this;
        }

        Record saveStatus(SaveStatus saveStatus)
        {
            this.saveStatus = saveStatus;
            flags = setChanged(SAVE_STATUS, flags);
            return this;
        }

        Record executeAt(Timestamp executeAt)
        {
            this.executeAt = executeAt;
            flags = setChanged(CommandChange.Field.EXECUTE_AT, flags);
            return this;
        }

        Record durability(Durability durability)
        {
            this.durability = durability;
            flags = setChanged(DURABILITY, flags);
            return this;
        }
    }

    /** The records of a write that is applied locally (but still waits on other replicas), oldest first. */
    private static List<Supplier<Record>> appliedWrite(Timestamp executeAt)
    {
        List<Supplier<Record>> records = new ArrayList<>();
        records.add(() -> new Record().participants(StoreParticipants.all(ROUTE, true)).saveStatus(SaveStatus.PreAccepted).executeAt(TXN_ID));
        records.add(() -> new Record().participants(StoreParticipants.all(ROUTE, false)).saveStatus(SaveStatus.Stable).executeAt(executeAt));
        records.add(() -> new Record().saveStatus(SaveStatus.Applying).executeAt(executeAt));
        records.add(() -> new Record().saveStatus(SaveStatus.Applied).executeAt(executeAt));
        records.add(() -> new Record().durability(Durability.AllQuorums));
        return records;
    }

    /** GC'd before the transaction (but not its executeAt, which the data store's unique HLCs must retain), durable at a quorum */
    @Test
    public void gcBeforeTxnIdButNotExecuteAt()
    {
        test(gcBefore(BEFORE_EXECUTE_AT), DurableBefore.create(RANGES, AFTER_EXECUTE_AT, TxnId.NONE));
    }

    @Test
    public void gcBeforeTxnIdButNotExecuteAtUniversallyDurable()
    {
        test(gcBefore(BEFORE_EXECUTE_AT), DurableBefore.create(RANGES, AFTER_EXECUTE_AT, AFTER_EXECUTE_AT));
    }

    @Test
    public void gcBeforeExecuteAt()
    {
        test(gcBefore(AFTER_EXECUTE_AT), DurableBefore.create(RANGES, AFTER_EXECUTE_AT, TxnId.NONE));
    }

    @Test
    public void notGcd()
    {
        test(RedundantBefore.create(RANGES, TxnId.NONE, SomeStatus.NONE), DurableBefore.create(RANGES, AFTER_EXECUTE_AT, TxnId.NONE));
    }

    private static void test(RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        List<Supplier<Record>> records = appliedWrite(EXECUTE_AT);
        Cleanup full = merge(records, (1 << records.size()) - 1).shouldCleanup(FULL, redundantBefore, durableBefore);
        for (int subset = 1 ; subset < 1 << records.size() ; ++subset)
        {
            CommandChange.Builder merged = merge(records, subset);
            Cleanup partial = merged.shouldCleanup(PARTIAL, redundantBefore, durableBefore);
            if (partial == NO || partial == EXPUNGE)
                continue;

            assertTrue(partial.compareTo(full) <= 0,
                       String.format("PARTIAL input %s (records %s) decided %s, but FULL input decided only %s", merged, Integer.toBinaryString(subset), partial, full));
        }
    }

    /** Merge the selected records, newest first, as the journal does */
    private static CommandChange.Builder merge(List<Supplier<Record>> records, int subset)
    {
        CommandChange.Builder merged = new Record();
        for (int i = records.size() - 1 ; i >= 0 ; --i)
        {
            if ((subset & (1 << i)) == 0)
                continue;

            Record record = records.get(i).get();
            record.clearSuperseded(false, merged);
            merged.fillInMissingOrCleanup(false, record);
        }
        return merged;
    }

    private static RedundantBefore gcBefore(TxnId bound)
    {
        return RedundantBefore.create(RANGES, bound.addFlag(Timestamp.Flag.SHARD_BOUND), GC_BEFORE_AND_LOCALLY_DURABLE);
    }
}
