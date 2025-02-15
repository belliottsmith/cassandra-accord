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

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.utils.Invariants;

import static accord.coordinate.ExecuteFlag.HAS_UNIQUE_HLC;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class DepsCalculator extends Deps.Builder implements CommandSummaries.ActiveCommandVisitor<TxnId, Object>
{
    private boolean hasUnappliedDependency;
    private long maxAppliedHlc;

    public DepsCalculator()
    {
        super(true);
    }

    @Override
    public void visit(TxnId self, Object o, SummaryStatus status, Unseekable keyOrRange, TxnId txnId)
    {
        if (self == null || !self.equals(txnId))
            add(keyOrRange, txnId);
        if (status.compareTo(APPLIED) < 0)
            hasUnappliedDependency = true;
    }

    @Override
    public void visitMaxAppliedHlc(long maxAppliedHlc)
    {
        if (maxAppliedHlc > this.maxAppliedHlc)
            this.maxAppliedHlc = maxAppliedHlc;
    }

    public ExecuteFlags executeFlags(TxnId txnId)
    {
        ExecuteFlags flags = ExecuteFlags.none();
        if (!hasUnappliedDependency)
            flags = flags.with(READY_TO_EXECUTE);
        if (maxAppliedHlc < txnId.hlc())
            flags = flags.with(HAS_UNIQUE_HLC);
        return flags;
    }

    public Deps calculate(SafeCommandStore safeStore, TxnId txnId, StoreParticipants participants, long minEpoch, Timestamp executeAt, boolean nullIfRedundant)
    {
        return calculate(safeStore, txnId, participants.touches(), minEpoch, executeAt, nullIfRedundant);
    }

    public Deps calculate(SafeCommandStore safeStore, TxnId txnId, Participants<?> touches, long minEpoch, Timestamp executeAt, boolean nullIfRedundant)
    {
        RangeDeps redundant;
        try (RangeDeps.BuilderByRange redundantBuilder = RangeDeps.builderByRange())
        {
            redundant = safeStore.redundantBefore().collectDeps(touches, redundantBuilder, EpochSupplier.constant(minEpoch), executeAt)
                                 .build();
        }

        if (nullIfRedundant && !txnId.is(EphemeralRead))
        {
            TxnId maxRedundantBefore = redundant.maxTxnId(null);
            if (maxRedundantBefore != null && maxRedundantBefore.compareTo(executeAt) >= 0)
            {
                Invariants.require(maxRedundantBefore.is(ExclusiveSyncPoint));
                return null;
            }
        }

        // NOTE: ExclusiveSyncPoint *relies* on STARTED_BEFORE to ensure it reports a dependency on *every* earlier TxnId that may execute (before or after it).
        safeStore.visit(touches, executeAt, txnId.witnesses(), this, executeAt.equals(txnId) ? null : txnId, null);
        Deps result = super.build();
        result = new Deps(result.keyDeps, result.rangeDeps.with(redundant), result.directKeyDeps);
        Invariants.require(!result.contains(txnId));
        return result;
    }

    public static Deps calculateDeps(SafeCommandStore safeStore, TxnId txnId, StoreParticipants participants, long minEpoch, Timestamp executeAt, boolean nullIfRedundant)
    {
        return calculateDeps(safeStore, txnId, participants.touches(), minEpoch, executeAt, nullIfRedundant);
    }

    public static Deps calculateDeps(SafeCommandStore safeStore, TxnId txnId, Participants<?> touches, long minEpoch, Timestamp executeAt, boolean nullIfRedundant)
    {
        try (DepsCalculator calculator = new DepsCalculator())
        {
            return calculator.calculate(safeStore, txnId, touches, minEpoch, executeAt, nullIfRedundant);
        }
    }
}
