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

import javax.annotation.Nullable;

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import static accord.coordinate.ExecuteFlag.HAS_UNIQUE_HLC;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class DepsCalculator extends Deps.Builder implements CommandSummaries.ActiveCommandVisitor<TxnId, DepsCalculator.MinDependencyCalculator>
{
    public static class MinDependencyCalculator
    {
        final MaxDecidedRX maxDecidedRX;
        final TxnId absoluteMinDecidedId;
        final TxnId txnId;
        Unseekable prevKeyOrRange;
        TxnId prevMinDecidedId;

        MinDependencyCalculator(MaxDecidedRX maxDecidedRX, Unseekables<?> keysOrRanges, TxnId txnId)
        {
            this.maxDecidedRX = maxDecidedRX;
            this.absoluteMinDecidedId = maxDecidedRX.minDecidedDependencyId(keysOrRanges, txnId);
            this.txnId = txnId;
        }

        boolean include(Durability durability, Unseekable keyOrRange, TxnId depId)
        {
            if (durability.isDurablyCommitted() || depId.isSyncPoint())
            {
                if (absoluteMinDecidedId != null && depId.compareTo(absoluteMinDecidedId) < 0)
                    return false;

                if (!keyOrRange.equals(prevKeyOrRange))
                {
                    prevKeyOrRange = keyOrRange;
                    prevMinDecidedId = maxDecidedRX.minDecidedDependencyId(keyOrRange, txnId);
                }

                if (prevMinDecidedId != null && depId.compareTo(prevMinDecidedId) < 0)
                    return false;
            }
            return true;
        }
    }

    private boolean hasUnappliedDependency;
    private long maxAppliedHlc;

    public DepsCalculator()
    {
        super(true);
    }

    @Override
    public void visit(TxnId self, @Nullable MinDependencyCalculator minDepCalc, SummaryStatus status, Durability durability, Unseekable keyOrRange, TxnId depId)
    {
        if (minDepCalc != null && !minDepCalc.include(durability, keyOrRange, depId))
            return;

        if (self == null || !self.equals(depId))
            add(keyOrRange, depId);
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
        {
            flags = flags.with(READY_TO_EXECUTE);
            // we don't know whether hlc is unique unless dependencies have applied
            if (maxAppliedHlc < txnId.hlc())
                flags = flags.with(HAS_UNIQUE_HLC);
        }
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
                Invariants.require(maxRedundantBefore.isSyncPoint());
                return null;
            }
        }

        // NOTE: ExclusiveSyncPoint *relies* on STARTED_BEFORE to ensure it reports a dependency on *every* earlier TxnId that may execute (before or after it).
        MinDependencyCalculator minDepCalc = null;
        // the main difference between RX and RV is whether we apply this filtering
        if (txnId.is(ExclusiveSyncPoint)) minDepCalc = new MinDependencyCalculator(safeStore.maxDecidedRX(), touches, txnId);
        safeStore.visit(touches, executeAt, txnId.witnesses(), this, executeAt.equals(txnId) ? null : txnId, minDepCalc);
        Deps result = super.build();
        result = new Deps(result.keyDeps, result.rangeDeps.with(redundant));
        Invariants.require(!txnId.isVisible() || !result.contains(txnId));
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
