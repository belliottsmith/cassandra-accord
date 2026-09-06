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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.CommandSummaries.SummaryStatus;
import accord.local.MaxDecidedRX.DecidedRX;
import accord.messages.Reply;
import accord.messages.ReplyList;
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
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.AbstractImmediate;
import accord.utils.async.CancellableAsyncResult;

import static accord.coordinate.ExecuteFlag.HAS_UNIQUE_HLC;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.LoadKeys.INCR;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class DepsCalculator extends Deps.Builder implements CommandSummaries.ActiveCommandVisitor<TxnId, DepsCalculator.MinDependencyCalculator>, Consumer<SafeCommandStore>, ExecutionContext
{
    public abstract static class AbstractDepsReply<R extends AbstractDepsReply<R>> extends AbstractImmediate<R> implements ReplyList<R>, Reply, CancellableAsyncResult<R>
    {
        @Override
        public boolean isSuccess()
        {
            return true;
        }

        @Override
        public AsyncResult<R> invoke(BiConsumer<? super R, Throwable> callback)
        {
            callback.accept((R)this, null);
            return this;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public CancellableAsyncResult<R> get(int i)
        {
            Invariants.requireArgument(i == 0);
            return this;
        }

        @Override
        public void cancel()
        {
        }

        @Override
        public void cancelReplies()
        {
        }
    }

    static class AsyncDepsReply<R extends AbstractDepsReply<R>> extends AsyncResults.CancellableChain<R> implements ReplyList<R>
    {
        public AsyncDepsReply(AsyncChain<R> chain)
        {
            super(chain);
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public CancellableAsyncResult<R> get(int i)
        {
            Invariants.require(i == 0);
            return this;
        }

        @Override
        public void cancelReplies()
        {
            cancel();
        }
    }

    public abstract static class DepsReplyCalculator<R extends AbstractDepsReply<R>> extends DepsCalculator implements Function<Void, R>
    {
        public DepsReplyCalculator(TxnId txnId, Timestamp executeAt, StoreParticipants participants)
        {
            super(txnId, executeAt, participants);
        }

        public DepsReplyCalculator(TxnId txnId, Timestamp executeAt, Participants<?> touches)
        {
            super(txnId, executeAt, touches);
        }

        public ReplyList<R> calculate(SafeCommandStore safeStore)
        {
            try
            {
                if (safeStore.canExecuteWith(this))
                {
                    accept(safeStore);
                    return apply(null);
                }
                else
                {
                    AsyncChain<R> chain = safeStore.commandStore().continuationChain(this, this).map(this);
                    return new AsyncDepsReply<>(chain);
                }
            }
            catch (Throwable t)
            {
                close();
                throw t;
            }
        }
    }

    public static class MinDependencyCalculator
    {
        final MaxDecidedRX maxDecidedRX;
        final DecidedRX minDecidedRx;
        final TxnId txnId;
        Unseekable prevKeyOrRange;
        DecidedRX prevDecidedRx;

        MinDependencyCalculator(MaxDecidedRX maxDecidedRX, Unseekables<?> keysOrRanges, TxnId txnId)
        {
            this.maxDecidedRX = maxDecidedRX;
            this.minDecidedRx = maxDecidedRX.forDeps(keysOrRanges, txnId);
            this.txnId = txnId;
        }

        boolean include(Durability durability, Unseekable keyOrRange, TxnId depId)
        {
            if (durability.isDurablyCommitted() || depId.isSyncPoint())
            {
                if (minDecidedRx != null && minDecidedRx.excludeDecided(depId))
                    return false;

                if (!keyOrRange.equals(prevKeyOrRange))
                {
                    prevKeyOrRange = keyOrRange;
                    prevDecidedRx = maxDecidedRX.forDeps(keyOrRange, txnId);
                }

                if (prevDecidedRx != null && prevDecidedRx.excludeDecided(depId))
                    return false;
            }
            return true;
        }
    }

    // TODO (expected): we can also track whether we have only single-key writes that have been Accepted with ballot 0 (or timestamp != t0), or else Committed[1];
    //  in this case we can decide immediately if we have a unique hlc as we don't run the risk of other keys inserting some arbitrary timestamp
    //  [1] probably unsafe to use Accepted with ballot > 0, as there could be a timestamp battle, and the timestamp we see might not be the one that gets decided.
    protected final TxnId txnId;
    protected final Timestamp executeAt;
    private final Participants<?> touches;
    private long sumUnappliedAge, maxUnappliedAge;
    private int unappliedCount;
    private long maxAppliedHlc;
    private RangeDeps redundant;
    private MinDependencyCalculator minDepCalc;

    public DepsCalculator(TxnId txnId, Timestamp executeAt, StoreParticipants touches)
    {
        this(txnId, executeAt, touches.touches());
    }

    public DepsCalculator(TxnId txnId, Timestamp executeAt, Participants<?> touches)
    {
        super(true);
        this.txnId = txnId;
        this.touches = touches;
        this.executeAt = executeAt.equals(txnId) ? txnId : executeAt;
    }

    @Override
    public final void visit(TxnId self, @Nullable MinDependencyCalculator minDepCalc, SummaryStatus status, Durability durability, Unseekable keyOrRange, TxnId depId)
    {
        if (minDepCalc != null && !minDepCalc.include(durability, keyOrRange, depId))
            return;

        if (self == null || !self.equals(depId))
            add(keyOrRange, depId);

        if (status.compareTo(APPLIED) < 0)
        {
            unappliedCount += 1;
            long age = Math.max(0, executeAt.hlc() - depId.hlc());
            sumUnappliedAge += age;
            if (age > maxUnappliedAge)
                maxUnappliedAge = age;
        }
    }

    @Override
    public final void visitMaxAppliedHlc(long maxAppliedHlc)
    {
        if (maxAppliedHlc > this.maxAppliedHlc)
            this.maxAppliedHlc = maxAppliedHlc;
    }

    public final ExecuteFlags executeFlags()
    {
        ExecuteFlags flags = ExecuteFlags.none();
        if (unappliedCount == 0)
        {
            flags = flags.with(READY_TO_EXECUTE);
            // we don't know whether hlc is unique unless dependencies have applied
            if (maxAppliedHlc < txnId.hlc())
                flags = flags.with(HAS_UNIQUE_HLC);
        }
        return flags;
    }

    public final Deps deps()
    {
        Deps result = super.build();
        result = new Deps(result.keyDeps, result.rangeDeps.with(redundant));
        Invariants.require(!txnId.isVisible() || !result.contains(txnId));
        return result;
    }

    public final Timestamp executeAt(Timestamp witnessedAt, Node node)
    {
        Timestamp executeAt = witnessedAt;
        if (unappliedCount > 0 && node.agent().softReject(unappliedCount, maxUnappliedAge, sumUnappliedAge))
            executeAt = executeAt.addFlag(Timestamp.Flag.SOFT_REJECT);
        return executeAt;
    }

    public Deps calculate(SafeCommandStore safeStore, TxnId txnId, long minEpoch, Timestamp executeAt, boolean rejectIfRedundant)
    {
        if (!initialise(safeStore, minEpoch, rejectIfRedundant))
            return null;

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

    public final boolean initialise(SafeCommandStore safeStore, long minEpoch, boolean rejectIfRedundant)
    {
        try (RangeDeps.BuilderByRange redundantBuilder = RangeDeps.builderByRange())
        {
            redundant = safeStore.redundantBefore().collectDeps(touches, redundantBuilder, EpochSupplier.constant(minEpoch), executeAt)
                                 .build();
        }

        if (rejectIfRedundant && !txnId.is(EphemeralRead))
        {
            TxnId maxRedundantBefore = redundant.maxTxnId(null);
            if (maxRedundantBefore != null && maxRedundantBefore.compareTo(executeAt) >= 0)
            {
                Invariants.require(maxRedundantBefore.isSyncPoint());
                return false;
            }
        }

        // the main difference between RX and RV is whether we apply this filtering
        if (txnId.is(ExclusiveSyncPoint))
            minDepCalc = new MinDependencyCalculator(safeStore.maxDecidedRX(), touches, txnId);
        return true;
    }

    @Override
    public void accept(SafeCommandStore safeStore)
    {
        try
        {
            safeStore.visit(safeStore.context().keys(), executeAt, txnId.witnesses(), this, executeAt == txnId ? null : txnId, minDepCalc);
        }
        catch (Throwable t)
        {
            try { close(); }
            catch (Throwable t2) { try { t.addSuppressed(t2); } catch (Throwable ignore) {} }
            throw t;
        }
    }

    public static Deps calculateDeps(SafeCommandStore safeStore, TxnId txnId, StoreParticipants participants, long minEpoch, Timestamp executeAt, boolean rejectIfRedundant)
    {
        return calculateDeps(safeStore, txnId, participants.touches(), minEpoch, executeAt, rejectIfRedundant);
    }

    public static Deps calculateDeps(SafeCommandStore safeStore, TxnId txnId, Participants<?> touches, long minEpoch, Timestamp executeAt, boolean rejectIfRedundant)
    {
        try (DepsCalculator calculator = new DepsCalculator(txnId, executeAt, touches))
        {
            return calculator.calculate(safeStore, txnId, minEpoch, executeAt, rejectIfRedundant);
        }
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public String reason()
    {
        return "Calculate Deps";
    }

    @Override
    public boolean retryPartial()
    {
        return false;
    }

    @Override
    public Unseekables<?> keys()
    {
        return touches;
    }

    @Override
    public LoadKeys loadKeys()
    {
        return INCR;
    }

    @Override
    public LoadKeysFor loadKeysFor()
    {
        return LoadKeysFor.READ_WRITE;
    }

    @Override
    public boolean isIdempotent()
    {
        return true;
    }
}
