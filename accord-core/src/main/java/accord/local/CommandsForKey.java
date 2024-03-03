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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;

import accord.impl.CommandsSummary;
import accord.local.Command.WaitingOn;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedList;
import accord.utils.TriPredicate;
import net.nicoulaj.compilecommand.annotations.Inline;
import net.openhft.chronicle.wire.TriConsumer;

import static accord.local.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.local.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.CommandsForKey.InternalStatus.STABLE;
import static accord.local.CommandsForKey.InternalStatus.HISTORICAL;
import static accord.local.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED;
import static accord.local.CommandsForKey.InternalStatus.TRANSITIVELY_KNOWN;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.isParanoid;
import static accord.utils.SortedArrays.linearUnion;

/**
 * A specialised collection for efficiently representing and querying everything we need for making coordination
 * and recovery decisions about a key's command conflicts.
 *
 * Every command we know about that is not shard-redundant is listed in the TxnId[] collection, which is sorted by TxnId.
 * This list implies the contents of the deps of all commands in the collection - it is assumed that in the normal course
 * of events every transaction will include the full set of TxnId we know. We only encode divergences from this, stored
 * in each command's {@code missing} collection.
 *
 * We then go one step further, exploiting the fact that the missing collection exists solely to implement recovery,
 * and so we elide from this missing collection any TxnId we have recorded as Committed or higher.
 * Any recovery coordinator that contacts this replica will report that the command has been agreed to execute,
 * and so will not need to decipher any fast-path decisions. So the missing collection is redundant, as no command's deps
 * will not be queried for this TxnId's presence/absence.
 * TODO (expected) this logic applies equally well to Accepted
 *
 * The goal with these behaviours is that this missing collection will ordinarily be empty, and represented by the exact
 * same NO_TXNIDS array instance as every other command.
 *
 * We also exploit the property that most commands will also agree to execute at their proposed TxnId. If we have
 * no missing collection to encode, and no modified executeAt, we store a global NoInfo object that takes up no
 * space on heap. These NoInfo objects permit further efficiencies, as we may perform class-pointer comparisons
 * before querying any contents to skip uninteresting contents, permitting fast iteration of the collection's contents.
 *
 * We also impose the condition that every TxnId is uniquely represented in the collection, so any executeAt and missing
 * collection that represents the same value as the TxnId must be the same object present in the main TxnId[].
 *
 * This collection also implements transitive dependency elision.
 * When evaluating mapReduceActive, we first establish the last-executing Stable write command (i.e. those whose deps
 * are considered durably decided, and so must wait for all commands Committed with a lower executeAt).
 * We then elide any Committed command that has a lower executeAt than this command.
 *
 * Both commands must be known at a majority, but neither might be Committed at any other replica.
 * Either command may therefore be recovered.
 * If the later command is recovered, this replica will report its Stable deps thereby recovering them.
 * If this replica is not contacted, some other replica must participate that either has taken the same action as this replica,
 * or else does not know the later command is Stable, and so will report the earlier command as a dependency again.
 * If the earlier command is recovered, this replica will report that it is Committed, and so will not consult
 * this replica's collection to decipher any fast path decision. Any other replica must either do the same, or else
 * will correctly record this transaction as present in any relevant deps of later transactions.
 *
 * TODO (expected): optimisations:
 *    3) consider storing a prefix of TxnId that are all NoInfo PreApplied encoded as a BitStream as only required for computing missing collection
 *    4) consider storing (or caching) an int[] of records with an executeAt that occurs out of order, sorted by executeAt
 *
 * TODO (expected): track whether a TxnId is a write on this key only for execution (rather than globally)
 * TODO (expected): merge TimestampsForKey
 * TODO (required): randomised testing
 */
public class CommandsForKey implements CommandsSummary
{
    private static final boolean PRUNE_TRANSITIVE_DEPENDENCIES = true;
    public static final TxnId[] NO_TXNIDS = new TxnId[0];
    public static final Info[] NO_INFOS = new Info[0];

    public static class SerializerSupport
    {
        public static CommandsForKey create(Key key, TxnId redundantBefore, TxnId[] txnIds, Info[] infos)
        {
            return new CommandsForKey(key, redundantBefore, txnIds, infos);
        }
    }

    public enum InternalStatus
    {
        TRANSITIVELY_KNOWN(false, false), // (unwitnessed; no need for mapReduce to witness)
        HISTORICAL(false, false),
        PREACCEPTED(false),
        ACCEPTED(true),
        COMMITTED(true),
        STABLE(true),
        APPLIED(true),
        INVALID_OR_TRUNCATED(false);

        static final EnumMap<SaveStatus, InternalStatus> convert = new EnumMap<>(SaveStatus.class);
        static final InternalStatus[] VALUES = values();
        static
        {
            convert.put(SaveStatus.PreAccepted, PREACCEPTED);
            convert.put(SaveStatus.AcceptedInvalidateWithDefinition, PREACCEPTED);
            convert.put(SaveStatus.Accepted, ACCEPTED);
            convert.put(SaveStatus.AcceptedWithDefinition, ACCEPTED);
            convert.put(SaveStatus.PreCommittedWithDefinition, PREACCEPTED);
            convert.put(SaveStatus.PreCommittedWithAcceptedDeps, ACCEPTED);
            convert.put(SaveStatus.PreCommittedWithDefinitionAndAcceptedDeps, ACCEPTED);
            convert.put(SaveStatus.Committed, COMMITTED);
            convert.put(SaveStatus.Stable, STABLE);
            convert.put(SaveStatus.ReadyToExecute, STABLE);
            convert.put(SaveStatus.PreApplied, STABLE);
            convert.put(SaveStatus.Applying, STABLE);
            convert.put(SaveStatus.Applied, APPLIED);
            convert.put(SaveStatus.TruncatedApplyWithDeps, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.TruncatedApplyWithOutcome, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.TruncatedApply, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.ErasedOrInvalidated, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.Erased, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.Invalidated, INVALID_OR_TRUNCATED);
        }

        public final boolean hasInfo;
        public final NoInfo asNoInfo;
        public final InfoWithAdditions asNoInfoOrAdditions;
        final InternalStatus expectMatch;

        InternalStatus(boolean hasInfo)
        {
            this(hasInfo, true);
        }

        InternalStatus(boolean hasInfo, boolean expectMatch)
        {
            this.hasInfo = hasInfo;
            this.asNoInfo = new NoInfo(this);
            this.asNoInfoOrAdditions = new InfoWithAdditions(asNoInfo, NO_TXNIDS, 0);
            this.expectMatch = expectMatch ? this : null;
        }

        boolean hasExecuteAt()
        {
            return hasInfo;
        }

        boolean hasDeps()
        {
            return hasInfo;
        }

        boolean hasStableDeps()
        {
            return this == STABLE || this == APPLIED;
        }

        public Timestamp depsKnownBefore(TxnId txnId, @Nullable Timestamp executeAt)
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled InternalStatus: " + this);
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED:
                case HISTORICAL:
                    throw new AssertionError("Invalid InternalStatus to know deps");

                case PREACCEPTED:
                case ACCEPTED:
                    return txnId;

                case APPLIED:
                case STABLE:
                case COMMITTED:
                    return executeAt == null ? txnId : executeAt;
            }
        }

        @VisibleForTesting
        public static InternalStatus from(SaveStatus status)
        {
            return convert.get(status);
        }

        public static InternalStatus get(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    public static class Info
    {
        public final InternalStatus status;
        // ACCESS DIRECTLY WITH CARE: if null, TxnId is implied; use accessor method to ensure correct value is returned
        public final @Nullable Timestamp executeAt;
        public final TxnId[] missing; // those TxnId we know of that would be expected to be found in the provided deps, but aren't

        private Info(InternalStatus status, @Nullable Timestamp executeAt, TxnId[] missing)
        {
            this.status = status;
            this.executeAt = executeAt;
            this.missing = missing;
        }

        public static Info create(@Nonnull TxnId txnId, InternalStatus status, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing)
        {
            return new Info(status,
                            Invariants.checkArgument(executeAt, executeAt == txnId || executeAt.compareTo(txnId) > 0),
                            Invariants.checkArgument(missing, missing == NO_TXNIDS || missing.length > 0));
        }

        public static Info createMock(InternalStatus status, @Nullable Timestamp executeAt, TxnId[] missing)
        {
            return new Info(status, executeAt, Invariants.checkArgument(missing, missing == null || missing == NO_TXNIDS));
        }

        @VisibleForTesting
        public Timestamp executeAt(TxnId txnId)
        {
            return executeAt;
        }

        Timestamp depsKnownBefore(TxnId txnId)
        {
            return status.depsKnownBefore(txnId, executeAt);
        }

        Info update(TxnId txnId, TxnId[] newMissing)
        {
            return newMissing == NO_TXNIDS && executeAt == txnId ? status.asNoInfo : Info.create(txnId, status, executeAt(txnId), newMissing);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return status == info.status && Objects.equals(executeAt, info.executeAt) && Arrays.equals(missing, info.missing);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "Info{" +
                   "status=" + status +
                   ", executeAt=" + executeAt +
                   ", missing=" + Arrays.toString(missing) +
                   '}';
        }
    }

    public static class NoInfo extends Info
    {
        NoInfo(InternalStatus status)
        {
            super(status, null, NO_TXNIDS);
        }

        public Timestamp executeAt(TxnId txnId)
        {
            return txnId;
        }
    }

    static class Cursor
    {
        int index()
        {

        }
        boolean next()
        {

        }
    }

    private static class Index
    {
        static final int[] NO_INTS = new int[0];
        static final Index EMPTY = new Index(NO_INTS, -1, -1, -1);

        final int[] outOfOrder;
        // the min/max indexes at which we might find one of these statuses NOT the index of the min/max
        final int minActive, minUncommitted, minStable, maxStable, maxApplied;
        final Timestamp maxCommittedWrite;

        private Index(int[] outOfOrder, int minStable, int maxStableOrApplied, int maxApplied)
        {
            this.outOfOrder = outOfOrder;
            this.minStable = minStable;
            this.maxStableOrApplied = maxStableOrApplied;
            this.maxApplied = maxApplied;
        }

        Index update(int updatePos, Info prevInfo, Info newInfo)
        {

        }

        Index update(int updatePos, Info prevInfo, InfoWithAdditions newInfo)
        {

        }

        Index removeBefore(int index)
        {

        }

        <P> void inExecuteAtOrder(TxnId[] txnIds, Info[] infos, P param, TriConsumer<TxnId, Info, P> consume)
        {

        }

        static Index build(TxnId[] txnIds, Info[] infos)
        {

        }

        Timestamp maxCommittedWriteBefore(Timestamp before, TxnId[] txnIds, Info[] infos)
        {
            Timestamp maxCommitted = this.maxCommittedWrite;
            if (maxCommitted == null)
                return null;

            if (maxCommitted.compareTo(before) < 0)
                return maxCommitted;

            int i = Arrays.binarySearch(txnIds, before);
            if (i < 0) i = -1 -i;
            i = maxWriteIndex(txnIds, infos, (id, info, b) -> info.status.hasStableDeps() && info.executeAt(id).compareTo(b) <= 0, before, i);
            return i < 0 ? null : infos[i].executeAt(txnIds[i]);
        }

        // find the first command to execute that started on-or-after the provided index
        public Cursor byExecuteAtStartedOnOrAfter(int startIndex)
        {
        }

        public int maxIndexExecutesBefore(int startPos, Timestamp executeAt)
        {
        }

        public Cursor executesAfterByExecuteAt(int startIndex)
        {
        }
    }

    private final Key key;
    private final TxnId redundantBefore;
    private final TxnId[] txnIds;
    private final Info[] infos;
    private final Index index;

    private CommandsForKey(Key key, TxnId redundantBefore, TxnId[] txnIds, Info[] infos)
    {
        this(key, redundantBefore, txnIds, infos, Index.build(txnIds, infos));
    }

    CommandsForKey(Key key, TxnId redundantBefore, TxnId[] txnIds, Info[] infos, Index index)
    {
        this.key = key;
        this.redundantBefore = redundantBefore;
        this.txnIds = txnIds;
        this.infos = infos;
        this.index = index;
        if (isParanoid()) Invariants.checkArgument(SortedArrays.isSortedUnique(txnIds));
    }

    public CommandsForKey(Key key)
    {
        this.key = key;
        this.redundantBefore = TxnId.NONE;
        this.txnIds = NO_TXNIDS;
        this.infos = NO_INFOS;
        this.index = Index.EMPTY;
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    public Key key()
    {
        return key;
    }

    public int size()
    {
        return txnIds.length;
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId);
    }

    public TxnId txnId(int i)
    {
        return txnIds[i];
    }

    public Info info(int i)
    {
        return infos[i];
    }

    public TxnId redundantBefore()
    {
        return redundantBefore;
    }

    /**
     * All commands before/after (exclusive of) the given timestamp
     * <p>
     * Note that {@code testDep} applies only to commands that MAY have the command in their deps; if specified any
     * commands that do not know any deps will be ignored, as will any with an executeAt prior to the txnId.
     * <p>
     */
    public <P1, T> T mapReduceFull(TxnId testTxnId,
                                   Kinds testKind,
                                   TestStartedAt testStartedAt,
                                   TestDep testDep,
                                   TestStatus testStatus,
                                   CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        int start, end;
        boolean isKnown;
        {
            int insertPos = Arrays.binarySearch(txnIds, testTxnId);
            isKnown = insertPos >= 0;
            if (!isKnown && testDep == WITH) return initialValue;
            if (!isKnown) insertPos = -1 - insertPos;
            switch (testStartedAt)
            {
                default: throw new AssertionError("Unhandled TestStartedAt: " + testTxnId);
                case STARTED_BEFORE: start = 0; end = insertPos; break;
                case STARTED_AFTER: start = insertPos; end = txnIds.length; break;
                case ANY: start = 0; end = txnIds.length;
            }
        }

        for (int i = start; i < end ; ++i)
        {
            TxnId txnId = txnIds[i];
            if (!testKind.test(txnId.kind())) continue;

            Info info = infos[i];
            InternalStatus status = info.status;
            switch (testStatus)
            {
                default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                case IS_PROPOSED:
                    if (status == ACCEPTED || status == COMMITTED) break;
                    else continue;
                case IS_STABLE:
                    if (status.compareTo(STABLE) >= 0 && status.compareTo(INVALID_OR_TRUNCATED) < 0) break;
                    else continue;
                case ANY_STATUS:
                    if (status == TRANSITIVELY_KNOWN)
                        continue;
            }

            Timestamp executeAt = info.executeAt(txnId);
            if (testDep != ANY_DEPS)
            {
                if (!status.hasInfo)
                    continue;

                if (executeAt.compareTo(testTxnId) <= 0)
                    continue;

                boolean hasAsDep = Arrays.binarySearch(info.missing, testTxnId) < 0;
                if (hasAsDep != (testDep == WITH))
                    continue;
            }

            initialValue = map.apply(p1, key, txnId, executeAt, initialValue);
        }
        return initialValue;
    }

    public <P1, T> T mapReduceActive(Timestamp startedBefore,
                                     Kinds testKind,
                                     CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        Timestamp maxCommitted = index.maxCommittedWriteBefore(startedBefore, txnIds, infos);
        int start = index.minActive, end = insertPos(start, startedBefore);

        for (int i = start; i < end ; ++i)
        {
            TxnId txnId = txnIds[i];
            if (!testKind.test(txnId.kind()))
                continue;

            Info info = infos[i];
            switch (info.status)
            {
                case COMMITTED:
                case STABLE:
                case APPLIED:
                    // TODO (expected): prove the correctness of this approach
                    if (!PRUNE_TRANSITIVE_DEPENDENCIES || maxCommitted == null || info.executeAt(txnId).compareTo(maxCommitted) >= 0)
                        break;
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED:
                    continue;
            }

            initialValue = map.apply(p1, key, txnId, info.executeAt(txnId), initialValue);
        }
        return initialValue;
    }

    public CommandsForKey update(SafeCommandStore safeStore, Command prev, Command next)
    {
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            return this;

        TxnId txnId = next.txnId();
        int pos = Arrays.binarySearch(txnIds, txnId);
        if (pos < 0)
        {
            pos = -1 - pos;
            if (newStatus.hasInfo) return insert(pos, txnId, newStatus, next);
            else return insert(pos, txnId, newStatus.asNoInfo);
        }
        else
        {
            // we do not permit multiple versions of the same TxnId, so use the existing one
            txnId = txnIds[pos];

            // update
            InternalStatus prevStatus = prev == null ? null : InternalStatus.from(prev.saveStatus());
            if (newStatus == prevStatus && (!newStatus.hasInfo || next.acceptedOrCommitted().equals(prev.acceptedOrCommitted())))
                return this;

            @Nonnull Info cur = Invariants.nonNull(infos[pos]);
            // TODO (required): HACK to permit prev.saveStatus() == SaveStatus.AcceptedInvalidateWithDefinition as we don't always update as keys aren't guaranteed to be provided
            //    fix as soon as we support async updates
            Invariants.checkState(cur.status.expectMatch == prevStatus || (prev != null && prev.status() == Status.AcceptedInvalidate));

            if (newStatus.hasInfo) return update(pos, txnId, cur, newStatus, next);
            else return update(pos, cur, newStatus.asNoInfo);
        }
    }

    public static boolean needsUpdate(Command prev, Command updated)
    {
        if (!updated.txnId().kind().isGloballyVisible())
            return false;

        SaveStatus prevStatus;
        Ballot prevAcceptedOrCommitted;
        if (prev == null)
        {
            prevStatus = SaveStatus.NotDefined;
            prevAcceptedOrCommitted = Ballot.ZERO;
        }
        else
        {
            prevStatus = prev.saveStatus();
            prevAcceptedOrCommitted = prev.acceptedOrCommitted();
        }

        return needsUpdate(prevStatus, prevAcceptedOrCommitted, updated.saveStatus(), updated.acceptedOrCommitted());
    }

    public static boolean needsUpdate(SaveStatus prevStatus, Ballot prevAcceptedOrCommitted, SaveStatus updatedStatus, Ballot updatedAcceptedOrCommitted)
    {
        InternalStatus prev = InternalStatus.from(prevStatus);
        InternalStatus updated = InternalStatus.from(updatedStatus);
        return updated != prev || (updated != null && updated.hasInfo && !prevAcceptedOrCommitted.equals(updatedAcceptedOrCommitted));
    }

    @Inline
    private static <P> int maxWriteIndex(TxnId[] txnIds, Info[] infos, TriPredicate<TxnId, Info, P> test, P param, int beforeIndex)
    {
        int i = beforeIndex;
        Timestamp max = null;
        int maxIndex = -1;
        while (--i >= 0)
        {
            TxnId txnId = txnIds[i];
            if (txnId.kind() != Write) continue;

            Info info = infos[i];
            if (!test.test(txnId, info, param)) continue;

            max = info.executeAt(txnId);
            maxIndex = i;
            break;
        }

        while (--i >= 0)
        {
            TxnId txnId = txnIds[i];
            if (txnId.kind() != Write) continue;

            Info info = infos[i];
            if (info.getClass() == NoInfo.class) continue;
            if (info.executeAt == txnId) continue;
            if (!test.test(txnId, info, param)) continue;
            if (info.executeAt.compareTo(max) <= 0) continue;

            max = info.executeAt;
            maxIndex = i;
        }
        return maxIndex;
    }

    private CommandsForKey insert(int insertPos, TxnId insertTxnId, InternalStatus newStatus, Command command)
    {
        InfoWithAdditions newInfo = computeInsert(insertPos, insertTxnId, newStatus, command);
        if (newInfo.additionCount == 0)
            return insert(insertPos, insertTxnId, newInfo.info);

        TxnId[] newTxnIds = new TxnId[txnIds.length + newInfo.additionCount + 1];
        Info[] newInfos = new Info[newTxnIds.length];
        insertWithAdditions(insertPos, insertTxnId, newInfo, newTxnIds, newInfos);
        return update(newTxnIds, newInfos, index.update(insertPos, null, newInfo));
    }

    private CommandsForKey update(int updatePos, TxnId txnId, Info prevInfo, InternalStatus newStatus, Command command)
    {
        InfoWithAdditions newInfo = computeUpdate(updatePos, txnId, newStatus, command);
        if (newInfo.additionCount == 0)
            return update(updatePos, prevInfo, newInfo.info);

        TxnId[] newTxnIds = new TxnId[txnIds.length + newInfo.additionCount];
        Info[] newInfos = new Info[newTxnIds.length];
        int newPos = updateWithAdditions(updatePos, txnIds[updatePos], newInfo, newTxnIds, newInfos);
        if (prevInfo.status.compareTo(COMMITTED) < 0 && newStatus.compareTo(COMMITTED) >= 0)
            removeMissing(newTxnIds, newInfos, newPos);
        return update(newTxnIds, newInfos, index.update(updatePos, prevInfo, newInfo));
    }

    private int updateWithAdditions(int updatePos, TxnId updateTxnId, InfoWithAdditions withInfo, TxnId[] newTxnIds, Info[] newInfos)
    {
        return updateOrInsertWithAdditions(updatePos, updatePos, updateTxnId, withInfo, newTxnIds, newInfos);
    }

    private void insertWithAdditions(int pos, TxnId updateTxnId, InfoWithAdditions withInfo, TxnId[] newTxnIds, Info[] newInfos)
    {
        updateOrInsertWithAdditions(pos, -1, updateTxnId, withInfo, newTxnIds, newInfos);
    }

    private int updateOrInsertWithAdditions(int sourceInsertPos, int sourceUpdatePos, TxnId updateTxnId, InfoWithAdditions withInfo, TxnId[] newTxnIds, Info[] newInfos)
    {
        TxnId[] additions = withInfo.additions;
        int additionCount = withInfo.additionCount;
        int additionInsertPos = Arrays.binarySearch(additions, 0, additionCount, updateTxnId);
        additionInsertPos = Invariants.checkArgument(-1 - additionInsertPos, additionInsertPos < 0);
        int targetInsertPos = sourceInsertPos + additionInsertPos;

        // additions plus the updateTxnId when necessary
        TxnId[] missingSource = additions;
        boolean insertSelfMissing = sourceUpdatePos < 0 && withInfo.info.status.compareTo(COMMITTED) < 0;

        // the most recently constructed pure insert missing array, so that it may be reused if possible
        TxnId[] cachedMissing = null;
        int i = 0, j = 0, missingCount = 0, missingLimit = additionCount, count = 0;
        while (i < infos.length)
        {
            if (count == targetInsertPos)
            {
                newTxnIds[count] = updateTxnId;
                newInfos[count] = withInfo.info;
                if (i == sourceUpdatePos) ++i;
                else if (insertSelfMissing) ++missingCount;
                ++count;
                continue;
            }

            int c = j == additionCount ? -1 : txnIds[i].compareTo(additions[j]);
            if (c < 0)
            {
                TxnId txnId = txnIds[i];
                Info info = infos[i];
                if (i == sourceUpdatePos)
                {
                    info = withInfo.info;
                }
                else if (info.status.hasDeps())
                {
                    Timestamp depsKnownBefore = info.status.depsKnownBefore(txnId, info.executeAt);
                    if (insertSelfMissing && missingSource == additions && (missingCount != j || (depsKnownBefore != txnId && depsKnownBefore.compareTo(updateTxnId) > 0)))
                    {
                        missingSource = mergeMissing(additions, additionCount, updateTxnId, additionInsertPos);
                        ++missingLimit;
                    }

                    int to = to(txnId, depsKnownBefore, missingSource, missingCount, missingLimit);
                    if (to > 0)
                    {
                        TxnId[] missing = info.missing == NO_TXNIDS && to == missingCount
                                          ? cachedMissing = ensureCachedMissing(missingSource, to, cachedMissing)
                                          : linearUnion(info.missing, info.missing.length, missingSource, to, cachedTxnIds());
                        info = info.update(txnId, missing);
                    }
                }
                newTxnIds[count] = txnId;
                newInfos[count] = info;
                i++;
            }
            else if (c > 0)
            {
                newTxnIds[count] = additions[j++];
                ++missingCount;
                newInfos[count] = TRANSITIVELY_KNOWN.asNoInfo;
            }
            else
            {
                throw illegalState(txnIds[i] + " should be an insertion, but found match when merging with origin");
            }
            count++;
        }

        if (j < additionCount)
        {
            if (count <= targetInsertPos)
            {
                int length = targetInsertPos - count;
                System.arraycopy(additions, j, newTxnIds, count, length);
                Arrays.fill(newInfos, count, targetInsertPos, TRANSITIVELY_KNOWN.asNoInfo);
                newTxnIds[targetInsertPos] = updateTxnId;
                newInfos[targetInsertPos] = withInfo.info;
                count = targetInsertPos + 1;
                j = additionInsertPos;
            }
            System.arraycopy(additions, j, newTxnIds, count, additionCount - j);
            Arrays.fill(newInfos, count, count + additionCount - j, TRANSITIVELY_KNOWN.asNoInfo);
        }
        else if (count == targetInsertPos)
        {
            newTxnIds[targetInsertPos] = updateTxnId;
            newInfos[targetInsertPos] = withInfo.info;
        }

        cachedTxnIds().forceDiscard(additions, additionCount);
        return targetInsertPos;
    }

    private CommandsForKey update(int pos, Info curInfo, Info newInfo)
    {
        if (curInfo == newInfo)
            return this;

        Info[] newInfos = infos.clone();
        newInfos[pos] = newInfo;
        if (curInfo.status.compareTo(COMMITTED) < 0 && newInfo.status.compareTo(COMMITTED) >= 0)
            removeMissing(txnIds, newInfos, pos);
        Index index = this.index.update(pos, curInfo, newInfo);
        return update(txnIds, newInfos, index);
    }

    /**
     * Insert a new txnId and info
     */
    private CommandsForKey insert(int pos, TxnId newTxnId, Info newInfo)
    {
        TxnId[] newTxnIds = new TxnId[txnIds.length + 1];
        System.arraycopy(txnIds, 0, newTxnIds, 0, pos);
        newTxnIds[pos] = newTxnId;
        System.arraycopy(txnIds, pos, newTxnIds, pos + 1, txnIds.length - pos);

        Info[] newInfos = new Info[infos.length + 1];
        if (newInfo.status.compareTo(COMMITTED) >= 0)
        {
            System.arraycopy(infos, 0, newInfos, 0, pos);
            newInfos[pos] = newInfo;
            System.arraycopy(infos, pos, newInfos, pos + 1, infos.length - pos);
        }
        else
        {
            insertInfoAndOneMissing(pos, newTxnId, newInfo, infos, newInfos, newTxnIds, 1);
        }
        return update(newTxnIds, newInfos, index.update(pos, null, newInfo));
    }

    /**
     * Insert a new txnId and info, then insert the txnId into the missing collection of any command that should have already caused us to witness it
     */
    private static void insertInfoAndOneMissing(int insertPos, TxnId txnId, Info newInfo, Info[] oldInfos, Info[] newInfos, TxnId[] newTxnIds, int offsetAfterInsertPos)
    {
        TxnId[] oneMissing = null;
        for (int i = 0 ; i < insertPos ; ++i)
        {
            Info oldInfo = oldInfos[i];
            if (oldInfo.getClass() != NoInfo.class)
            {
                Timestamp depsKnownBefore = oldInfo.depsKnownBefore(null);
                if (depsKnownBefore != null && depsKnownBefore.compareTo(txnId) > 0)
                {
                    TxnId[] missing;
                    if (oldInfo.missing == NO_TXNIDS)
                        missing = oneMissing = ensureOneMissing(txnId, oneMissing);
                    else
                        missing = SortedArrays.insert(oldInfo.missing, txnId, TxnId[]::new);

                    newInfos[i] = Info.create(txnId, oldInfo.status, oldInfo.executeAt, missing);
                    continue;
                }
            }
            newInfos[i] = oldInfo;
        }

        newInfos[insertPos] = newInfo;

        for (int i = insertPos; i < oldInfos.length ; ++i)
        {
            Info oldInfo = oldInfos[i];
            if (!oldInfo.status.hasDeps())
            {
                newInfos[i + offsetAfterInsertPos] = oldInfo;
                continue;
            }

            TxnId[] missing;
            if (oldInfo.missing == NO_TXNIDS)
                missing = oneMissing = ensureOneMissing(txnId, oneMissing);
            else
                missing = SortedArrays.insert(oldInfo.missing, txnId, TxnId[]::new);

            int newIndex = i + offsetAfterInsertPos;
            newInfos[newIndex] = Info.create(newTxnIds[newIndex], oldInfo.status, oldInfo.executeAt(newTxnIds[newIndex]), missing);
        }
    }

    private static void removeMissing(TxnId[] txnIds, Info[] infos, int pos)
    {
        TxnId removeTxnId = txnIds[pos];
        for (int i = 0 ; i < infos.length ; ++i)
        {
            Info info = infos[i];
            if (info.getClass() == NoInfo.class) continue;

            TxnId[] missing = info.missing;
            if (missing == NO_TXNIDS) continue;

            // linear scan on object identity going to be faster in practice than binary search almost every time
            int j = 0;
            while (j < missing.length && missing[j] != removeTxnId) ++j;
            if (j == missing.length) continue;

            if (missing.length == 1)
            {
                missing = NO_TXNIDS;
            }
            else
            {
                int length = missing.length;
                TxnId[] newMissing = new TxnId[length - 1];
                System.arraycopy(missing, 0, newMissing, 0, j);
                System.arraycopy(missing, j + 1, newMissing, j, length - (1 + j));
                missing = newMissing;
            }

            infos[i] = info.update(txnIds[i], missing);
        }
    }

    private static TxnId[] ensureOneMissing(TxnId txnId, TxnId[] oneMissing)
    {
        return oneMissing != null ? oneMissing : new TxnId[] { txnId };
    }

    private static TxnId[] ensureCachedMissing(TxnId[] missingIds, int missingIdCount, TxnId[] cachedMissing)
    {
        return cachedMissing != null && cachedMissing.length == missingIdCount ? cachedMissing : Arrays.copyOf(missingIds, missingIdCount);
    }

    private static TxnId[] mergeMissing(TxnId[] additions, int additionCount, TxnId updateTxnId, int additionInsertPos)
    {
        TxnId[] missingSource = new TxnId[additionCount + 1];
        System.arraycopy(additions, 0, missingSource, 0, additionInsertPos);
        System.arraycopy(additions, additionInsertPos, missingSource, additionInsertPos + 1, additionCount - additionInsertPos);
        missingSource[additionInsertPos] = updateTxnId;
        return missingSource;
    }

    private static int to(TxnId txnId, Timestamp depsKnownBefore, TxnId[] missingSource, int missingCount, int missingLimit)
    {
        if (depsKnownBefore == txnId) return missingCount;
        int to = Arrays.binarySearch(missingSource, 0, missingLimit, depsKnownBefore);
        if (to < 0) to = -1 - to;
        return to;
    }

    static class InfoWithAdditions
    {
        final Info info;
        final TxnId[] additions;
        final int additionCount;

        InfoWithAdditions(Info info, TxnId[] additions, int additionCount)
        {
            this.info = info;
            this.additions = additions;
            this.additionCount = additionCount;
        }
    }

    private InfoWithAdditions computeInsert(int insertPos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        return computeInfoAndAdditions(insertPos, -1, txnId, newStatus, command);
    }

    private InfoWithAdditions computeUpdate(int updatePos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        return computeInfoAndAdditions(updatePos, updatePos, txnId, newStatus, command);
    }

    private InfoWithAdditions computeInfoAndAdditions(int insertPos, int updatePos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        Timestamp executeAt = txnId;
        if (newStatus.hasInfo)
        {
            executeAt = command.executeAt();
            if (executeAt.equals(txnId)) executeAt = txnId;
        }
        Timestamp depsKnownBefore = newStatus.depsKnownBefore(txnId, executeAt);
        return computeInfoAndAdditions(insertPos, updatePos, txnId, newStatus, executeAt, depsKnownBefore, command.partialDeps().keyDeps.txnIds(key));
    }

    private InfoWithAdditions computeInfoAndAdditions(int insertPos, int updatePos, TxnId txnId, InternalStatus newStatus, Timestamp executeAt, Timestamp depsKnownBefore, SortedList<TxnId> deps)
    {
        int depsKnownBeforePos;
        if (depsKnownBefore == txnId)
        {
            depsKnownBeforePos = insertPos;
        }
        else
        {
            depsKnownBeforePos = Arrays.binarySearch(txnIds, insertPos, txnIds.length, depsKnownBefore);
            Invariants.checkState(depsKnownBeforePos < 0);
            depsKnownBeforePos = -1 - depsKnownBeforePos;
        }

        TxnId[] additions = NO_TXNIDS, missing = NO_TXNIDS;
        int additionCount = 0, missingCount = 0;

        int depsIndex = deps.find(redundantBefore);
        if (depsIndex < 0) depsIndex = -1 - depsIndex;
        int txnIdsIndex = 0;
        while (txnIdsIndex < depsKnownBeforePos && depsIndex < deps.size())
        {
            TxnId t = txnIds[txnIdsIndex];
            TxnId d = deps.get(depsIndex);
            int c = t.compareTo(d);
            if (c == 0)
            {
                ++txnIdsIndex;
                ++depsIndex;
            }
            else if (c < 0)
            {
                // we expect to be missing ourselves
                // we also permit any transaction we have recorded as COMMITTED or later to be missing, as recovery will not need to consult our information
                if (txnIdsIndex != updatePos && infos[txnIdsIndex].status.compareTo(COMMITTED) < 0)
                {
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                    missing[missingCount++] = t;
                }
                txnIdsIndex++;
            }
            else
            {
                if (additionCount >= additions.length)
                    additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));

                additions[additionCount++] = d;
                depsIndex++;
            }
        }

        while (txnIdsIndex < depsKnownBeforePos)
        {
            if (txnIdsIndex != updatePos && infos[txnIdsIndex].status.compareTo(COMMITTED) < 0)
            {
                TxnId t = txnIds[txnIdsIndex];
                if (missingCount == missing.length)
                    missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                missing[missingCount++] = t;
            }
            txnIdsIndex++;
        }

        while (depsIndex < deps.size())
        {
            if (additionCount >= additions.length)
                additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));
            additions[additionCount++] = deps.get(depsIndex++);
        }

        if (missingCount == 0 && executeAt == txnId)
            return additionCount == 0 ? newStatus.asNoInfoOrAdditions : new InfoWithAdditions(newStatus.asNoInfo, additions, additionCount);

        return new InfoWithAdditions(Info.create(txnId, newStatus, executeAt, cachedTxnIds().completeAndDiscard(missing, missingCount)), additions, additionCount);
    }

    private CommandsForKey update(TxnId[] newTxnIds, Info[] newInfos, Index index)
    {
        return new CommandsForKey(key, redundantBefore, newTxnIds, newInfos, index);
    }

    public void updateWaitingOn(TxnId waitingId, int keyIndex, WaitingOn.Update update)
    {
        // TODO (expected): use Index to save some computation
        int txnIdx = indexOf(waitingId);
        Timestamp executeAt = waitingId;
        int executeIdx = txnIdx;
        Info waitingInfo = infos[txnIdx];
        Invariants.checkState(waitingInfo.status == STABLE);
        if (waitingInfo.executeAt != null)
        {
            executeAt = waitingInfo.executeAt;
            executeIdx = SortedArrays.exponentialSearch(txnIds, txnIdx, txnIds.length, executeAt);
            if (executeIdx < 0) executeIdx = -1 - executeIdx;
        }
        for (int i = executeIdx - 1; i >= 0 ; --i)
        {
            Info info = infos[i];
            if (info == APPLIED.asNoInfo) continue;
            if (info == INVALID_OR_TRUNCATED.asNoInfo) continue;

            switch (info.status)
            {
                case APPLIED:
                case INVALID_OR_TRUNCATED:
                    continue;

                case HISTORICAL:
                case TRANSITIVELY_KNOWN:
                case PREACCEPTED:
                case ACCEPTED:
                    if (waitingInfo.missing == NO_TXNIDS || Arrays.binarySearch(waitingInfo.missing, txnIds[i]) < 0)
                        return; // still waiting

                case STABLE:
                case COMMITTED:
                    if (info.executeAt == null || info.executeAt.compareTo(executeAt) < 0)
                        return; // must still be waiting

            }
        }

        update.removeWaitingOnKey(keyIndex);
    }

    public void notify(int updatedPos, InternalStatus prevStatus, SafeCommandStore safeStore)
    {
        // first notify anything that might now be ready to execute
        TxnId updatedTxnId = txnIds[updatedPos];
        Info updatedInfo = infos[updatedPos];
        switch (updatedInfo.status)
        {
            case TRANSITIVELY_KNOWN:
            case PREACCEPTED:
            case HISTORICAL:
            case ACCEPTED:
//                if (index.)
                break;

            case STABLE:
            case COMMITTED:
            {
                // somebody waiting on us might now be ready to execute, but only if we execute after them and were not previously committed
                if (prevStatus == COMMITTED || updatedInfo.executeAt == null || updatedInfo.executeAt == updatedTxnId)
                    break;

                int minIndex = index.minUncommitted;
                int lastIndex = index.maxIndexExecutesBefore(updatedPos, updatedInfo.executeAt);
                for (int i = updatedPos + 1; i < lastIndex ; ++i)
                {
                    TxnId txnId = txnIds[i];
                    Info info = infos[i];
                    if (info.status.compareTo(COMMITTED) < 0) continue;
                    if (info.executeAt(txnId).compareTo(updatedInfo.executeAt) > 0) continue;

                    if (txnId.kind().witnesses().test(updatedTxnId.kind()))
                    {
                        for (int j = info.missing.length - 1; j > 0 ; --j)
                        {
                            
                        }
                    }

                    // writes are witnessed by everything, so can terminate once we've visited a committed one
                    // as anything later must depend on this write
                    if (txnId.kind() == Write)
                        break;
                }
                break;
            }

            case INVALID_OR_TRUNCATED:
            case APPLIED:
            {
                // somebody might now be ready to execute
                Cursor cursor = index.executesAfterByExecuteAt(updatedPos);
                while (cursor.next())
                {
                    int i = cursor.index();
                    Info info = infos[i];
                    if (info.)
                    TxnId txnId = txnIds[i];
                }
            }
        }
    }

    public CommandsForKey withoutRedundant(TxnId redundantBefore)
    {
        if (this.redundantBefore.compareTo(redundantBefore) >= 0)
            return this;

        int pos = insertPos(0, redundantBefore);
        if (pos == 0)
            return new CommandsForKey(key, redundantBefore, txnIds, infos, index);

        TxnId[] newTxnIds = Arrays.copyOfRange(txnIds, pos, txnIds.length);
        Info[] newInfos = Arrays.copyOfRange(infos, pos, infos.length);
        for (int i = 0 ; i < newInfos.length ; ++i)
        {
            Info info = newInfos[i];
            if (info.getClass() == NoInfo.class) continue;
            if (info.missing == NO_TXNIDS) continue;
            int j = Arrays.binarySearch(info.missing, redundantBefore);
            if (j < 0) j = -1 - j;
            if (j <= 0) continue;
            TxnId[] newMissing = j == info.missing.length ? NO_TXNIDS : Arrays.copyOfRange(info.missing, j, info.missing.length);
            newInfos[i] = info.update(txnIds[i], newMissing);
        }
        return new CommandsForKey(key, redundantBefore, newTxnIds, newInfos, index.removeBefore(pos));
    }

    public CommandsForKey registerHistorical(TxnId txnId)
    {
        int i = Arrays.binarySearch(txnIds, txnId);
        if (i >= 0)
            return infos[i].status.compareTo(HISTORICAL) >= 0 ? this : update(i, infos[i], HISTORICAL.asNoInfo);

        return insert(-1 - i, txnId, HISTORICAL.asNoInfo);
    }

    private int insertPos(int min, Timestamp timestamp)
    {
        int i = Arrays.binarySearch(txnIds, min, txnIds.length, timestamp);
        if (i < 0) i = -1 -i;
        return i;
    }

    public TxnId findFirst()
    {
        return txnIds.length > 0 ? txnIds[0] : null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return Objects.equals(key, that.key)
               && Objects.equals(redundantBefore, that.redundantBefore)
               && Arrays.equals(txnIds, that.txnIds)
               && Arrays.equals(infos, that.infos);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
