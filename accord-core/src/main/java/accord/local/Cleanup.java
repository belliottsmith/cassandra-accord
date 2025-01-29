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

import javax.annotation.Nonnull;

import accord.api.Agent;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_ONLY;
import static accord.local.RedundantStatus.Property.TRUNCATE_BEFORE;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.SaveStatus.Vestigial;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.Any;

/**
 * Logic related to whether metadata about transactions is safe to discard given currently available information.
 * The data may not be completely discarded if parts of it will still be necessary.
 */
public enum Cleanup
{
    NO(Uninitialised),
    // we don't know if the command has been applied or invalidated as we have incomplete information
    // so erase what information we don't need in future to decide this
    TRUNCATE_WITH_OUTCOME(TruncatedApplyWithOutcome),
    TRUNCATE(TruncatedApply),
    VESTIGIAL(Vestigial),
    INVALIDATE(Invalidated),
    ERASE(Erased),
    // we can stop storing the (inspected portion of the) record entirely
    EXPUNGE(Erased);

    private static final Cleanup[] VALUES = values();

    public final SaveStatus appliesIfNot;

    Cleanup(SaveStatus appliesIfNot)
    {
        this.appliesIfNot = appliesIfNot;
    }

    public final Cleanup filter(SaveStatus saveStatus)
    {
        return saveStatus.compareTo(appliesIfNot) >= 0 ? NO : this;
    }

    public enum Input { PARTIAL, FULL }

    // TODO (required): simulate compaction of log records in burn test
    public static Cleanup shouldCleanup(Input input, SafeCommandStore safeStore, Command command)
    {
        return shouldCleanup(input, safeStore, command, command.participants());
    }

    public static Cleanup shouldCleanup(Input input, SafeCommandStore safeStore, Command command, @Nonnull StoreParticipants participants)
    {
        return shouldCleanup(input, safeStore.agent(), command.txnId(), command.executeAt(), command.saveStatus(), command.durability(), participants,
                             safeStore.redundantBefore(), safeStore.durableBefore());
    }

    public static Cleanup shouldCleanup(Input input, Agent agent, Command command, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanup(input, agent, command.txnId(), command.executeAt(), command.saveStatus(), command.durability(), command.participants(),
                             redundantBefore, durableBefore);
    }

    public static Cleanup shouldCleanup(Input input, Agent agent, TxnId txnId, Timestamp executeAt, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Cleanup cleanup = shouldCleanupInternal(input, agent, txnId, executeAt, status, durability, participants, redundantBefore, durableBefore);
        return cleanup.filter(status);
    }

    /**
     * Decide if (and how) we can cleanup the transaction in question.
     * </p>
     * We can ERASE data once we are certain no other replicas require our information.
     * We can EXPUNGE data once we can reliably and safely EXPUNGE any partial record.
     * To achieve the latter, we use only global summary information and the TxnId -
     * and if present any applyAt.
     *
     * [If implementations require unique HLCs they must guarantee to save any applyAt alongside
     * a Route that can be used to report the applyAt to any participating keys on restart.]
     *
     * </p>
     * A transaction may be truncated as soon as it is durable locally and all shard(s)
     * the CommandStore participates in, but must retain the transaction Outcome until
     * it is durable at a majority of replicas on all shards.
     * This permits other shards to contact us for recovery information.
     *
     * Note importantly that we cannot safely truncate commands that are pre-bootstrap that have
     * not yet been applied, as we may be a member of the quorum that coordinated the command, even
     * if we have not bootstrapped the range.
     * TODO (expected): this requirement could be restricted to the home shard.
     * </p>
     * If we know a transaction is invalidated, but don't know its FullRoute,
     * we pessimistically assume the whole cluster may need to see its outcome
     * TODO (expected): we should be able to rely on replicas to infer Invalidated from an Erased record
     */
    private static Cleanup shouldCleanupInternal(Input input, Agent agent, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.kind() == EphemeralRead)
            return NO;

        if (expunge(txnId, executeAt, saveStatus, participants, redundantBefore, durableBefore))
            return expunge();

        if (participants == null)
            return NO;

        if (participants.hasFullRoute())
            return cleanupWithFullRoute(input, agent, participants, txnId, executeAt, saveStatus, durability, redundantBefore, durableBefore);
        return cleanupWithoutFullRoute(input, txnId, saveStatus, participants, redundantBefore, durableBefore);
    }

    private static Cleanup cleanupWithFullRoute(Input input, Agent agent, StoreParticipants participants, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // We first check if the command is redundant locally, i.e. whether it has been applied to all non-faulty replicas of the local shard
        // If not, we don't want to truncate its state else we may make catching up for these other replicas much harder
        FullRoute<?> route = Route.castToFullRoute(participants.route());
        if (!saveStatus.known.is(ApplyAtKnown)) executeAt = null;
        RedundantStatus redundant = redundantBefore.status(txnId, executeAt, route);
        Invariants.require(redundant.none(NOT_OWNED),"Command " + txnId + " that is being loaded is not owned by this shard on route " + route);

        if (redundant.none(LOCALLY_REDUNDANT))
            return NO;

        Cleanup ifUndecided = cleanupIfUndecidedOrDefunctWithFullRoute(input, txnId, saveStatus, redundant, redundant.all(TRUNCATE_BEFORE) ? null : NO);
        if (ifUndecided != null)
            return ifUndecided;

        if (input == FULL)
        {
            Participants<?> executes = participants.stillExecutes();
            if (!saveStatus.hasBeen(Applied) && (executes == null || (!executes.isEmpty() && redundantBefore.preBootstrapOrStale(txnId, executes) != ALL)))
            {
                // if we should execute this transaction locally, and we have not done so by the time we reach a GC point, something has gone wrong
                TxnId supersededBy = redundantBefore.max(participants.route(), e -> e.shardAppliedBefore);
                Participants<?> on = executes.slice(redundantBefore.foldl(participants.route(), (e, r, s) -> s.equals(e.shardAppliedBefore) ? r.with(Ranges.of(e.range)) : r, Ranges.EMPTY, supersededBy), Minimal);
                String message = "Loading " + redundant + " command " + txnId + " with status " + saveStatus + " (that should have been Applied). Expected to be witnessed and executed by " + supersededBy + ".";
                agent.onViolation(message, on, txnId, executeAt, supersededBy, supersededBy);
                return truncate(txnId);
            }
        }

        Durability test = Durability.max(durability, durableBefore.min(txnId, participants.route()));
        switch (test)
        {
            default: throw new UnhandledEnum(durability);
            case Local:
            case NotDurable:
            case ShardUniversal:
                return truncateWithOutcome();

            case MajorityOrInvalidated:
            case Majority:
                return truncate(txnId);

            case UniversalOrInvalidated:
            case Universal:
                if (redundant.get(GC_BEFORE) == ALL)
                    return erase();
                return truncate(txnId);
        }
    }

    private static Cleanup cleanupWithoutFullRoute(Input input, TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // TODO (expected): consider if we can truncate more aggressively partial records, although we cannot infer anything from the fact they're undecided
        if (input == PARTIAL || saveStatus.hasBeen(Truncated))
            return NO;

        Invariants.require(!saveStatus.hasBeen(PreCommitted));
        if (participants.owns().isEmpty())
        {
            // we don't want to erase something that we only don't own because it hasn't been initialised
            if (saveStatus == Uninitialised)
                return NO;

            if (txnId.compareTo(redundantBefore.minShardRedundantBefore()) >= 0)
                return NO;

            return vestigial(txnId);
        }

        RedundantStatus redundant = redundantBefore.status(txnId, null, participants.owns());
        return cleanupUndecided(txnId, redundant);
    }

    private static Cleanup cleanupIfUndecidedOrDefunctWithFullRoute(Input input, TxnId txnId, SaveStatus saveStatus, RedundantStatus redundantStatus, Cleanup ifDecided)
    {
        if (saveStatus.hasBeen(PreCommitted))
        {
            // TODO (required): consider more the invariants we're guaranteeing here, particularly with respect to other shards
            //  also consider whether we interfere with stronger cleanup that would run after in cleanupWithFullRoute
            if (input != PARTIAL && redundantStatus.all(SHARD_APPLIED_ONLY) && !redundantStatus.any(LOCALLY_APPLIED) && redundantStatus.all(LOCALLY_DEFUNCT))
                return truncate(txnId);
            return ifDecided;
        }

        if (input == PARTIAL)
            return NO;

        return cleanupUndecided(txnId, redundantStatus);
    }

    private static Cleanup cleanupUndecided(TxnId txnId, RedundantStatus redundantStatus)
    {
        if (redundantStatus.any(LOCALLY_APPLIED))
            return invalidate(txnId);

        if (redundantStatus.all(SHARD_APPLIED_AND_LOCALLY_REDUNDANT))
            return vestigial(txnId);

        return NO;
    }

    private static boolean expunge(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.is(Any) && durableBefore.min(txnId) != UniversalOrInvalidated)
            return false;

        // since we cannot guarantee to witness participants for all records, we must use the global durableBefore bounds
        TxnId minGcBefore = redundantBefore.minGcBefore();
        if (minGcBefore.compareTo(txnId) <= 0)
            return false;

        if (!requiresUniqueHlcs() || !txnId.is(Write)) return true;
        if (!saveStatus.known.is(ApplyAtKnown)) return true;
        if (executeAt == null) return true;
        if (minGcBefore.is(HLC_BOUND) && executeAt.uniqueHlc() < minGcBefore.hlc()) return true;
        return participants.executes().isEmpty();
    }

    public static Cleanup forOrdinal(int ordinal)
    {
        return VALUES[ordinal];
    }

    // convenient for debugging
    private static Cleanup invalidate(TxnId txnId)
    {
        return INVALIDATE;
    }
    private static Cleanup truncateWithOutcome() { return TRUNCATE_WITH_OUTCOME; }
    private static Cleanup truncate(TxnId txnId)
    {
        return TRUNCATE;
    }
    private static Cleanup vestigial(TxnId txnId)
    {
        return VESTIGIAL;
    }
    private static Cleanup erase() { return ERASE; }
    private static Cleanup expunge() { return EXPUNGE; }
}
