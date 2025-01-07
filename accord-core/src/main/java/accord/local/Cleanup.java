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
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.RedundantStatus.NOT_OWNED;
import static accord.local.RedundantStatus.TRUNCATE_BEFORE;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.SaveStatus.Vestigial;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Invariants.illegalState;

/**
 * Logic related to whether metadata about transactions is safe to discard given currently available information.
 * The data may not be completely discarded if parts of it will still be necessary.
 */
public enum Cleanup
{
    NO(Uninitialised),
    // we don't know if the command has been applied or invalidated as we have incomplete information
    // so erase what information we don't need in future to decide this
    // TODO (required): tighten up semantics here (and maybe infer more aggressively)
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
     * </p>
     *
     */
    private static Cleanup shouldCleanupInternal(Input input, Agent agent, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.kind() == EphemeralRead)
            return NO;

        if (expunge(txnId, executeAt, saveStatus, participants, redundantBefore, durableBefore))
            return expunge();

        if (participants == null)
            return NO;

        if (!participants.hasFullRoute())
            return cleanupIfUndecided(input, txnId, saveStatus, participants, redundantBefore);

        return cleanupWithFullRoute(input, agent, participants, txnId, executeAt, saveStatus, durability, redundantBefore, durableBefore);
    }

    private static Cleanup cleanupIfUndecided(Input input, TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, RedundantBefore redundantBefore)
    {
        // TODO (expected): consider if we can truncate more aggressively partial records, although we cannot infer anything from the fact they're undecided
        if (input == PARTIAL || saveStatus.hasBeen(PreCommitted))
            return NO;

        if (participants.owns().isEmpty())
            return txnId.compareTo(redundantBefore.minShardRedundantBefore()) < 0 ? vestigial() : NO;

        RedundantStatus redundant = redundantBefore.status(txnId, null, participants.owns());
        switch (redundant)
        {
            default: throw new UnhandledEnum(redundant);
            case WAS_OWNED:
            case WAS_OWNED_CLOSED:
            case LIVE:
            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
            case PRE_BOOTSTRAP_OR_STALE:
            case LOCALLY_REDUNDANT:
            case PARTIALLY_LOCALLY_REDUNDANT:
            case PARTIALLY_SHARD_REDUNDANT:
                return NO;

            case WAS_OWNED_PARTIALLY_RETIRED:
            case WAS_OWNED_RETIRED:
            case SHARD_REDUNDANT_AND_PRE_BOOTSTRAP_OR_STALE:
                return vestigial();

            case SHARD_REDUNDANT:
            case PARTIALLY_SHARD_FULLY_LOCALLY_REDUNDANT:
            case TRUNCATE_BEFORE:
            case GC_BEFORE:
                // correctness here relies on the fact that we process expunges first via global properties
                // so, if we reach here we would expect any Erase/Truncate to still exist and this path would not be taken
                return invalidate();
        }
    }

    private static Cleanup cleanupWithFullRoute(Input input, Agent agent, StoreParticipants participants, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // We first check if the command is redundant locally, i.e. whether it has been applied to all non-faulty replicas of the local shard
        // If not, we don't want to truncate its state else we may make catching up for these other replicas much harder
        FullRoute<?> route = Route.castToFullRoute(participants.route());
        if (!saveStatus.known.is(ApplyAtKnown)) executeAt = null;
        RedundantStatus redundant = redundantBefore.status(txnId, executeAt, route);
        if (redundant == NOT_OWNED)
            illegalState("Command " + txnId + " that is being loaded is not owned by this shard on route " + route);

        switch (redundant)
        {
            default: throw new UnhandledEnum(redundant);
            case WAS_OWNED:
            case WAS_OWNED_CLOSED:
            case LIVE:
            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
            case PRE_BOOTSTRAP_OR_STALE:
            case LOCALLY_REDUNDANT:
            case PARTIALLY_LOCALLY_REDUNDANT:
            case PARTIALLY_SHARD_REDUNDANT:
                return NO;

            case WAS_OWNED_PARTIALLY_RETIRED:
                // all keys are no longer owned, and at least one key is locally redundant
                if (txnId.is(ExclusiveSyncPoint))
                    return NO;

            case WAS_OWNED_RETIRED:
                return vestigial();

            case SHARD_REDUNDANT_AND_PRE_BOOTSTRAP_OR_STALE:
                return truncate(saveStatus);

            case SHARD_REDUNDANT:
            case PARTIALLY_SHARD_FULLY_LOCALLY_REDUNDANT:
                if (!saveStatus.hasBeen(PreCommitted))
                    return input == FULL ? invalidate() : NO;
                return NO;

            case GC_BEFORE:
            case TRUNCATE_BEFORE:
                if (!saveStatus.hasBeen(PreCommitted))
                    return input == FULL ? invalidate() : NO;

                if (input == FULL)
                {
                    Participants<?> executes = participants.stillExecutes();
                    if (!saveStatus.hasBeen(Applied) && (executes == null || (!executes.isEmpty() && redundantBefore.preBootstrapOrStale(txnId, executes) != FULLY)))
                    {
                        // if we should execute this transaction locally, and we have not done so by the time we reach a GC point, something has gone wrong
                        agent.onViolation(String.format("Loading %s command %s with status %s (that should have been Applied). Expected to be witnessed and executed by %s.", redundant, txnId, saveStatus, redundantBefore.max(participants.route(), e -> e.shardAppliedOrInvalidatedBefore)));
                        return truncate(saveStatus);
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
                        return truncate();

                    case UniversalOrInvalidated:
                    case Universal:
                        if (redundant == TRUNCATE_BEFORE)
                            return truncate();
                        return erase();
                }
        }
    }

    private static boolean expunge(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // since we cannot guarantee to witness participants for all records, we must use the global durableBefore bounds
        // TODO (expected): introduce a single-key flag, as this would facilitate faster cleanup of common transactions
        if (durableBefore.min(txnId) != UniversalOrInvalidated)
            return false;

        // TODO (desired): we should perhaps weaken this to separately account whether remotely and locally redundant?
        //  i.e., if we know that the shard is remotely durable and we know we don't need it locally (e.g. due to bootstrap)
        //  then we can safely erase. Revisit as part of rationalising RedundantBefore registers.

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
    private static Cleanup invalidate() { return INVALIDATE; }
    private static Cleanup truncate(SaveStatus saveStatus)
    {
        return saveStatus.known.is(ApplyAtKnown) ? truncate() : vestigial();
    }
    private static Cleanup truncateWithOutcome() { return TRUNCATE_WITH_OUTCOME; }
    private static Cleanup truncate() { return TRUNCATE; }
    private static Cleanup vestigial() { return VESTIGIAL; }
    private static Cleanup erase() { return ERASE; }
    private static Cleanup expunge() { return EXPUNGE; }
}
