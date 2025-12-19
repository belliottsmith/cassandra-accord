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

package accord.primitives;

import accord.api.ProgressLog;
import accord.local.CommandSummaries.SummaryStatus;
import accord.messages.BeginRecovery;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasDecidedExecuteAt;
import static accord.api.ProgressLog.BlockedUntil.HasStableDeps;
import static accord.api.ProgressLog.BlockedUntil.NotBlocked;
import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.CommandSummaries.SummaryStatus.COMMITTED;
import static accord.local.CommandSummaries.SummaryStatus.INVALIDATED;
import static accord.local.CommandSummaries.SummaryStatus.NOTACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.NOT_DIRECTLY_WITNESSED;
import static accord.local.CommandSummaries.SummaryStatus.PREACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.STABLE;
import static accord.primitives.Known.PrivilegedVote.NoVote;
import static accord.primitives.Known.Definition.*;
import static accord.primitives.Known.*;
import static accord.primitives.Known.KnownDeps.*;
import static accord.primitives.Known.KnownExecuteAt.*;
import static accord.primitives.Known.KnownRoute.CoveringRoute;
import static accord.primitives.Known.KnownRoute.FullRoute;
import static accord.primitives.Known.KnownRoute.MaybeRoute;
import static accord.primitives.Known.Outcome.*;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Phase.*;

public enum Status
{
    NotDefined        (Phase.None, NOT_DIRECTLY_WITNESSED, Nothing),
    PreAccepted       (PreAccept, PREACCEPTED,                             DefinitionAndRoute),

    AcceptedInvalidate(Accept,    NOTACCEPTED,                             MaybeRoute,             DefinitionUnknown, ExecuteAtUnknown,      DepsUnknown,       Unknown), // may or may not have witnessed

    AcceptedMedium    (Accept,    ACCEPTED,                                CoveringRoute,          DefinitionUnknown, ExecuteAtProposed,     DepsProposedFixed, Unknown), // may or may not have witnessed
    AcceptedSlow      (Accept,    ACCEPTED,                                CoveringRoute,          DefinitionUnknown, ExecuteAtProposed,     DepsProposed,      Unknown), // may or may not have witnessed

    /**
     * PreCommitted is a peculiar state, half-way between Accepted and Committed.
     * We know the transaction is Committed and its execution timestamp, but we do
     * not know its dependencies, and we may still have state leftover from the Accept round
     * that is necessary for recovery.
     *
     * So, for execution of other transactions we may treat a PreCommitted transaction as Committed,
     * using the timestamp to update our dependency set to rule it out as a dependency.
     * But we do not have enough information to execute the transaction, and when recovery calculates
     * {@link BeginRecovery#acceptedStartedBeforeWithoutWitnessing},
     * {@link BeginRecovery#hasCommittedExecutesAfterWithoutWitnessing},
     * and {@link BeginRecovery#committedStartedBeforeAndWitnessed} we may not have the dependencies
     * to calculate the result. For these operations we treat ourselves as whatever Accepted status
     * we may have previously taken, using any proposed dependencies to compute the result.
     *
     * This state exists primarily to permit us to efficiently separate work between different home shards.
     * Take a transaction A that reaches the Committed status and commits to all of its home shard A*'s replicas,
     * but fails to commit to all shards. A takes an execution time later than its TxnId, and in the process
     * adopts a dependency on a transaction B that is coordinated by its home shard B*, that has itself taken
     * a dependency upon A. Importantly, B commits a lower executeAt than A and so will execute first, and once A*
     * commits B, A will remove it from its dependencies. However, there is insufficient information on A*
     * to commit B since it does not know A*'s dependencies, and B* will not process B until A* executes A.
     * To solve this problem we simply permit the executeAt we discover for B to be propagated to A* without
     * its dependencies. Though this does complicate the state machine a little.
     */
    PreCommitted      (Commit,     PREACCEPTED,  FullRoute,  DefinitionUnknown, ExecuteAtKnown,   DepsUnknown,   Unknown),
    Committed         (Commit,     COMMITTED,    FullRoute,  DefinitionKnown,   ExecuteAtKnown,   DepsCommitted, Unknown),
    Stable            (Execute,    STABLE,       FullRoute,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,     Unknown),
    PreApplied        (Persist,    STABLE,       FullRoute,  DefinitionKnown,   ApplyAtKnown,     DepsKnown,     Outcome.Apply),
    Applied           (Persist,    APPLIED,      FullRoute,  DefinitionKnown,   ApplyAtKnown,     DepsKnown,     Outcome.Apply),
    // TODO (required): TruncatedApply should be treated as APPLIED for summary status; when computing recovery decisions
    //  anything already APPLIED should be treated as not witnessing anything being recovered from preaccept status
    //  EXCEPT this cannot apply for touches \notin owns... consider some more how we handle this case
    Truncated         (Cleanup,  null,  MaybeRoute, DefinitionErased,  ExecuteAtErased,  DepsErased,    Outcome.Erased),
    Invalidated       (Invalidate, INVALIDATED,  MaybeRoute, NoOp,              NoExecuteAt,      NoDeps,        Outcome.Abort),
    ;

    /**
     * Represents the phase of a transaction from the perspective of coordination
     * None:       the transaction is not currently being processed by us (it may be known to us, but only transitively)
     * PreAccept:  the transaction is being disseminated and is seeking an execution order
     * Accept:     the transaction did not achieve 1RT consensus and is making durable its execution order
     * Commit:     the transaction's execution time has been durably decided, and dependencies are being disseminated
     * Execute:    the transaction's execution dependencies have been durably disseminated, and the transaction is waiting to execute
     * Persist:    the transaction has executed, and its outcome is being persisted
     * Cleanup:    the transaction has completed, and state used for processing it is being reclaimed
     */
    public enum Phase
    {
        None(false),
        PreAccept(false),
        Accept(true),
        Commit(true),
        Execute(false),
        Persist(false),
        Cleanup(false),
        Invalidate(false)
        ;

        public final boolean tieBreakWithBallot;

        Phase(boolean tieBreakWithBallot)
        {
            this.tieBreakWithBallot = tieBreakWithBallot;
        }
    }

    public static final class Durability
    {
        private static final int MAYBE_INVALIDATED_BIT = 1;
        private static final int SHARDS_SHIFT = 3;
        private static final int PHASE_SHIFT = 1;
        private static final int PHASE_MASK = 0x3;
        public static final int ENCODING_BITS = 6;
        public static final int ENCODING_MASK = (1 << ENCODING_BITS) - 1;
        private static final Durability[] lookup = values();

        public static final Durability NotDurable = get(HasDecision.None, HasOutcome.None, HasOutcome.None, false);
        public static final Durability DurablyCommitted = get(HasDecision.DurablyCommitted, HasOutcome.None, HasOutcome.None, false);
        public static final Durability DurablyStable = get(HasDecision.DurablyStable, HasOutcome.None, HasOutcome.None, false);
        public static final Durability AllQuorums = get(HasDecision.DurablyStable, Quorum, Quorum, false);
        public static final Durability Universal = get(HasDecision.DurablyStable, HasOutcome.Universal, HasOutcome.Universal, false);
        public static final Durability Invalidated = get(HasDecision.None, HasOutcome.None, HasOutcome.None, true);
        public static final Durability QuorumOrInvalidated = get(HasDecision.None, Quorum, Quorum, true);
        public static final Durability UniversalOrInvalidated = get(HasDecision.None, HasOutcome.Universal, HasOutcome.Universal, true);

        public enum HasDecisionOrOutcome
        {
            None('N', "None"),
            FastPathDecided('F', "FastPath"),
            DurablyCommitted('C', "Committed"),
            DurablyStable('S', "Stable"),
            DurablyPreApplied('P', "PreApplied");

            private static final HasDecisionOrOutcome[] lookup = values();
            final char shortName;
            final String mediumName;

            HasDecisionOrOutcome(char shortName, String mediumName)
            {
                this.shortName = shortName;
                this.mediumName = mediumName;
            }

            public static HasDecisionOrOutcome forOrdinal(int ordinal)
            {
                return lookup[ordinal];
            }

            public static int maxEncoded()
            {
                return lookup.length - 1;
            }

            public HasDecision decision()
            {
                return this == DurablyPreApplied ? HasDecision.DurablyStable
                                                 : HasDecision.lookup[ordinal()];
            }

            public HasOutcome outcome()
            {
                return this == DurablyPreApplied ? Quorum : HasOutcome.None;
            }
        }

        public enum HasDecision
        {
            None('N', "None"),
            FastPathDecided('F', "FastPath"),
            DurablyCommitted('C', "Committed"),
            DurablyStable('S', "Stable");

            private static final HasDecision[] lookup = values();
            final char shortName;
            final String mediumName;

            HasDecision(char shortName, String mediumName)
            {
                this.shortName = shortName;
                this.mediumName = mediumName;
            }
        }

        /**
         * Represents the durability of a transaction's Persist phase.
         * NotDurable: the outcome has not been durably recorded
         * Quorum:     the outcome has been durably recorded to a quorum (maybe minority quorum where intersects other quorums) of each participating shard
         * Universal:  the outcome has been durably recorded to every healthy replica
         */
        public enum HasOutcome
        {
            None, Quorum, Universal;
            public final boolean isDurable() { return this != None; }
            public final boolean isUniversal() { return this == Universal; }
            private static final HasOutcome[] lookup = values();
            public static HasOutcome forOrdinal(int ordinal) { return lookup[ordinal]; }
            public static HasOutcome max(HasOutcome a, HasOutcome b) { return a.compareTo(b) >= 0 ? a : b; }
        }

        /**
         * Represents the durability of a transaction's Persist phase.
         * NotDurable: the outcome has not been durably recorded
         * Quorum:     the outcome has been durably recorded to a quorum (maybe minority quorum where intersects other quorums) of each participating shard
         * Universal:  the outcome has been durably recorded to every healthy replica
         */
        public enum HasOutcomeOrInvalidated
        {
            None, QuorumOrInvalidated, Quorum, UniversalOrInvalidated, Universal;
            public final boolean isDurableOrInvalidated() { return this != None; }
            public final boolean isMaybeInvalidated() { return this == QuorumOrInvalidated || this == UniversalOrInvalidated; }
            public final HasOutcomeOrInvalidated mergeMax(HasOutcomeOrInvalidated that) { return max(this, that); }
            private static final HasOutcomeOrInvalidated[] lookup = values();
            public static HasOutcomeOrInvalidated forOrdinal(int ordinal) { return lookup[ordinal]; }
            public static HasOutcomeOrInvalidated max(HasOutcomeOrInvalidated a, HasOutcomeOrInvalidated b) { return a.compareTo(b) >= 0 ? a : b; }
        }

        private static int encode(HasDecision phase, HasOutcome shardOutcome, HasOutcome allShardsOutcome, boolean isMaybeInvalidated)
        {
            return encode(phase.ordinal(), shardOutcome.ordinal(), allShardsOutcome.ordinal(), isMaybeInvalidated ? MAYBE_INVALIDATED_BIT : 0);
        }

        private static int encode(int phaseOrdinal, int shardOutcome, int allShardsOutcome, int maybeInvalidated)
        {
            Invariants.require(allShardsOutcome <= shardOutcome);
            Invariants.require(phaseOrdinal < HasDecision.DurablyCommitted.ordinal() || (maybeInvalidated == 0));
            int outcome = ((1 << shardOutcome) | allShardsOutcome) << SHARDS_SHIFT;
            return (phaseOrdinal << PHASE_SHIFT) | outcome | maybeInvalidated;
        }

        public final boolean isMaybeInvalidated()
        {
            return isMaybeInvalidated(encoded);
        }

        private static boolean isMaybeInvalidated(int encoded)
        {
            return 0 != maybeInvalidated(encoded);
        }

        private static int maybeInvalidated(int encoded)
        {
            return encoded & MAYBE_INVALIDATED_BIT;
        }

        private static int zeroIfInvalidated(int maybeNonZero, int maybeInvalidated)
        {
            return maybeNonZero & (maybeInvalidated - 1);
        }

        private static HasOutcome notInvalidated(int hasOutcomeOrdinal, int encoded)
        {
            return HasOutcome.lookup[zeroIfInvalidated(hasOutcomeOrdinal, maybeInvalidated(encoded))];
        }

        private static int orInvalidatedOrdinal(int hasOutcomeOrdinal, int encoded)
        {
            return (hasOutcomeOrdinal << 1) - maybeInvalidated(encoded);
        }

        private static HasOutcomeOrInvalidated orInvalidated(int hasOutcomeOrdinal, int encoded)
        {
            return HasOutcomeOrInvalidated.lookup[orInvalidatedOrdinal(hasOutcomeOrdinal, encoded)];
        }

        private static int decisionOrdinal(int encoded)
        {
            return (encoded >>> PHASE_SHIFT) & PHASE_MASK;
        }

        private static int maxPhaseOrdinal(int a, int b)
        {
            return Math.max(decisionOrdinal(a), decisionOrdinal(b));
        }

        private static int shardOrdinal(int encoded)
        {
            return 28 - Integer.numberOfLeadingZeros(encoded);
        }

        private static int maxShardOrdinal(int a, int b)
        {
            return shardOrdinal(a | b);
        }

        private static int minShardOrdinal(int a, int b)
        {
            return Math.min(shardOrdinal(a), shardOrdinal(b));
        }

        private static int allShardsOrdinal(int encoded)
        {
            encoded ^= Integer.highestOneBit(encoded);
            return encoded >>> SHARDS_SHIFT;
        }

        private static int maxAllShardsOrdinal(int a, int b)
        {
            a ^= Integer.highestOneBit(a);
            b ^= Integer.highestOneBit(b);
            return Math.max(a, b) >>> SHARDS_SHIFT;
        }

        private static int mergeMaybeInvalidated(int a, int b, int maxPhase)
        {
            int ami = maybeInvalidated(a);
            int bmi = maybeInvalidated(b);
            if (ami == bmi)
                return ami;

            a = zeroIfInvalidated(a, ami);
            b = zeroIfInvalidated(b, bmi);
            return maxPhase >= HasDecision.DurablyCommitted.ordinal() || ((a | b) >>> SHARDS_SHIFT) > 1 ?
                   0 : MAYBE_INVALIDATED_BIT;
        }

        private final int encoded;

        private Durability(int encoded)
        {
            Invariants.require(0 == (encoded & ~0xFF));
            this.encoded = encoded;
        }

        public int encoded()
        {
            return encoded;
        }

        public HasDecisionOrOutcome decisionOrOutcome()
        {
            return decisionOrOutcome(encoded);
        }

        private static HasDecisionOrOutcome decisionOrOutcome(int encoded)
        {
            int ordinal = decisionOrdinal(encoded);
            if (ordinal != HasDecision.DurablyStable.ordinal())
                return HasDecisionOrOutcome.lookup[ordinal];
            return allShards(encoded).compareTo(Quorum) >= 0 ? HasDecisionOrOutcome.DurablyPreApplied : HasDecisionOrOutcome.DurablyStable;
        }

        public HasDecision decision()
        {
            return decision(encoded);
        }

        private static HasDecision decision(int encoded)
        {
            return HasDecision.lookup[decisionOrdinal(encoded)];
        }

        public HasOutcome shard()
        {
            return shard(encoded);
        }

        private static HasOutcome shard(int encoded)
        {
            return HasOutcome.lookup[zeroIfInvalidated(shardOrdinal(encoded), maybeInvalidated(encoded))];
        }

        private static HasOutcome shardUnsafe(int encoded)
        {
            return HasOutcome.lookup[shardOrdinal(encoded)];
        }

        private static HasOutcomeOrInvalidated shardOrInvalidated(int encoded)
        {
            return orInvalidated(shardOrdinal(encoded), encoded);
        }

        public HasOutcome allShards()
        {
            return allShards(encoded);
        }

        private static HasOutcome allShards(int encoded)
        {
            return notInvalidated(allShardsOrdinal(encoded), encoded);
        }

        private static HasOutcome allShardsUnsafe(int encoded)
        {
            return HasOutcome.lookup[allShardsOrdinal(encoded)];
        }

        public HasOutcomeOrInvalidated allShardsOrInvalidated()
        {
            return allShardsOrInvalidated(encoded);
        }

        private static HasOutcomeOrInvalidated allShardsOrInvalidated(int encoded)
        {
            return orInvalidated(allShardsOrdinal(encoded), encoded);
        }

        public SaveStatus durableSaveStatus()
        {
            HasDecisionOrOutcome decisionOrOutcome = decisionOrOutcome();
            switch (decisionOrOutcome)
            {
                default: throw new UnhandledEnum(decisionOrOutcome);
                case None:
                case FastPathDecided:   return SaveStatus.NotDefined;
                case DurablyCommitted:  return SaveStatus.Committed;
                case DurablyStable:     return SaveStatus.Stable;
                case DurablyPreApplied: return SaveStatus.PreApplied;
            }
        }

        public ProgressLog.BlockedUntil durablyUnblocked()
        {
            HasDecisionOrOutcome decisionOrOutcome = decisionOrOutcome();
            switch (decisionOrOutcome)
            {
                default: throw new UnhandledEnum(decisionOrOutcome);
                case None:
                case FastPathDecided:
                    return NotBlocked;

                case DurablyCommitted:
                    // we could report HasCommittedDeps here, but we don't much care to fetch Committed deps
                    // as it doesn't advance the state machine. The BlockedUntil is used for await in recovery.
                    return HasDecidedExecuteAt;

                case DurablyStable:
                    return HasStableDeps;

                case DurablyPreApplied:
                    return CanApply;
            }
        }

        public boolean isDurable()
        {
            return zeroIfInvalidated(allShardsOrdinal(encoded), maybeInvalidated(encoded)) >= Quorum.ordinal();
        }

        public boolean isUniversal()
        {
            return zeroIfInvalidated(allShardsOrdinal(encoded), maybeInvalidated(encoded)) >= HasOutcome.Universal.ordinal();
        }

        public boolean isUniversalOrInvalidated()
        {
            return orInvalidatedOrdinal(allShardsOrdinal(encoded), encoded) >= HasOutcomeOrInvalidated.UniversalOrInvalidated.ordinal();
        }

        public boolean isDurableOrInvalidated()
        {
            return orInvalidatedOrdinal(allShardsOrdinal(encoded), encoded) >= HasOutcomeOrInvalidated.QuorumOrInvalidated.ordinal();
        }

        public boolean isFastPathDurablyDecided()
        {
            return decisionOrdinal(encoded) >= HasDecision.FastPathDecided.ordinal();
        }

        public boolean isDurablyCommitted()
        {
            return decisionOrdinal(encoded) >= HasDecision.DurablyCommitted.ordinal();
        }

        public boolean isDurablyStable()
        {
            return decisionOrdinal(encoded) >= HasDecision.DurablyStable.ordinal();
        }

        @Override
        public String toString()
        {
            return toString(encoded);
        }

        private static String toString(int encoded)
        {
            HasDecision decision = decision(encoded);
            HasOutcomeOrInvalidated shard = shardOrInvalidated(encoded);
            HasOutcomeOrInvalidated allShards = allShardsOrInvalidated(encoded);
            return decision.mediumName + '/' + shard.name() + '/' + allShards.name();
        }

        public static Durability nonNullOrMergeMax(@Nullable Durability a, @Nullable Durability b)
        {
            if (a == null) return b;
            if (b == null) return a;
            return a.mergeMax(b);
        }

        public final boolean isAtLeast(Durability that)
        {
            int phase = decisionOrdinal(this.encoded);
            return phase >= decisionOrdinal(that.encoded)
                   && shardOrdinal(encoded) >= shardOrdinal(that.encoded)
                   && allShardsOrdinal(encoded) >= allShardsOrdinal(that.encoded)
                   && mergeMaybeInvalidated(encoded, that.encoded, phase) == maybeInvalidated(encoded);
        }

        // max of each ordinal, and NOT_INVALIDATED if set on either
        public Durability mergeMax(Durability that)
        {
            int phase = maxPhaseOrdinal(this.encoded, that.encoded);
            int shard = maxShardOrdinal(this.encoded, that.encoded);
            int allShards = maxAllShardsOrdinal(this.encoded, that.encoded);
            int maybeInvalidated = mergeMaybeInvalidated(this.encoded, that.encoded, phase);
            return selfOrLookup(encode(phase, shard, allShards, maybeInvalidated));
        }

        public Durability mergeShardsOrReplicas(Durability that)
        {
            int phase = maxPhaseOrdinal(this.encoded, that.encoded);
            int allShards = maxAllShardsOrdinal(this.encoded, that.encoded);
            int shard = Math.max(allShards, minShardOrdinal(this.encoded, that.encoded));
            int maybeInvalidated = mergeMaybeInvalidated(this.encoded, that.encoded, phase);
            return selfOrLookup(encode(phase, shard, allShards, maybeInvalidated));
        }

        private Durability selfOrLookup(int encoded)
        {
            if (encoded == this.encoded)
                return this;
            return forEncoded(encoded);
        }

        public static Durability get(HasDecision decision, HasOutcome shardOutcome, HasOutcome allShardsOutcome, boolean isMaybeInvalidated)
        {
            return forEncoded(encode(decision, shardOutcome, allShardsOutcome, isMaybeInvalidated));
        }

        public static Durability forEncoded(int encoded)
        {
            if (encoded < 0 || encoded > lookup.length)
                throw new IndexOutOfBoundsException(encoded);
            Durability durability = lookup[encoded];
            Invariants.require(durability != null, "Invalid durability requested %d", encoded);
            return durability;
        }

        private static Durability[] values()
        {
            Invariants.require(HasOutcome.lookup.length <= 4);
            Durability[] result = new Durability[64];
            for (HasDecision phase : HasDecision.lookup)
            {
                for (HasOutcome shard : HasOutcome.lookup)
                {
                    for (HasOutcome allShards : HasOutcome.lookup)
                    {
                        if (allShards.ordinal() > shard.ordinal())
                            break;

                        for (boolean isMaybeInvalidated : new boolean[] { false, true})
                        {
                            if (isMaybeInvalidated && phase.compareTo(HasDecision.DurablyCommitted) >= 0)
                                continue;

                            int encoded = encode(phase, shard, allShards, isMaybeInvalidated);
                            result[encoded] = new Durability(encoded);
                        }
                    }
                }
            }
            return result;
        }
    }

    public final Phase phase;
    public final SummaryStatus summary;
    public final Known minKnown;

    Status(Phase phase, SummaryStatus summary, Known minKnown)
    {
        this.phase = phase;
        this.summary = summary;
        this.minKnown = minKnown;
    }

    Status(Phase phase, SummaryStatus summary, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this.phase = phase;
        this.summary = summary;
        this.minKnown = new Known(route, definition, executeAt, deps, outcome, NoVote);
    }

    // TODO (desired, clarity): investigate all uses of hasBeen, and migrate as many as possible to testing
    //                          Phase, ReplicationPhase and ExecutionStatus where these concepts are inadequate,
    //                          see if additional concepts can be introduced
    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }

    public static <T> T max(Collection<T> list, Function<T, Status> getStatus, Function<T, Ballot> getAcceptedOrCommittedBallot, Predicate<T> filter)
    {
        T max = null;
        Status maxStatus = null;
        Ballot maxBallot = null;
        for (T item : list)
        {
            if (!filter.test(item))
                continue;

            Status status = getStatus.apply(item);
            Ballot ballot = getAcceptedOrCommittedBallot.apply(item);
            if (max == null || isGreater(status, ballot, maxStatus, maxBallot))
            {
                max = item;
                maxStatus = status;
                maxBallot = ballot;
            }
        }

        return max;
    }

    private static boolean isGreater(Status testStatus, Ballot testBallot, Status thanStatus, Ballot thanBallot)
    {
        Phase phase = testStatus.phase;
        int c = phase.compareTo(thanStatus.phase);
        if (c != 0)
            return c > 0;

        if (phase.tieBreakWithBallot)
        {
            c = testBallot.compareTo(thanBallot);
            if (c != 0)
                return c > 0;
        }

        return testStatus.compareTo(thanStatus) > 0;
    }

    public static <T> T max(T a, Status statusA, Ballot ballotA, T b, Status statusB, Ballot ballotB)
    {
        int c = statusA.phase.compareTo(statusB.phase);
        if (c > 0) return a;
        if (c < 0) return b;
        if ((statusA.phase.tieBreakWithBallot ? ballotA.compareTo(ballotB) : statusA.compareTo(statusB)) >= 0)
            return a;
        return b;
    }
}
