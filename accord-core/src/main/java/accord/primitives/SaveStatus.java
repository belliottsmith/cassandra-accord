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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Known.PrivilegedVote;
import accord.primitives.Known.Definition;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.Known.KnownRoute;
import accord.primitives.Known.Outcome;
import accord.primitives.Status.Phase;

import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.primitives.Known.PrivilegedVote.NoVote;
import static accord.primitives.Known.PrivilegedVote.VotePreAccept;
import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsProposedFixed;
import static accord.primitives.Known.Outcome.Apply;
import static accord.primitives.Known.Outcome.WasApply;
import static accord.primitives.SaveStatus.LocalExecution.CleaningUp;
import static accord.primitives.SaveStatus.LocalExecution.NotReady;
import static accord.primitives.Known.Definition.DefinitionErased;
import static accord.primitives.Known.Definition.DefinitionKnown;
import static accord.primitives.Known.Definition.DefinitionUnknown;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsProposed;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtErased;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtProposed;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtUnknown;
import static accord.primitives.Known.KnownRoute.FullRoute;
import static accord.primitives.Known.KnownRoute.MaybeRoute;
import static accord.primitives.Known.Outcome.Unknown;
import static accord.primitives.SaveStatus.LocalExecution.ReadyToExclude;
import static accord.primitives.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.primitives.Status.Truncated;

/**
 * A version of Status that preserves additional local state, including whether we have previously been PreAccepted
 * and therefore know the definition of the transaction, and what knowledge remains post-truncation.
 *
 * This would potentially complicate users of Status, and the distributed state machine is complicated
 * enough. But it helps to formalise the relationships here as an auxiliary enum.
 * Intended to be used internally by Command implementations.
 */
public enum SaveStatus
{
    Uninitialised                   (Status.NotDefined),
    NotDefined                      (Status.NotDefined),
    PreAccepted                     (Status.PreAccepted),
    PreAcceptedWithVote             (Status.PreAccepted,                                                                                                    VotePreAccept),
    PreAcceptedWithDeps             (Status.PreAccepted,            FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsFromCoordinator,    Unknown,    VotePreAccept),

    // note: this status means we have durably cleared any Accepted that may have been inflight, but we must retain DepsFromCoordinator for deciding if a fast path decision may have been taken
    // note: AcceptedInvalidate(s), PreNotAccepted(s) and NotAccepted(s) clear any proposed Deps.
    // This means voters recovering an earlier transaction will not consider the record when excluding the possibility of another transaction's fast-path commit.
    // This is safe, because any Accept that may override the Pre/NotAccepted will construct new Deps that must now witness the recovering transaction.

    PreNotAccepted                  (Status.PreNotAccepted),
    PreNotAcceptedWithDefinition    (Status.PreNotAccepted,         FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsUnknown,        Unknown),
    PreNotAcceptedWithDefAndVote    (Status.PreNotAccepted,         FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsUnknown,        Unknown,        VotePreAccept),
    PreNotAcceptedWithDefAndDeps    (Status.PreNotAccepted,         FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsFromCoordinator,Unknown,        VotePreAccept),
    NotAccepted                     (Status.NotAccepted),
    NotAcceptedWithDefinition       (Status.NotAccepted,            FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsUnknown,        Unknown),
    NotAcceptedWithDefAndVote       (Status.NotAccepted,            FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsUnknown,        Unknown,        VotePreAccept),
    NotAcceptedWithDefAndDeps       (Status.NotAccepted,            FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsFromCoordinator,Unknown,        VotePreAccept),
    AcceptedInvalidate              (Status.AcceptedInvalidate),
    AcceptedInvalidateWithDefinition(Status.AcceptedInvalidate,     FullRoute,  DefinitionKnown,    ExecuteAtUnknown,   DepsUnknown,        Unknown),

    AcceptedMedium                  (Status.AcceptedMedium),
    AcceptedMediumWithDefinition    (Status.AcceptedMedium,         FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposedFixed,  Unknown),
    AcceptedMediumWithDefAndVote    (Status.AcceptedMedium,         FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposedFixed,  Unknown,        VotePreAccept),
    AcceptedSlow                    (Status.AcceptedSlow),
    AcceptedSlowWithDefinition      (Status.AcceptedSlow,           FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposed,       Unknown),
    AcceptedSlowWithDefAndVote      (Status.AcceptedSlow,           FullRoute,  DefinitionKnown,    ExecuteAtProposed,  DepsProposed,       Unknown,        VotePreAccept),
    PreCommitted                    (Status.PreCommitted,                                                                                            ReadyToExclude),
    PreCommittedWithDefinition      (Status.PreCommitted,           FullRoute,  DefinitionKnown,    ExecuteAtKnown,     DepsUnknown,        Unknown, ReadyToExclude),
    PreCommittedWithDeps            (Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionUnknown,  ExecuteAtKnown,     DepsProposed,       Unknown, ReadyToExclude),
    PreCommittedWithFixedDeps       (Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionUnknown,  ExecuteAtKnown,     DepsProposedFixed,  Unknown, ReadyToExclude),
    PreCommittedWithDefAndDeps      (Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionKnown,    ExecuteAtKnown,     DepsProposed,       Unknown, ReadyToExclude),
    PreCommittedWithDefAndFixedDeps (Status.PreCommitted, ACCEPTED, FullRoute,  DefinitionKnown,    ExecuteAtKnown,     DepsProposedFixed,  Unknown, ReadyToExclude),

    Committed                       (Status.Committed,                                                                                               ReadyToExclude),
    Stable                          (Status.Stable,                                                                                                  WaitingToExecute),
    ReadyToExecute                  (Status.Stable,                                                                                                  LocalExecution.ReadyToExecute),

    PreApplied                      (Status.PreApplied,                                                                                              LocalExecution.WaitingToApply),
    Applying                        (Status.PreApplied,                                                                                              LocalExecution.Applying),
    // similar to Truncated, but doesn't imply we have any global knowledge about application
    Applied                         (Status.Applied,                                                                                                 LocalExecution.Applied),
    // TruncatedApplyWithDeps is a state never adopted within a single replica; it is however a useful state we may enter by combining state from multiple replicas
    // TODO (expected): TruncatedApplyWithDeps should be redundant now we have migrated away from SaveStatus in CheckStatusOk to Known; remove in isolated commit once stable
    //   however: we may want to retain it for the case where we want to truncate its payload but need to be able to answer recovery decisions
    TruncatedApplyWithDeps          (Status.Truncated,              FullRoute,  DefinitionErased,   ExecuteAtKnown,     DepsKnown,          Apply,   CleaningUp),
    TruncatedApplyWithOutcome       (Status.Truncated,              FullRoute,  DefinitionErased,   ExecuteAtKnown,     DepsErased,         Apply,   CleaningUp),
    TruncatedApply                  (Status.Truncated,              FullRoute,  DefinitionErased,   ExecuteAtKnown,     DepsErased,         WasApply,CleaningUp),
    // NOTE: Erased should ONLY be adopted on a replica that knows EVERY shard has successfully applied the transaction at all healthy replicas (or else that it is durably invalidated)
    Erased                          (Status.Truncated,              MaybeRoute, DefinitionErased,   ExecuteAtErased,    DepsErased,         Outcome.Erased,CleaningUp),
    // ErasedOrVestigial means the command cannot be completed and is either pre-bootstrap, did not commit, or did not participate in this shard's epoch
    ErasedOrVestigial               (Status.Truncated,              MaybeRoute, DefinitionUnknown,  ExecuteAtUnknown,   DepsUnknown,        Unknown, CleaningUp),
    Invalidated                     (Status.Invalidated,                                                                                             CleaningUp),
    ;

    /**
     * Note that this is a LOCAL concept ONLY, and should not be used to infer anything remotely.
     */
    public enum LocalExecution
    {
        /**
         * Still coordinating a decision
         */
        NotReady,

        /**
         * Ready to exclude based on the decided executeAt, but the dependencies are not known.
         */
        ReadyToExclude,

        /**
         * A complete execution decision has been made, but the dependencies have not executed
         */
        WaitingToExecute,

        /**
         * The command is ready to execute, and a coordinator should promptly compute and distribute the command's outcome
         */
        ReadyToExecute,

        /**
         * The command has been executed and its outcome is known, but we have not locally executed all of its dependencies
         * TODO (expected): we should only await this when we have no local execution dependencies, or we know that
         *   the command is durable remotely
         */
        WaitingToApply,

        /**
         * The command is being asynchronously applied to the local data store
         */
        Applying,

        /**
         * The command has been applied to the local data store
         */
        Applied,

        /**
         * Some or all of the command's local state has been garbage collected
         */
        CleaningUp
    }

    private static final SaveStatus[] lookup = values();

    public final Status status;
    public final Phase phase;
    public final SummaryStatus summary;
    public final Known known;
    public final LocalExecution execution;

    SaveStatus(Status status)
    {
        this(status, status.phase);
    }

    SaveStatus(Status status, PrivilegedVote privilegedVote)
    {
        this(status, status.summary, status.phase, status.minKnown.with(privilegedVote), NotReady);
    }

    SaveStatus(Status status, LocalExecution execution)
    {
        this(status, status.phase, execution);
    }

    SaveStatus(Status status, Phase phase)
    {
        this(status, phase, NotReady);
    }

    SaveStatus(Status status, Phase phase, LocalExecution execution)
    {
        this(status, phase, status.minKnown, execution);
    }

    SaveStatus(Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
    {
        this(status, route, definition, executeAt, deps, outcome, NotReady);
    }

    SaveStatus(Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, PrivilegedVote privilegedVote)
    {
        this(status, status.summary, status.phase, new Known(route, definition, executeAt, deps, outcome, privilegedVote), NotReady);
    }

    SaveStatus(Status status, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, LocalExecution execution)
    {
        this(status, status.summary, route, definition, executeAt, deps, outcome, execution);
    }

    SaveStatus(Status status, SummaryStatus summaryStatus, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome, LocalExecution execution)
    {
        this(status, summaryStatus, status.phase, new Known(route, definition, executeAt, deps, outcome, NoVote), execution);
    }

    SaveStatus(Status status, Phase phase, Known known, LocalExecution execution)
    {
        this(status, status.summary, phase, known, execution);
    }

    SaveStatus(Status status, SummaryStatus summaryStatus, Phase phase, Known known, LocalExecution execution)
    {
        this.status = status;
        this.summary = summaryStatus;
        this.phase = phase;
        this.known = known;
        this.execution = execution;
    }

    public boolean is(Status status)
    {
        return this.status.equals(status);
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public boolean isUninitialised()
    {
        return compareTo(Uninitialised) <= 0;
    }

    public boolean isComplete()
    {
        switch (this)
        {
            case Applied:
            case Invalidated:
                return true;
            default:
                return false;
        }
    }

    // TODO (expected): merge Known only, and ensure 1:1 mapping so can reconstruct composite
    // TODO (expected, testing): exhaustive testing, particularly around PreCommitted
    public static SaveStatus get(Status status, Known known)
    {
        if (known.isInvalidated())
            return Invalidated;

        if (status.compareTo(Status.PreCommitted) < 0)
        {
            if (known.executeAt().isDecidedAndKnownToExecute())
            {
                switch (status)
                {
                    case NotDefined: return PreCommitted;
                    case PreAccepted: return PreCommittedWithDefinition;
                    case PreNotAccepted:
                    case NotAccepted:
                    case AcceptedMedium:
                    case AcceptedSlow:
                    case AcceptedInvalidate:
                        known = known.with(DepsUnknown);
                        // if the decision is known, we're really PreCommitted
                }
            }
            else
            {
                switch (status)
                {
                    case NotDefined: return NotDefined;
                    case PreAccepted:
                        return known.is(NoVote) ? PreAccepted : known.is(DepsUnknown) ? PreAcceptedWithVote : PreAcceptedWithDeps;
                    case PreNotAccepted:
                        if (!known.isDefinitionKnown())
                            return PreNotAccepted;
                        return known.is(NoVote) ? PreNotAcceptedWithDefinition : known.is(DepsUnknown) ? PreNotAcceptedWithDefAndVote : PreNotAcceptedWithDefAndDeps;
                    case NotAccepted:
                        if (!known.isDefinitionKnown()) return NotAccepted;
                        if (!known.hasPrivilegedVote()) return NotAcceptedWithDefinition;
                        return known.is(DepsUnknown) ? NotAcceptedWithDefAndVote : NotAcceptedWithDefAndDeps;
                    case AcceptedMedium:
                        if (!known.isDefinitionKnown()) return AcceptedMedium;
                        if (!known.hasPrivilegedVote()) return AcceptedMediumWithDefinition;
                        return AcceptedMediumWithDefAndVote;
                    case AcceptedSlow:
                        if (!known.isDefinitionKnown()) return AcceptedSlow;
                        if (!known.hasPrivilegedVote()) return AcceptedSlowWithDefinition;
                        return AcceptedSlowWithDefAndVote;
                    case AcceptedInvalidate:
                        return known.isDefinitionKnown() ? AcceptedInvalidateWithDefinition : AcceptedInvalidate;
                        // if the decision is known, we're really PreCommitted
                }
            }
        }

        switch (status)
        {
            default: throw new AssertionError("Unexpected status: " + status);
            case PreNotAccepted:
            case NotAccepted:
            case AcceptedMedium:
            case AcceptedSlow:
            case AcceptedInvalidate:
            case PreCommitted:
                if (!known.isDefinitionKnown())
                {
                    switch (known.deps())
                    {
                        case DepsProposedFixed: return PreCommittedWithFixedDeps;
                        case DepsProposed:      return PreCommittedWithDeps;
                        default:                return PreCommitted;
                    }
                }
                switch (known.deps())
                {
                    default:                    throw new AssertionError("Unexpected DepsKnown: " + known.deps());
                    case DepsErased:
                    case DepsUnknown:
                    case DepsFromCoordinator:   return PreCommittedWithDefinition; // TODO (required): consider if this is correct
                    case DepsProposedFixed:     return PreCommittedWithDefAndFixedDeps;
                    case DepsProposed:          return PreCommittedWithDefAndDeps;
                    case DepsKnown:
                    case DepsCommitted:
                }
            case Committed: return known.deps() == DepsKnown ? Stable : Committed;
            case Stable: return Stable;
            case PreApplied: return PreApplied;
            case Applied: return Applied;
            case Invalidated: return Invalidated;
        }
    }

    private static final Known DefinitionOnly = new Known(FullRoute, DefinitionKnown, ExecuteAtUnknown, DepsUnknown, Unknown, NoVote);
    public static SaveStatus withDefinition(SaveStatus status)
    {
        return enrich(status, DefinitionOnly);
    }

    public static SaveStatus enrich(SaveStatus status, Known known)
    {
        // most statuses already know everything they can
        switch (status.status)
        {
            case NotDefined:
            case PreAccepted:
            case AcceptedInvalidate:
            case PreNotAccepted:
            case NotAccepted:
            case AcceptedMedium:
            case AcceptedSlow:
            case PreCommitted:
            case Committed:
                if (known.isSatisfiedBy(status.known))
                    return status;
                return get(status.status, status.known.atLeast(known));

            case Stable:
                return status;

            case Truncated:
                switch (status)
                {
                    default: throw new AssertionError("Unexpected status: " + status);
                    case ErasedOrVestigial:
                        if (known.outcome().isInvalidated())
                            return Invalidated;

                        if (!known.outcome().isOrWasApply() || known.is(ExecuteAtKnown))
                            return ErasedOrVestigial;

                    case Erased:
                        if (!known.outcome().isOrWasApply() || !known.is(ExecuteAtKnown))
                            return Erased;

                    case TruncatedApply:
                        if (known.outcome() != Apply)
                            return TruncatedApply;

                    case TruncatedApplyWithOutcome:
                        if (known.deps() != DepsKnown)
                            return TruncatedApplyWithOutcome;

                    case TruncatedApplyWithDeps:
                        if (!known.isDefinitionKnown())
                            return TruncatedApplyWithDeps;

                        return Applied;
                }
        }

        return status;
    }

    // TODO (expected): tighten up distinction of "preferKnowledge" and its interaction with CheckStatus
    public static SaveStatus merge(SaveStatus a, Ballot ballotA, SaveStatus b, Ballot ballotB, boolean preferKnowledge)
    {
        // we first enrich cleanups with the knowledge of the other, to avoid counter-intuitive situations where
        // we might be able to convert a TruncatedWithApply into Applied, but instead select something much earlier
        // such as ReadyToExecute; we then apply the normal max process to the results
        if (a.phase == Phase.Cleanup) a = enrich(a, b.known);
        if (b.phase == Phase.Cleanup) b = enrich(b, a.known);
        SaveStatus result = max(a, a, ballotA, b, b, ballotB, preferKnowledge);
        return enrich(result, (result == a ? b : a).known);
    }

    public static <T> T max(T av, SaveStatus a, Ballot ballotA, T bv, SaveStatus b, Ballot ballotB, boolean preferKnowledge)
    {
        if (a == b)
            return av;

        if (a.phase != b.phase)
        {
            return a.phase.compareTo(b.phase) > 0
                   ? (preferKnowledge && a.phase == Phase.Cleanup ? bv : av)
                   : (preferKnowledge && b.phase == Phase.Cleanup ? av : bv);
        }

        if (a.phase.tieBreakWithBallot)
            return ballotA.compareTo(ballotB) >= 0 ? av : bv;

        if (preferKnowledge && a.lowerHasMoreKnowledge(b))
            return a.compareTo(b) <= 0 ? av : bv;
        return a.compareTo(b) >= 0 ? av : bv;
    }

    public static <T> T max(List<T> list, Function<T, SaveStatus> getStatus, Function<T, Ballot> getAcceptedOrCommittedBallot, Predicate<T> filter, boolean preferKnowledge)
    {
        T max = null;
        SaveStatus maxStatus = null;
        Ballot maxBallot = null;
        for (T item : list)
        {
            if (!filter.test(item))
                continue;

            SaveStatus status = getStatus.apply(item);
            Ballot ballot = getAcceptedOrCommittedBallot.apply(item);
            boolean update = max == null
                             || maxStatus.phase.compareTo(status.phase) < 0
                             || (maxStatus.phase.tieBreakWithBallot ? maxStatus.phase == status.phase && maxBallot.compareTo(ballot) < 0
                                                                    : maxStatus.compareTo(status) < 0);

            if (!update)
                continue;

            max = item;
            maxStatus = status;
            maxBallot = ballot;
        }

        return max;
    }

    // TODO (desired): this isn't a simple linear relationship - Committed has some more knowledge, but some less; PreAccepted has much less
    public boolean lowerHasMoreKnowledge(SaveStatus than)
    {
        if (this.is(Truncated) && !than.is(Status.NotDefined))
            return true;

        if (than.is(Truncated) && !this.is(Status.NotDefined))
            return true;

        return false;
    }

    public static SaveStatus forOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal > lookup.length)
            throw new IndexOutOfBoundsException(ordinal);
        return lookup[ordinal];
    }
}
