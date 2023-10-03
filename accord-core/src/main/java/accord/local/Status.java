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

import accord.messages.BeginRecovery;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import static accord.local.Status.Definition.*;
import static accord.local.Status.Known.*;
import static accord.local.Status.KnownDeps.*;
import static accord.local.Status.KnownExecuteAt.*;
import static accord.local.Status.KnownRoute.Covering;
import static accord.local.Status.KnownRoute.Full;
import static accord.local.Status.KnownRoute.Maybe;
import static accord.local.Status.Outcome.*;
import static accord.local.Status.Phase.*;

public enum Status
{
    NotDefined        (None,      Nothing),
    PreAccepted       (PreAccept, DefinitionAndRoute),
    AcceptedInvalidate(Accept,    Maybe,          DefinitionUnknown, ExecuteAtUnknown,  DepsUnknown,  Unknown), // may or may not have witnessed
    Accepted          (Accept,    Covering,       DefinitionUnknown, ExecuteAtProposed, DepsProposed, Unknown), // may or may not have witnessed

    /**
     * PreCommitted is a peculiar state, half-way between Accepted and Committed.
     * We know the transaction is Committed and its execution timestamp, but we do
     * not know its dependencies, and we may still have state leftover from the Accept round
     * that is necessary for recovery.
     *
     * So, for execution of other transactions we may treat a PreCommitted transaction as Committed,
     * using the timestamp to update our dependency set to rule it out as a dependency.
     * But we do not have enough information to execute the transaction, and when recovery calculates
     * {@link BeginRecovery#acceptedStartedBeforeWithoutWitnessing}, {@link BeginRecovery#hasCommittedExecutesAfterWithoutWitnessing}
     *
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
    PreCommitted      (Accept,  Maybe, DefinitionUnknown, ExecuteAtKnown,   DepsUnknown, Unknown),

    Committed         (Commit,  Full,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Unknown),
    // TODO (expected): do we need ReadyToExecute here, or can we keep it to SaveStatus only?
    ReadyToExecute    (Commit,  Full,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Unknown),
    // TODO (expected): do we need both PreApplied and Applied here, or can we keep them to SaveStatus only?
    PreApplied        (Persist, Full,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Outcome.Apply),
    Applied           (Persist, Full,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Outcome.Apply),
    Truncated         (Cleanup, Nothing),
    Invalidated       (Persist, Maybe, NoOp,              NoExecuteAt,      NoDeps,      Outcome.Invalidated),
    ;

    /**
     * Represents the phase of a transaction from the perspective of coordination
     * None:       the transaction is not currently being processed by us (it may be known to us, but only transitively)
     * PreAccept:  the transaction is being disseminated and is seeking an execution order
     * Accept:     the transaction did not achieve 1RT consensus and is making durable its execution order
     * Commit:     the transaction's execution order has been durably decided, and is being disseminated
     * Persist:    the transaction has executed, and its outcome is being persisted
     * Cleanup:    the transaction has completed, and state used for processing it is being reclaimed
     */
    public enum Phase
    {
        None, PreAccept, Accept, Commit, Persist, Cleanup
    }

    /**
     * A vector of various facets of knowledge about, or required for, processing a transaction.
     * Each property is treated independently, there is no precedence relationship between them.
     * Each property's values are however ordered with respect to each other.
     */
    public static class Known
    {
        public static final Known Nothing            = new Known(Maybe, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Unknown);
        // TODO (expected): deprecate DefinitionOnly
        public static final Known DefinitionOnly     = new Known(Maybe, DefinitionKnown,   ExecuteAtUnknown, DepsUnknown, Unknown);
        public static final Known DefinitionAndRoute = new Known(Full,  DefinitionKnown,   ExecuteAtUnknown, DepsUnknown, Unknown);
        public static final Known ExecuteAtOnly      = new Known(Maybe, DefinitionUnknown, ExecuteAtKnown,   DepsUnknown, Unknown);
        public static final Known Decision           = new Known(Full,  DefinitionKnown,   ExecuteAtKnown,   DepsKnown,   Unknown);
        public static final Known Apply              = new Known(Full,  DefinitionUnknown, ExecuteAtKnown,   DepsKnown,   Outcome.Apply);
        public static final Known Invalidated        = new Known(Maybe, DefinitionUnknown, ExecuteAtUnknown, DepsUnknown, Outcome.Invalidated);

        public final KnownRoute route;
        public final Definition definition;
        public final KnownExecuteAt executeAt;
        public final KnownDeps deps;
        public final Outcome outcome;

        public Known(Known copy)
        {
            this.route = copy.route;
            this.definition = copy.definition;
            this.executeAt = copy.executeAt;
            this.deps = copy.deps;
            this.outcome = copy.outcome;
        }

        public Known(KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Outcome outcome)
        {
            this.route = route;
            this.definition = definition;
            this.executeAt = executeAt;
            this.deps = deps;
            this.outcome = outcome;
        }

        public Known atLeast(Known with)
        {
            KnownRoute maxRoute = route.atLeast(with.route);
            Definition maxDefinition = definition.compareTo(with.definition) >= 0 ? definition : with.definition;
            KnownExecuteAt maxExecuteAt = executeAt.compareTo(with.executeAt) >= 0 ? executeAt : with.executeAt;
            KnownDeps maxDeps = deps.compareTo(with.deps) >= 0 ? deps : with.deps;
            Outcome maxOutcome = outcome.atLeast(with.outcome);
            if (maxRoute == route && maxDefinition == definition && maxExecuteAt == executeAt && maxDeps == deps &&  maxOutcome == outcome)
                return this;
            if (maxRoute == with.route && maxDefinition == with.definition && maxExecuteAt == with.executeAt && maxDeps == with.deps && maxOutcome == with.outcome)
                return with;
            return new Known(maxRoute, maxDefinition, maxExecuteAt, maxDeps, maxOutcome);
        }

        public Known validForBoth(Known with)
        {
            KnownRoute minRoute = route.validForBoth(with.route);
            Definition minDefinition = definition.compareTo(with.definition) <= 0 ? definition : with.definition;
            KnownExecuteAt maxExecuteAt = executeAt.compareTo(with.executeAt) >= 0 ? executeAt : with.executeAt;
            KnownDeps minDeps = deps.compareTo(with.deps) <= 0 ? deps : with.deps;
            Outcome minOutcome = outcome.atLeast(with.outcome);
            if (minRoute == route && minDefinition == definition && maxExecuteAt == executeAt && minDeps == deps &&  minOutcome == outcome)
                return this;
            if (minRoute == with.route && minDefinition == with.definition && maxExecuteAt == with.executeAt && minDeps == with.deps && minOutcome == with.outcome)
                return with;
            return new Known(minRoute, minDefinition, maxExecuteAt, minDeps, minOutcome);
        }

        public boolean isSatisfiedBy(Known that)
        {
            return this.definition.compareTo(that.definition) <= 0
                    && this.executeAt.compareTo(that.executeAt) <= 0
                    && this.deps.compareTo(that.deps) <= 0
                    && this.outcome.isSatisfiedBy(that.outcome);
        }

        /**
         * The logical epoch on which the specified knowledge is best sought or sent.
         * i.e., if we include an outcome then the execution epoch
         */
        public LogicalEpoch epoch()
        {
            if (outcome.isOrWasApply())
                return LogicalEpoch.Execution;

            return LogicalEpoch.Coordination;
        }

        public long fetchEpoch(TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (executeAt == null)
                return txnId.epoch();

            if (outcome.isOrWasApply() && !executeAt.equals(Timestamp.NONE))
                return executeAt.epoch();

            return txnId.epoch();
        }

        public Known with(Outcome newOutcome)
        {
            if (outcome == newOutcome)
                return this;
            return new Known(route, definition, executeAt, deps, newOutcome);
        }

        public Known with(KnownDeps newDeps)
        {
            if (deps == newDeps)
                return this;
            return new Known(route, definition, executeAt, newDeps, outcome);
        }

        // TODO (expected): merge propagates and propagatesStatus
        public Known propagates()
        {
            if (outcome == Outcome.Invalidated)
                return Invalidated;

            if (definition == DefinitionUnknown)
                return executeAt.isDecided() ? ExecuteAtOnly : Nothing;

            KnownExecuteAt executeAt = this.executeAt;
            if (!executeAt.isDecided())
                return DefinitionOnly;

            // cannot propagate proposed deps; and cannot propagate known deps without executeAt
            KnownDeps deps = this.deps;
            if (!deps.isDecided())
                return SaveStatus.PreCommittedWithDefinition.known;

            switch (outcome)
            {
                default: throw new AssertionError("Unhandled outcome: " + outcome);
                case Unknown:
                case WasApply:
                case Erased:
                    return Committed.minKnown;

                case Apply:
                    return PreApplied.minKnown;
            }
        }

        public Status propagatesStatus()
        {
            switch (outcome)
            {
                default: throw new AssertionError();
                case Erased:
                    return Status.Truncated;

                case Invalidated:
                    return Status.Invalidated;

                case Apply:
                case WasApply:
                    if (executeAt.isDecided() && definition.isKnown() && deps.hasDecidedDeps())
                        return PreApplied;

                case Unknown:
                    if (executeAt.isDecided() && definition.isKnown() && deps.hasDecidedDeps())
                        return Committed;

                    if (executeAt.isDecided())
                        return PreCommitted;

                    if (definition.isKnown())
                        return PreAccepted;

                    if (deps.hasDecidedDeps())
                        throw new IllegalStateException();
            }

            return outcome == WasApply ? Truncated : NotDefined;
        }

        public boolean isDefinitionKnown()
        {
            return definition.isKnown();
        }

        public boolean hasDefinitionBeenKnown()
        {
            return definition.isKnown() || outcome.isDecided();
        }

        public boolean isTruncated()
        {
            return outcome.isTruncated();
        }

        /**
         * The command may have an incomplete route when this is false
         */
        public boolean hasFullRoute()
        {
            return definition.isKnown() || outcome.isOrWasApply();
        }

        public boolean canProposeInvalidation()
        {
            return deps.canProposeInvalidation() && executeAt.canProposeInvalidation() && outcome.canProposeInvalidation();
        }

        public Known subtract(Known subtract)
        {
            if (!subtract.isSatisfiedBy(this))
                return Known.Nothing;

            Definition newDefinition = subtract.definition.compareTo(definition) >= 0 ? DefinitionUnknown : definition;
            KnownExecuteAt newExecuteAt = subtract.executeAt.compareTo(executeAt) >= 0 ? ExecuteAtUnknown : executeAt;
            KnownDeps newDeps = subtract.deps.compareTo(deps) >= 0 ? DepsUnknown : deps;
            Outcome newOutcome = outcome.subtract(subtract.outcome);
            return new Known(route, newDefinition, newExecuteAt, newDeps, newOutcome);
        }

        public boolean isDecided()
        {
            return executeAt.isDecided() || outcome.isDecided();
        }

        public boolean isDecidedToExecute()
        {
            return executeAt.hasDecidedExecuteAt() || outcome.isOrWasApply();
        }

        public String toString()
        {
            return Stream.of(definition.isKnown() ? "Definition" : null,
                             executeAt.isDecided() ? "ExecuteAt" : null,
                             deps.hasDecidedDeps() ? "Deps" : null,
                             outcome.isDecided() ? outcome.toString() : null
            ).filter(Objects::nonNull).collect(Collectors.joining(",", "[", "]"));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Known that = (Known) o;
            return route == that.route && definition == that.definition && executeAt == that.executeAt && deps == that.deps && outcome == that.outcome;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasDefinition()
        {
            return definition.isKnown();
        }

        public boolean hasDecidedDeps()
        {
            return deps.hasDecidedDeps();
        }
    }

    public enum KnownRoute
    {
        /**
         * A route may or may not be known, but it may not cover (or even intersect) this shard.
         * The route should be relied upon only if it is a FullRoute.
         */
        Maybe,

        /**
         * A route is known that covers the ranges this shard participates in.
         * Note that if the status is less than Committed, this may not be the final set of owned ranges,
         * and the route may not cover whatever this is decided as.
         *
         * This status primarily exists to communicate semantically to the reader.
         */
        Covering,

        /**
         * The full route is known. <i>Generally</i> this coincides with knowing the Definition.
         */
        Full
        ;

        public boolean hasFull()
        {
            return this == Full;
        }

        public KnownRoute validForBoth(KnownRoute that)
        {
            if (this == that) return this;
            if (this == Full || that == Full) return Full;
            return Maybe;
        }

        public KnownRoute atLeast(KnownRoute that)
        {
            return this.compareTo(that) >= 0 ? this : that;
        }
    }

    public enum KnownExecuteAt
    {
        /**
         * No decision is known to have been reached. If executeAt is not null, it represents either when
         * the transaction was witnessed, or some earlier ExecuteAtProposed that was invalidated by AcceptedInvalidate
         */
        ExecuteAtUnknown,

        /**
         * A decision to execute the transaction is known to have been proposed, and the associated executeAt timestamp
         */
        ExecuteAtProposed,

        /**
         * A decision to execute the transaction is known to have been reached, and the associated executeAt timestamp
         */
        ExecuteAtKnown,

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoExecuteAt
        ;

        public boolean isDecided()
        {
            return compareTo(ExecuteAtKnown) >= 0;
        }

        public boolean hasDecidedExecuteAt()
        {
            return this == ExecuteAtKnown;
        }

        public boolean canProposeInvalidation()
        {
            return this == ExecuteAtUnknown;
        }
    }

    public enum KnownDeps
    {
        /**
         * No decision is known to have been reached
         */
        DepsUnknown,

        /**
         * A decision to execute the transaction is known to have been proposed, and the associated dependencies
         * for the shard(s) in question are known for the coordination epoch (txnId.epoch) only.
         */
        DepsProposed,

        /**
         * A decision to execute the transaction is known to have been reached, and the associated dependencies
         * for the shard(s) in question are known for the coordination and execution epochs.
         */
        DepsKnown,

        /**
         * A decision to invalidate the transaction is known to have been reached
         */
        NoDeps
        ;

        public boolean hasDecidedDeps()
        {
            return this == DepsKnown;
        }

        public boolean isDecided()
        {
            return compareTo(DepsKnown) >= 0;
        }

        public boolean canProposeInvalidation()
        {
            return this == DepsUnknown;
        }

        public boolean hasProposedOrDecidedDeps()
        {
            return this == DepsProposed || this == DepsKnown;
        }
    }

    /**
     * Whether the transaction's definition is known.
     */
    public enum Definition
    {
        /**
         * The definition is not known
         */
        DefinitionUnknown,

        /**
         * The definition is known
         *
         * TODO (expected, clarity): distinguish between known for coordination epoch and known for commit/execute
         */
        DefinitionKnown,

        /**
         * The definition is irrelevant, as the transaction has been invalidated and may be treated as a no-op
         */
        NoOp;

        public boolean canProposeInvalidation()
        {
            return this == DefinitionUnknown;
        }

        public boolean isKnown()
        {
            return this == DefinitionKnown;
        }
    }

    /**
     * Whether a transaction's outcome (and its application) is known
     */
    public enum Outcome
    {
        /**
         * The outcome is not yet known (and may not yet be decided)
         */
        Unknown,

        /**
         * The transaction has been *completely cleaned up* - this means it has been made
         * durable at every live replica of every shard we contacted
         */
        Erased,

        /**
         * The transaction has been cleaned-up, but was applied and the relevant portion of its outcome has been cleaned up
         */
        WasApply,

        /**
         * The outcome is known
         */
        Apply,

        /**
         * The transaction is known to have been invalidated
         */
        Invalidated
        ;

        public boolean isOrWasApply()
        {
            return this == Apply || this == WasApply;
        }

        public boolean isSatisfiedBy(Outcome other)
        {
            switch (this)
            {
                default: throw new AssertionError();
                case Unknown:
                    return true;
                case WasApply:
                    if (other == Apply)
                        return true;
                case Apply:
                case Invalidated:
                case Erased:
                    return other == this;
            }
        }

        public boolean canProposeInvalidation()
        {
            return this == Unknown;
        }

        public boolean isInvalidated()
        {
            return this == Invalidated;
        }

        public boolean propagatesBetweenShards()
        {
            return this == Invalidated;
        }

        public boolean isTruncated()
        {
            return this == Erased || this == WasApply;
        }

        public Outcome atLeast(Outcome that)
        {
            return this.compareTo(that) >= 0 ? this : that;
        }

        // return a status that can be used to reconstruct for both covered ranges.
        // note: if one of the two is truncated, this pollutes the other with the concept of truncation;
        // DO NOT rely on this for deriving actual truncation status
        // TODO (expected): reify this distinction to avoid mistakes
        public Outcome canReconstructForBoth(Outcome that)
        {
            if (this == that) return this;
            Outcome max = this.compareTo(that) >= 0 ? this : that;
            Outcome min = this.compareTo(that) <= 0 ? this : that;
            if (max == Invalidated)
            {
                Invariants.checkState(min != Apply && min != WasApply);
                return Invalidated;
            }
            if (max == Apply || max == WasApply) return WasApply;
            return max;
        }

        public Outcome subtract(Outcome that)
        {
            return this.compareTo(that) <= 0 ? Unknown : this;
        }

        public boolean isDecided()
        {
            return this != Unknown;
        }
    }

    public enum LogicalEpoch
    {
        Coordination, Execution
    }

    /**
     * Represents the durability of a transaction's Persist phase.
     * NotDurable: the outcome has not been durably recorded
     * Local:      the outcome has been durably recorded at least locally
     * ShardUniversalOrInvalidated: the outcome has been durably recorded at all healthy replicas of the shard, or is invalidated
     * ShardUniversal:      the outcome has been durably recorded at all healthy replicas of the shard
     * MajorityOrInvalidated:   the outcome has been durably recorded to a majority of each participating shard
     * Majority:   the outcome has been durably recorded to a majority of each participating shard
     * Universal:  the outcome has been durably recorded to every healthy replica
     * DurableOrInvalidated:  the outcome was either invalidated, or has been durably recorded to every healthy replica
     */
    public enum Durability
    {
        NotDurable, Local, ShardUniversal,
        MajorityOrInvalidated, Majority,
        UniversalOrInvalidated, Universal;

        public boolean isDurable()
        {
            return this == Majority || this == Universal;
        }

        public boolean isDurableOrInvalidated()
        {
            return compareTo(MajorityOrInvalidated) >= 0;
        }

        public boolean isMaybeInvalidated()
        {
            return this == NotDurable || this == MajorityOrInvalidated || this == UniversalOrInvalidated;
        }

        public static Durability merge(Durability a, Durability b)
        {
            int c = a.compareTo(b);
            if (c < 0) { Durability tmp = a; a = b; b = tmp; }
            if (a == UniversalOrInvalidated && (b == Majority || b == ShardUniversal || b == Local)) a = Universal;
            if ((a == ShardUniversal) && (b == Local || b == NotDurable)) a = Local;
            if (b == NotDurable && a.compareTo(MajorityOrInvalidated) < 0) a = NotDurable;
            return a;
        }

        public static Durability mergeAtLeast(Durability a, Durability b)
        {
            int c = a.compareTo(b);
            if (c < 0) { Durability tmp = a; a = b; b = tmp; }
            if (a == UniversalOrInvalidated && (b == Majority || b == ShardUniversal || b == Local)) a = Universal;
            return a;
        }
    }

    public final Phase phase;
    public final Known minKnown;

    Status(Phase phase, Known minKnown)
    {
        this.phase = phase;
        this.minKnown = minKnown;
    }

    Status(Phase phase, KnownRoute route, Definition definition, KnownExecuteAt executeAt, KnownDeps deps, Status.Outcome outcome)
    {
        this.phase = phase;
        this.minKnown = new Known(route, definition, executeAt, deps, outcome);
    }

    // TODO (desired, clarity): investigate all uses of hasBeen, and migrate as many as possible to testing
    //                          Phase, ReplicationPhase and ExecutionStatus where these concepts are inadequate,
    //                          see if additional concepts can be introduced
    public boolean hasBeen(Status equalOrGreaterThan)
    {
        return compareTo(equalOrGreaterThan) >= 0;
    }

    public static <T> T max(List<T> list, Function<T, Status> getStatus, Function<T, Ballot> getAccepted, Predicate<T> filter)
    {
        T max = null;
        Status maxStatus = null;
        Ballot maxAccepted = null;
        for (T item : list)
        {
            if (!filter.test(item))
                continue;

            Status status = getStatus.apply(item);
            Ballot accepted = getAccepted.apply(item);
            boolean update = max == null
                          || maxStatus.phase.compareTo(status.phase) < 0
                          || (status.phase == Accept && maxAccepted.compareTo(accepted) < 0)
                          || status.phase != Accept && maxStatus.phase == status.phase && maxStatus.compareTo(status) > 0;

            if (!update)
                continue;

            max = item;
            maxStatus = status;
            maxAccepted = accepted;
        }

        return max;
    }

    public static <T> T max(T a, Status statusA, Ballot acceptedA, T b, Status statusB, Ballot acceptedB)
    {
        int c = statusA.phase.compareTo(statusB.phase);
        if (c > 0) return a;
        if (c < 0) return b;
        if (statusA.phase != Phase.Accept || acceptedA.compareTo(acceptedB) >= 0)
            return a;
        return b;
    }

    public static Status simpleMin(Status a, Status b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    public static Status simpleMax(Status a, Status b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }
}
