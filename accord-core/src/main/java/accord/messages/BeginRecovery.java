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

package accord.messages;

import java.util.Collection;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.*;
import accord.local.Node.Id;
import accord.local.CommandSummaries.IsDep;
import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.local.CommandSummaries.IsDep.IS_NOT_DEP;
import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.INVALIDATED;
import static accord.local.CommandSummaries.ComputeIsDep.EITHER;
import static accord.local.CommandSummaries.TestStartedAt.ANY;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Ok;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Reject;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Retired;
import static accord.messages.BeginRecovery.RecoverReply.Kind.Truncated;
import static accord.messages.PreAccept.calculateDeps;
import static accord.primitives.EpochSupplier.constant;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Status.Accepted;
import static accord.primitives.Status.Phase;
import static accord.primitives.Status.PreAccepted;
import static accord.utils.Invariants.illegalState;

public class BeginRecovery extends TxnRequest.WithUnsynced<BeginRecovery.RecoverReply>
{
    public static class SerializationSupport
    {
        public static BeginRecovery create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route, long executeAtOrTxnIdEpoch)
        {
            return new BeginRecovery(txnId, scope, waitForEpoch, minEpoch, partialTxn, ballot, route, executeAtOrTxnIdEpoch);
        }
    }

    public final PartialTxn partialTxn;
    public final Ballot ballot;
    public final FullRoute<?> route;
    public final long executeAtOrTxnIdEpoch;

    public BeginRecovery(Id to, Topologies topologies, TxnId txnId, @Nullable Timestamp executeAt, Txn txn, FullRoute<?> route, Ballot ballot)
    {
        super(to, topologies, txnId, route);
        // TODO (expected): only scope.contains(route.homeKey); this affects state eviction and is low priority given size in C*
        this.partialTxn = txn.intersecting(scope, true);
        this.ballot = ballot;
        this.route = route;
        this.executeAtOrTxnIdEpoch = topologies.currentEpoch();
        Invariants.checkState(executeAt == null || executeAt.epoch() == topologies.currentEpoch());
    }

    private BeginRecovery(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, PartialTxn partialTxn, Ballot ballot, @Nullable FullRoute<?> route, long executeAtOrTxnIdEpoch)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.partialTxn = partialTxn;
        this.ballot = ballot;
        this.route = route;
        this.executeAtOrTxnIdEpoch = executeAtOrTxnIdEpoch;
    }

    @Override
    protected Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executeAtOrTxnIdEpoch, this);
    }

    @Override
    public RecoverReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, route, minEpoch, txnId, executeAtOrTxnIdEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Commands.AcceptOutcome outcome = Commands.recover(safeStore, safeCommand, participants, txnId, partialTxn, route, ballot);
        switch (outcome)
        {
            default: throw illegalState("Unhandled Outcome: " + outcome);
            case Redundant: throw illegalState("Invaid Outcome: " + outcome);

            case Truncated:
                return new RecoverNack(Truncated, null);

            case Retired:
                return new RecoverNack(Retired, null);

            case RejectedBallot:
                return new RecoverNack(Reject, safeCommand.current().promised());

            case Success:
        }

        Command command = safeCommand.current();

        LatestDeps deps; {
            PartialDeps coordinatedDeps = command.partialDeps();
            Deps localDeps = null;
            if (!command.known().deps.hasCommittedOrDecidedDeps())
            {
                localDeps = calculateDeps(safeStore, txnId, participants, constant(minEpoch), txnId, false);
            }
            if (localDeps != null && coordinatedDeps != null && !participants.touches().equals(coordinatedDeps.covering))
            {
                deps = LatestDeps.create(coordinatedDeps.covering, command.known().deps, command.acceptedOrCommitted(), coordinatedDeps, null);
                deps = LatestDeps.merge(deps, LatestDeps.create(participants.touches(), DepsUnknown, Ballot.ZERO, null, localDeps));
            }
            else
            {
                Participants<?> knownFor = coordinatedDeps == null ? participants.touches() : coordinatedDeps.covering;
                deps = LatestDeps.create(knownFor, command.known().deps, command.acceptedOrCommitted(), coordinatedDeps, localDeps);
            }
        }

        boolean supersedingRejects;
        Deps earlierCommittedWitness, earlierAcceptedNoWitness;
        if (command.hasBeen(Accepted))
        {
            supersedingRejects = false;
            earlierCommittedWitness = earlierAcceptedNoWitness = Deps.NONE;
        }
        else
        {
            // TODO (expected): modify the mapReduce API to perform this check in a single pass
            class Visitor implements CommandSummaries.AllCommandVisitor, AutoCloseable
            {
                Deps.Builder nowait;
                Deps.Builder wait;
                boolean supersedingRejects;

                @Override
                public boolean visit(Unseekable keyOrRange, TxnId testTxnId, Timestamp testExecuteAt, SummaryStatus status, @Nullable IsDep dep)
                {
                    if (testTxnId.compareTo(txnId) < 0)
                    {
                        if (status == INVALIDATED)
                        {
                            ensureNoWait().add(keyOrRange, testTxnId);
                        }
                        else if (testExecuteAt.compareTo(txnId) > 0)
                        {
                            switch (dep)
                            {
                                default: throw new AssertionError("Unhandled SummaryDep: " + dep);
                                case IS_DEP:
                                    ensureNoWait().add(keyOrRange, testTxnId);
                                    break;

                                case IS_NOT_DEP:
                                    /*
                                     * The idea here is to discover those transactions that have been decided to execute after us
                                     * and did not witness us as part of their pre-accept or accept round, as this means that we CANNOT have
                                     * taken the fast path. This is central to safe recovery, as if every transaction that executes later has
                                     * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
                                     * has not witnessed us we can safely invalidate it.
                                     */
                                    supersedingRejects = true;
                                    break;

                                case NOT_ELIGIBLE:
                                    Invariants.checkState(status.compareTo(ACCEPTED) <= 0);
                                    if (status == ACCEPTED)
                                        ensureWait().add(keyOrRange, testTxnId);
                            }
                        }
                    }
                    else if (dep == IS_NOT_DEP)
                    {
                        /*
                         * The idea here is to discover those transactions that were started after us and have been Accepted
                         * and did not witness us as part of their pre-accept round, as this means that we CANNOT have taken
                         * the fast path. This is central to safe recovery, as if every transaction that executes later has
                         * witnessed us we are safe to propose the pre-accept timestamp regardless, whereas if any transaction
                         * has not witnessed us we can safely invalidate (us).
                         */
                        supersedingRejects = true;
                    }

                    return !supersedingRejects;
                }

                private Deps.Builder ensureNoWait()
                {
                    if (nowait == null)
                        nowait = new Deps.Builder(true);
                    return nowait;
                }

                private Deps.Builder ensureWait()
                {
                    if (wait == null)
                        wait = new Deps.Builder(true);
                    return wait;
                }

                @Override
                public void close()
                {
                    if (nowait != null)
                    {
                        nowait.close();
                        nowait = null;
                    }
                    if (wait != null)
                    {
                        wait.close();
                        wait = null;
                    }
                }
            }

            try (Visitor visitor = new Visitor())
            {
                safeStore.visit(participants.owns(), txnId, txnId.witnessedBy(), ANY, txnId, EITHER, visitor);
                supersedingRejects = visitor.supersedingRejects;
                earlierCommittedWitness = visitor.nowait == null ? Deps.NONE : visitor.nowait.build();
                earlierAcceptedNoWitness = visitor.wait == null ? Deps.NONE : visitor.wait.build();
            }
        }

        Status status = command.status();
        Ballot accepted = command.acceptedOrCommitted();
        Timestamp executeAt = command.executeAt();
        Writes writes = command.writes();
        Result result = command.result();
        boolean acceptsFastPath = executeAt.equals(txnId) || participants.owns().isEmpty();
        return new RecoverOk(txnId, status, accepted, executeAt, deps, earlierCommittedWitness, earlierAcceptedNoWitness, acceptsFastPath, supersedingRejects, writes, result);
    }

    @Override
    public RecoverReply reduce(RecoverReply r1, RecoverReply r2)
    {
        // TODO (low priority, efficiency): should not operate on dependencies directly here, as we only merge them;
        //                                  want a cheaply mergeable variant (or should collect them before merging)

        RecoverReply.Kind r1kind = r1.kind(), r2kind = r2.kind();
        if (r1kind != Ok || r2kind != Ok)
        {
            if (r1kind == Retired && r2kind == Ok) return r2;
            if (r2kind == Retired && r1kind == Ok) return r1;
            return r1kind.compareTo(r2kind) >= 0 ? r1 : r2;
        }
        RecoverOk ok1 = (RecoverOk) r1;
        RecoverOk ok2 = (RecoverOk) r2;

        // set ok1 to the most recent of the two
        if (ok1 != Status.max(ok1, ok1.status, ok1.accepted, ok2, ok2.status, ok2.accepted))
        {
            RecoverOk tmp = ok1;
            ok1 = ok2;
            ok2 = tmp;
        }
        if (!ok1.status.hasBeen(PreAccepted)) throw new IllegalStateException();

        LatestDeps deps = LatestDeps.merge(ok1.deps, ok2.deps);
        Deps earlierCommittedWitness = ok1.earlierCommittedWitness.with(ok2.earlierCommittedWitness);
        Deps earlierAcceptedNoWitness = ok1.earlierAcceptedNoWitness.with(ok2.earlierAcceptedNoWitness)
                .without(earlierCommittedWitness::contains);
        Timestamp timestamp = ok1.status == PreAccepted ? Timestamp.max(ok1.executeAt, ok2.executeAt) : ok1.executeAt;

        return new RecoverOk(
            txnId, ok1.status, ok1.accepted, timestamp,
            deps, earlierCommittedWitness, earlierAcceptedNoWitness,
            ok1.selfAcceptsFastPath & ok2.selfAcceptsFastPath,
            ok1.supersedingRejects | ok2.supersedingRejects,
            ok1.writes, ok1.result
        );
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Unseekables<?> keys()
    {
        return scope;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.RECOVER;
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_RECOVER_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginRecovery{" +
               "txnId:" + txnId +
               ", txn:" + partialTxn +
               ", ballot:" + ballot +
               '}';
    }


    public static abstract class RecoverReply implements Reply
    {
        // TODO (expected): recover should gracefully handle partial truncation (currently expected to be handled by MaybeRecover)
        public enum Kind { Ok, Retired, Truncated, Reject }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_RECOVER_RSP;
        }

        public abstract Kind kind();
    }

    public static class RecoverOk extends RecoverReply
    {
        public final TxnId txnId; // for debugging
        public final Status status;
        public final Ballot accepted;
        public final Timestamp executeAt;
        public final LatestDeps deps;
        public final Deps earlierCommittedWitness;  // counter-point to earlierAcceptedNoWitness
        public final Deps earlierAcceptedNoWitness; // wait for these to commit
        public final boolean selfAcceptsFastPath;
        public final boolean supersedingRejects;
        public final Writes writes;
        public final Result result;

        public RecoverOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, LatestDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean selfAcceptsFastPath, boolean supersedingRejects, Writes writes, Result result)
        {
            this.txnId = txnId;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.status = status;
            this.deps = deps;
            this.earlierCommittedWitness = earlierCommittedWitness;
            this.earlierAcceptedNoWitness = earlierAcceptedNoWitness;
            this.selfAcceptsFastPath = selfAcceptsFastPath;
            this.supersedingRejects = supersedingRejects;
            this.writes = writes;
            this.result = result;
        }

        @Override
        public Kind kind()
        {
            return Ok;
        }

        @Override
        public String toString()
        {
            return toString("RecoverOk");
        }

        String toString(String kind)
        {
            return kind + "{" +
                   "txnId:" + txnId +
                   ", status:" + status +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", deps:" + deps +
                   ", earlierCommittedWitness:" + earlierCommittedWitness +
                   ", earlierAcceptedNoWitness:" + earlierAcceptedNoWitness +
                   ", selfAcceptsFastPath:" + selfAcceptsFastPath +
                   ", supersedingRejects:" + supersedingRejects +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }

        public static RecoverOk maxAccepted(Collection<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r != null && r.status.phase.compareTo(Phase.Accept) >= 0);
        }

        public static RecoverOk maxAcceptedNotTruncated(Collection<RecoverOk> recoverOks)
        {
            return Status.max(recoverOks, r -> r.status, r -> r.accepted, r -> r != null && r.status.phase.compareTo(Phase.Accept) >= 0 && r.status.phase.compareTo(Phase.Cleanup) < 0);
        }
    }

    public static class RecoverNack extends RecoverReply
    {
        public final Kind kind;
        public final @Nullable Ballot supersededBy;

        public RecoverNack(Kind kind, @Nullable Ballot supersededBy)
        {
            this.kind = kind;
            this.supersededBy = supersededBy;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public String toString()
        {
            return "RecoverNack{" +
                   "supersededBy:" + supersededBy +
                   '}';
        }
    }
}
