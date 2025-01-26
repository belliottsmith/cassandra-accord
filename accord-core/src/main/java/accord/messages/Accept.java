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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.KeyHistory;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.Toggles.filterDuplicateDependenciesFromAcceptReply;
import static accord.local.Commands.AcceptOutcome.Redundant;
import static accord.local.Commands.AcceptOutcome.RejectedBallot;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.local.KeyHistory.SYNC;

// TODO (low priority, efficiency): use different objects for send and receive, so can be more efficient
//                                  (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends TxnRequest.WithUnsynced<Accept.AcceptReply>
{
    public static class SerializerSupport
    {
        public static Accept create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps)
        {
            return new Accept(txnId, scope, waitForEpoch, minEpoch, kind, ballot, executeAt, partialDeps);
        }
    }

    public enum Kind { SLOW, MEDIUM }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    public final PartialDeps partialDeps;

    public Accept(Id to, Topologies topologies, Kind kind, Ballot ballot, TxnId txnId, FullRoute<?> route, Timestamp executeAt, Deps deps)
    {
        super(to, topologies, txnId, route);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = deps.intersecting(scope);
    }

    private Accept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = partialDeps;
    }

    @Override
    public AcceptReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, txnId.epoch(), executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        AcceptOutcome outcome = Commands.accept(safeStore, safeCommand, participants, txnId, kind, ballot, scope, executeAt, partialDeps);
        switch (outcome)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Truncated:
                return AcceptReply.redundant(ballot, participants, safeCommand.current());
            case RejectedBallot:
                return new AcceptReply(safeCommand.current().promised());
            case Retired:
                // if we're Retired, participants.owns() is empty, so we're just fetching deps
                // TODO (desired): optimise deps calculation; for some keys we only need to return the last RX
            case Success:
                Deps deps = calculateDeps(safeStore, participants);
                if (deps == null)
                    return AcceptReply.redundant(ballot, participants, safeCommand.current());

                Invariants.require(deps.maxTxnId(txnId).epoch() <= executeAt.epoch());
                if (filterDuplicateDependenciesFromAcceptReply())
                    deps = deps.without(this.partialDeps);
                return new AcceptReply(deps);
        }
    }

    private Deps calculateDeps(SafeCommandStore safeStore, StoreParticipants participants)
    {
        return PreAccept.calculateDeps(safeStore, txnId, participants, EpochSupplier.constant(minEpoch), executeAt, true);
    }

    @Override
    public AcceptReply reduce(AcceptReply r1, AcceptReply r2)
    {
        return AcceptReply.reduce(r1, r2);
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch(), this);
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
        return SYNC;
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
    }

    public String toString() {
        return "Accept{" +
                "kind: " + kind +
                ", ballot: " + ballot +
                ", txnId: " + txnId +
                ", executeAt: " + executeAt +
                ", deps: " + partialDeps +
                '}';
    }

    public static final class AcceptReply implements Reply
    {
        public static final AcceptReply SUCCESS = new AcceptReply(Success);

        public final AcceptOutcome outcome;
        public final Ballot supersededBy;
        // TODO (expected): only send back deps that weren't in those we received
        public final @Nullable Deps deps;
        public final @Nullable Timestamp committedExecuteAt;

        private AcceptReply(AcceptOutcome outcome)
        {
            this.outcome = outcome;
            this.supersededBy = null;
            this.deps = null;
            this.committedExecuteAt = null;
        }

        public AcceptReply(Ballot supersededBy)
        {
            this.outcome = RejectedBallot;
            this.supersededBy = supersededBy;
            this.deps = null;
            this.committedExecuteAt = null;
        }

        public AcceptReply(@Nonnull Deps deps)
        {
            this.outcome = Success;
            this.supersededBy = null;
            this.deps = deps;
            this.committedExecuteAt = null;
        }

        public AcceptReply(Ballot supersededBy, @Nullable Timestamp committedExecuteAt)
        {
            this.outcome = Redundant;
            this.supersededBy = supersededBy;
            this.deps = null;
            this.committedExecuteAt = committedExecuteAt;
        }

        static AcceptReply redundant(Ballot ballot, StoreParticipants participants, Command command)
        {
            if (participants.owns().isEmpty() && (command.is(Status.Truncated) || command.promised().compareTo(ballot) <= 0))
                return new AcceptReply(Deps.NONE);

            Ballot superseding = command.promised();
            if (superseding.compareTo(ballot) <= 0)
                superseding = null;
            return new AcceptReply(superseding, command.executeAtIfKnown());
        }

        public static AcceptReply reduce(AcceptReply r1, AcceptReply r2)
        {
            if (!r1.isOk() || !r2.isOk())
                return r1.outcome().compareTo(r2.outcome()) >= 0 ? r1 : r2;

            Deps deps = r1.deps == null ? r2.deps : r2.deps == null ? r1.deps : r1.deps.with(r2.deps);
            if (deps == r1.deps) return r1;
            if (deps == r2.deps) return r2;
            return new AcceptReply(deps);
        }

        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_RSP;
        }

        public boolean isOk()
        {
            return outcome == Success;
        }

        public AcceptOutcome outcome()
        {
            return outcome;
        }

        @Override
        public String toString()
        {
            switch (outcome)
            {
                default: throw new AssertionError();
                case Success:
                    return "AcceptOk{deps=" + deps + '}';
                case Redundant:
                    return "AcceptRedundant(" + supersededBy + ',' + committedExecuteAt + ")";
                case RejectedBallot:
                    return "AcceptNack(" + supersededBy + ")";
            }
        }
    }

    public static class NotAccept extends AbstractRequest<AcceptReply>
    {
        public final Status status;
        public final Ballot ballot;
        public final Participants<?> participants;

        public NotAccept(Status status, Ballot ballot, TxnId txnId, Participants<?> participants)
        {
            super(txnId);
            this.status = status;
            this.ballot = ballot;
            this.participants = participants;
        }

        @Override
        public Cancellable submit()
        {
            return node.mapReduceConsumeLocal(this, participants, txnId.epoch(), txnId.epoch(), this);
        }

        @Override
        public AcceptReply apply(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.notAccept(safeStore, this.participants, txnId);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            AcceptOutcome outcome = Commands.notAccept(safeStore, safeCommand, status, ballot);
            switch (outcome)
            {
                default: throw new IllegalArgumentException("Unknown status: " + outcome);
                case Redundant:
                case Truncated:
                    return AcceptReply.redundant(ballot, participants, safeCommand.current());
                case Retired:
                case Success:
                    return AcceptReply.SUCCESS;
                case RejectedBallot:
                    return new AcceptReply(safeCommand.current().promised());
            }
        }

        @Override
        public AcceptReply reduce(AcceptReply r1, AcceptReply r2)
        {
            return AcceptReply.reduce(r1, r2);
        }

        @Override
        public MessageType type()
        {
            return MessageType.NOT_ACCEPT_REQ;
        }

        @Override
        public String toString()
        {
            return "NotAccept{kind: " + status + ", ballot:" + ballot + ", txnId:" + txnId + ", key:" + participants + '}';
        }

        @Override
        public long waitForEpoch()
        {
            return txnId.epoch();
        }
    }
}
