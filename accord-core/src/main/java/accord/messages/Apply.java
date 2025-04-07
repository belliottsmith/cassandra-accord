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

import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Apply.ApplyReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.APPLY_REQ;
import static accord.messages.MessageType.StandardMessage.APPLY_RSP;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

public class Apply extends TxnRequest<ApplyReply>
{
    public static final Factory FACTORY = Apply::new;
    public static class SerializationSupport
    {
        public static Apply create(TxnId txnId, Ballot ballot, Route<?> scope, long minEpoch, long waitForEpoch, long maxEpoch, Kind kind, Timestamp executeAt, PartialDeps deps, PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, Result result)
        {
            return new Apply(kind, txnId, ballot, scope, minEpoch, waitForEpoch, maxEpoch, executeAt, deps, txn, fullRoute, writes, result);
        }
    }

    public interface Factory
    {
        Apply create(Kind kind, Id to, Topologies participates, TxnId txnId, Ballot ballot, Route<?> scope, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, FullRoute<?> fullRoute);
    }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    public final PartialDeps deps; // TODO (expected): this should be nullable, and only included if we did not send Commit (or if sending Maximal apply)
    public final @Nullable PartialTxn txn;
    public final @Nullable FullRoute<?> fullRoute;
    public final @Nullable Writes writes;
    public final Result result;
    public final long minEpoch;
    public final long maxEpoch;

    public enum Kind { Minimal, Maximal }

    protected Apply(Kind kind, Id to, Topologies participates, TxnId txnId, Ballot ballot, Route<?> sendTo, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, FullRoute<?> fullRoute)
    {
        super(to, participates, sendTo, txnId);
        Invariants.require(txnId.kind() != Txn.Kind.Write || writes != null);
        this.ballot = ballot;
        this.kind = kind;
        this.deps = deps.intersecting(scope);
        this.txn = kind == Kind.Maximal ? txn.intersecting(scope, true) : null;
        this.fullRoute = kind == Kind.Maximal ? fullRoute : null;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.minEpoch = participates.oldestEpoch();
        this.maxEpoch = participates.currentEpoch();
    }

    public static Topologies participates(Node node, Unseekables<?> route, TxnId txnId, Timestamp executeAt, Topologies executes)
    {
        return txnId.epoch() == executeAt.epoch() ? executes : participates(node, route, txnId, executeAt);
    }

    public static Topologies participates(Node node, Unseekables<?> route, TxnId txnId, Timestamp executeAt)
    {
        return node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch(), SHARE);
    }

    protected Apply(Kind kind, TxnId txnId, Ballot ballot, Route<?> route, long minEpoch, long waitForEpoch, long maxEpoch, Timestamp executeAt, PartialDeps deps, @Nullable PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, Result result)
    {
        super(txnId, route, waitForEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.deps = deps;
        this.txn = txn;
        this.fullRoute = fullRoute;
        this.writes = writes;
        this.result = result;
        this.minEpoch = minEpoch;
        this.maxEpoch = maxEpoch;
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, maxEpoch, this);
    }

    @Override
    public ApplyReply apply(SafeCommandStore safeStore)
    {
        Route<?> route = fullRoute != null ? fullRoute : scope;
        StoreParticipants participants = StoreParticipants.execute(safeStore, route, minEpoch, txnId, maxEpoch);
        return apply(safeStore, participants);
    }

    public ApplyReply apply(SafeCommandStore safeStore, StoreParticipants participants)
    {
        return apply(safeStore, participants, ballot, txn, txnId, executeAt, deps, participants.route(), writes, result);
    }

    public static ApplyReply apply(SafeCommandStore safeStore, StoreParticipants participants, Ballot ballot, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, Result result)
    {
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        return apply(safeStore, safeCommand, participants, ballot, txn, txnId, executeAt, deps, route, writes, result);
    }

    public static ApplyReply apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Ballot ballot, PartialTxn txn, TxnId txnId, Timestamp executeAt, PartialDeps deps, Route<?> route, Writes writes, Result result)
    {
        switch (Commands.apply(safeStore, safeCommand, participants, ballot, txnId, route, executeAt, deps, txn, writes, result))
        {
            default:
            case Success: return ApplyReply.Applied;
            case Redundant: return ApplyReply.Redundant;
            case Insufficient: return ApplyReply.Insufficient;
            case RaceWithRecovery: return ApplyReply.RaceWithRecovery;
        }
    }

    @Override
    public ApplyReply reduce(ApplyReply a, ApplyReply b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    @Override
    public KeyHistory keyHistory()
    {
        // TODO (expected): need to guarantee execution order then can make this ASYNC
        return KeyHistory.SYNC;
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public enum ApplyReply implements Reply
    {
        Applied, Redundant, Insufficient, RaceWithRecovery;

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }

        @Override
        public String toString()
        {
            return "Apply" + name();
        }

        @Override
        public boolean isFinal()
        {
            return this != Insufficient;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{kind:" + kind +
               ", txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
