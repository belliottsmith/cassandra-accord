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

package accord.coordinate;

import java.util.Map;
import java.util.function.BiConsumer;

import accord.api.ProtocolModifiers.Faults;
import accord.api.RoutingKey;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.SimpleTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Accept;
import accord.messages.Accept.AcceptReply;
import accord.messages.Callback;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedListMap;
import accord.utils.WrappableException;

import static accord.api.ProtocolModifiers.Toggles.filterDuplicateDependenciesFromAcceptReply;
import static accord.coordinate.ExecutePath.MEDIUM;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.primitives.Status.AcceptedInvalidate;
import static accord.primitives.Status.NotAccepted;
import static accord.primitives.Status.PreNotAccepted;
import static accord.primitives.TxnId.MediumPath.MEDIUM_PATH_TRACK_STABLE;
import static accord.utils.Invariants.debug;

abstract class Propose<R> implements Callback<AcceptReply>
{
    final Node node;
    final Accept.Kind kind;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;

    final SortedListMap<Id, AcceptReply> acceptOks;
    final Timestamp executeAt;
    final QuorumTracker acceptTracker;
    final BiConsumer<? super R, Throwable> callback;
    private boolean isDone;

    Propose(Node node, Topologies topologies, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
    {
        this.node = node;
        this.kind = kind;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.deps = deps;
        this.executeAt = executeAt;
        this.callback = callback;
        this.acceptOks = new SortedListMap<>(topologies.nodes(), AcceptReply[]::new);
        this.acceptTracker = new QuorumTracker(topologies);
        Invariants.checkState(txnId.isSyncPoint() || deps.maxTxnId(txnId).compareTo(executeAt) <= 0,
                              "Attempted to propose %s with an earlier executeAt than a conflicting transaction it witnessed: %s vs executeAt: %s", txnId, deps, executeAt);
    }

    void start()
    {
        SortedArrays.SortedArrayList<Node.Id> contact = acceptTracker.filterAndRecordFaulty();
        if (contact == null) callback.accept(null, new Timeout(null, null));
        else node.send(contact, to -> new Accept(to, acceptTracker.topologies(), kind, ballot, txnId, route, executeAt, deps), this);
    }

    @Override
    public void onSuccess(Id from, AcceptReply reply)
    {
        if (isDone)
            return;

        switch (reply.outcome())
        {
            default: throw new AssertionError("Unhandled AcceptOutcome: " + reply.outcome());
            case Truncated:
                isDone = true;
                callback.accept(null, new Truncated(txnId, route.homeKey()));
                break;

            case RejectedBallot:
                isDone = true;
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                break;

            case Redundant:
                if (reply.supersededBy != null || ballot.equals(Ballot.ZERO))
                {
                    isDone = true;
                    callback.accept(null, new Preempted(txnId, route.homeKey()));
                }
                else if (reply.committedExecuteAt != null)
                {
                    isDone = true;
                    callback.accept(null, new Redundant(txnId, route.homeKey(), reply.committedExecuteAt));
                }

                break;

            case Retired:
            case Success:
                acceptOks.put(from, reply);
                if (acceptTracker.recordSuccess(from) == Success)
                {
                    isDone = true;
                    onAccepted();
                }
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        // TODO (expected): we aren't tracking the specific failure here to report
        if (acceptTracker.recordFailure(from) == Failed)
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, route.homeKey()));
        }
    }

    @Override
    public boolean onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone)
            return false;

        isDone = true;
        callback.accept(null, failure);
        return true;
    }

    void onAccepted()
    {
        if (kind == Accept.Kind.MEDIUM)
        {
            adapter().execute(node, acceptTracker.topologies(), route, MEDIUM, txnId, txn, executeAt, deps, callback);
        }
        else
        {
            adapter().stabilise(node, acceptTracker.topologies(), route, ballot, txnId, txn, executeAt, mergeDeps(), callback);
        }
    }

    Deps mergeDeps()
    {
        Deps deps = Deps.merge(acceptOks, acceptOks.domainSize(), SortedListMap::getValue, ok -> ok.deps);
        if (Faults.discardPreAcceptDeps(txnId))
            return deps;

        if (txnId.is(MEDIUM_PATH_TRACK_STABLE))
        {
            if (!filterDuplicateDependenciesFromAcceptReply())
                deps = deps.without(this.deps);

            deps = deps.markUnstableBefore(txnId);
        }

        return deps.with(this.deps);
    }

    abstract CoordinationAdapter<R> adapter();

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class NotAccept implements Callback<AcceptReply>
    {
        final Node node;
        final Status status;
        final Ballot ballot;
        final TxnId txnId;
        final Participants<?> someParticipants;
        final BiConsumer<Void, Throwable> callback;
        final Map<Id, AcceptReply> debug;

        private final SimpleTracker<?> acceptTracker;
        private boolean isDone;

        NotAccept(Node node, Status status, Topologies topologies, Ballot ballot, TxnId txnId, Participants<?> someParticipants, BiConsumer<Void, Throwable> callback)
        {
            this.node = node;
            this.status = status;
            this.acceptTracker = new QuorumTracker(topologies);
            this.ballot = ballot;
            this.txnId = txnId;
            this.someParticipants = someParticipants;
            this.callback = callback;
            this.debug = debug() ? new SortedListMap<>(topologies.nodes(), AcceptReply[]::new) : null;
        }

        public static NotAccept proposeInvalidate(Node node, Ballot ballot, TxnId txnId, RoutingKey invalidateWithParticipant, BiConsumer<Void, Throwable> callback)
        {
            return proposeNotAccept(node, AcceptedInvalidate, ballot, txnId, invalidateWithParticipant, callback);
        }

        public static NotAccept proposeNotAccept(Node node, Status status, Ballot ballot, TxnId txnId, RoutingKey participatingKey, BiConsumer<Void, Throwable> callback)
        {
            Participants<?> participants = Participants.singleton(txnId.domain(), participatingKey);
            Topologies topologies = node.topology().forEpoch(participants, txnId.epoch());
            NotAccept notAccept = new NotAccept(node, status, topologies, ballot, txnId, participants, callback);
            node.send(topologies.nodes(), to -> new Accept.NotAccept(status, ballot, txnId, participants), notAccept);
            return notAccept;
        }

        public static NotAccept proposeAndCommitInvalidate(Node node, Ballot ballot, TxnId txnId, RoutingKey invalidateWithParticipant, Route<?> commitInvalidationTo, Timestamp invalidateUntil, BiConsumer<?, Throwable> callback)
        {
            return proposeInvalidate(node, ballot, txnId, invalidateWithParticipant, (success, fail) -> {
                if (fail != null)
                {
                    callback.accept(null, fail);
                }
                else
                {
                    node.withEpoch(invalidateUntil.epoch(), callback, t -> WrappableException.wrap(t), () -> {
                        commitInvalidate(node, txnId, commitInvalidationTo, invalidateUntil);
                        callback.accept(null, new Invalidated(txnId, invalidateWithParticipant));
                    });
                }
            });
        }

        public static NotAccept proposeAndPersistNotAccept(Node node, Ballot ballot, TxnId txnId, RoutingKey participatingKey, Route<?> commitNotAcceptTo, BiConsumer<?, Throwable> callback)
        {
            return proposeNotAccept(node, PreNotAccepted, ballot, txnId, participatingKey, (success, fail) -> {
                if (fail != null)
                {
                    if (callback != null)
                        callback.accept(null, fail);
                }
                else
                {
                    Topologies topologies = node.topology().forEpoch(commitNotAcceptTo, txnId.epoch());
                    node.send(topologies.nodes(), to -> new Accept.NotAccept(NotAccepted, ballot, txnId, commitNotAcceptTo));
                    if (callback != null)
                        callback.accept(null, null);
                }
            });
        }

        @Override
        public void onSuccess(Id from, AcceptReply reply)
        {
            if (isDone)
                return;

            if (debug != null) debug.put(from, reply);

            if (!reply.isOk())
            {
                isDone = true;
                callback.accept(null, new Preempted(txnId, null));
                return;
            }

            if (acceptTracker.recordSuccess(from) == Success)
            {
                isDone = true;
                callback.accept(null, null);
            }
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            if (isDone)
                return;

            if (acceptTracker.recordFailure(from) == Failed)
            {
                isDone = true;
                callback.accept(null, new Timeout(txnId, null));
            }
        }

        @Override
        public boolean onCallbackFailure(Id from, Throwable failure)
        {
            if (isDone) return false;

            isDone = true;
            callback.accept(null, failure);
            return true;
        }
    }
}
