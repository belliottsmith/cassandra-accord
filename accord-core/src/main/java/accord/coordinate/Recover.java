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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.TraceEventType;
import accord.api.Tracing;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.RecoveryTracker;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Accept;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;
import accord.utils.SortedListMap;
import accord.utils.UnhandledEnum;
import accord.utils.WrappableException;
import accord.utils.async.AsyncChainCombiner;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.api.ProgressLog.BlockedUntil.CommittedOrNotFastPathCommit;
import static accord.api.ProgressLog.BlockedUntil.HasCommittedDeps;
import static accord.api.ProgressLog.BlockedUntil.HasDecidedExecuteAt;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.coordinate.Propose.NotAccept.proposeInvalidate;
import static accord.coordinate.Recover.InferredFastPath.Reject;
import static accord.coordinate.Recover.InferredFastPath.Unknown;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.Accept.Kind.SLOW;
import static accord.messages.BeginRecovery.RecoverOk.maxAccepted;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedNotTruncated;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.AcceptedMedium;
import static accord.primitives.Status.AcceptedSlow;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FLOOR;

public class Recover implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    public enum InferredFastPath
    {
        Accept, Unknown, Reject;

        public InferredFastPath merge(InferredFastPath that)
        {
            return compareTo(that) >= 0 ? this : that;
        }
    }

    private final CoordinationAdapter<Result> adapter;
    private final Node node;
    private final SequentialAsyncExecutor executor;
    private final Ballot ballot;
    private final TxnId txnId;
    private final Txn txn;
    private final FullRoute<?> route;
    private final @Nullable Timestamp committedExecuteAt;
    private final boolean isFastPathDecided;
    private final LatentStoreSelector reportTo;
    private final BiConsumer<Outcome, Throwable> callback;
    private final @Nullable Tracing tracing;
    private boolean isDone;

    private SortedListMap<Id, RecoverOk> recoverOks;
    private final RecoveryTracker tracker;
    private boolean isBallotPromised;

    private Recover(Node node, SequentialAsyncExecutor executor, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route,
                    @Nullable Timestamp committedExecuteAt, boolean isFastPathDecided, LatentStoreSelector reportTo,
                    BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        this.isFastPathDecided = isFastPathDecided;
        Invariants.require(txnId.isVisible());
        this.adapter = node.coordinationAdapter(txnId, Recovery);
        this.node = node;
        this.executor = executor;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.committedExecuteAt = committedExecuteAt;
        this.reportTo = reportTo;
        this.callback = callback;
        this.tracing = tracing;
        this.tracker = new RecoveryTracker(topologies);
        this.recoverOks = new SortedListMap<>(topologies.nodes(), RecoverOk[]::new);
    }

    @Override
    public void accept(Result result, Throwable failure)
    {
        Invariants.require(!isDone);
        isDone = true;
        if (failure == null)
        {
            callback.accept(ProgressToken.APPLIED, null);
        }
        else if (failure instanceof Redundant)
        {
            Timestamp committedExecuteAt = ((Redundant) failure).committedExecuteAt;
            if (tracing != null)
                tracing.trace(null, "Recover found Redundant; retrying with known committedExecuteAt " + committedExecuteAt);
            retry(committedExecuteAt);
        }
        else
        {
            callback.accept(null, WrappableException.wrap(failure));
        }

        node.agent().coordinatorEvents().onRecoveryStopped(node, txnId, ballot, result, failure);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, boolean isFastPathDecided, BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        return recover(node, txnId, txn, route, isFastPathDecided, LatentStoreSelector.standard(), callback, tracing);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        Ballot ballot = node.uniqueTimestamp(Ballot::fromValues);
        return recover(node, ballot, txnId, txn, route, isFastPathDecided, reportTo, callback, tracing);
    }

    private static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        return recover(node, ballot, txnId, txn, route, null, isFastPathDecided, reportTo, callback, tracing);
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, @Nullable Timestamp committedExecuteAt, boolean isFastPathDecided, BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        return recover(node, ballot, txnId, txn, route, committedExecuteAt, isFastPathDecided, null, callback, tracing);
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, @Nullable Timestamp committedExecuteAt, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        Topologies topologies = node.topology().select(route, txnId, committedExecuteAt == null ? txnId : committedExecuteAt, SHARE, QuorumEpochIntersections.recover);
        return recover(node, topologies, ballot, txnId, txn, route, committedExecuteAt, isFastPathDecided, reportTo, callback, tracing);
    }

    private static Recover recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp committedExecuteAt, boolean isFastPathDecided, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback, @Nullable Tracing tracing)
    {
        Recover recover = new Recover(node, node.someSequentialExecutor(), topologies, ballot, txnId, txn, route, committedExecuteAt, isFastPathDecided, reportTo, callback, tracing);
        recover.start(topologies.nodes());
        return recover;
    }

    void start(Collection<Id> nodes)
    {
        node.agent().coordinatorEvents().onRecoveryStarted(txnId, ballot);
        node.send(nodes, to -> new BeginRecovery(to, tracker.topologies(), txnId, committedExecuteAt, isFastPathDecided, txn, route, ballot), executor, this);
    }

    @Override
    public void onSuccess(Id from, RecoverReply reply)
    {
        if (isDone || isBallotPromised)
            return;

        if (tracing != null)
            tracing.trace(null, "Recover received from %s: %s", from, reply);

        boolean acceptsFastPath;
        switch (reply.kind())
        {
            default: throw new AssertionError("Unhandled RecoverReply.Kind: " + reply.kind());
            case Reject:
            case Truncated:
                // TODO (required): handle partial truncations (both within a shard e.g. pre-bootstrap, and for some shards)
                accept(null, new Preempted(txnId, route.homeKey()));
                return;

            case Ok:
                RecoverOk ok = (RecoverOk) reply;
                recoverOks.put(from, ok);
                acceptsFastPath = ok.selfAcceptsFastPath;
                break;

            case Retired:
                acceptsFastPath = true;
        }

        // TODO (expected): don't trigger recovery unless we have Q+1 responses, or 1 or more responses have
        //  no entries in WP where it is itself the coordinator. This means we can always recover immediately.
        if (tracker.recordSuccess(from, acceptsFastPath) == Success)
            recover();
    }

    private void recover()
    {
        Invariants.require(!isBallotPromised);
        isBallotPromised = true;

        SortedListMap<Id, RecoverOk> recoverOks = this.recoverOks;
        if (!Invariants.debug()) this.recoverOks = null;
        List<RecoverOk> recoverOkList = recoverOks.valuesAsNullableList();
        RecoverOk acceptOrCommit = maxAccepted(recoverOkList);
        RecoverOk acceptOrCommitNotTruncated = acceptOrCommit == null || acceptOrCommit.status != Status.Truncated
                                               ? acceptOrCommit : maxAcceptedNotTruncated(recoverOkList);

        if (acceptOrCommitNotTruncated != null)
        {
            Timestamp executeAt = acceptOrCommitNotTruncated.executeAt;
            Status status; {
                Status tmp = acceptOrCommitNotTruncated.status;
                if (committedExecuteAt != null)
                {
                    Invariants.require(acceptOrCommitNotTruncated.status.compareTo(Status.PreCommitted) < 0 || executeAt.equals(committedExecuteAt));
                    // if we know from a prior Accept attempt that this is committed we can go straight to the commit phase
                    if (tmp == AcceptedMedium || tmp == AcceptedSlow)
                        tmp = Status.Committed;
                }
                status = tmp;
            }

            switch (status)
            {
                case Truncated: throw illegalState("Truncate should be filtered");
                case Invalidated:
                {
                    if (tracing != null)
                        tracing.trace(null, "Recover found Invalidated: committing to all shards.");

                    commitInvalidate(invalidateUntil(recoverOks));
                    return;
                }

                case AcceptedInvalidate:
                {
                    if (tracing != null)
                        tracing.trace(null, "Recover found AcceptedInvalidate: continuing Invalidate.");

                    invalidate(recoverOks);
                    return;
                }

                case NotDefined:
                case PreAccepted:
                    throw illegalState("Should only be possible to have Accepted or later commands");
            }

            LatestDeps.Merge merge = mergeDeps(recoverOkList);
            Participants<?> await = merge.notAccepted(route);
            awaitPartialEarlier(recoverOkList, await, () -> {
                switch (status)
                {
                    default: throw new UnhandledEnum(status);
                    case Applied:
                    case PreApplied:
                    {
                        if (tracing != null)
                            tracing.trace(null, "Recover found Applied; persisting.");

                        withStableDeps(merge, executeAt, (i, t) -> node.agent().acceptAndWrap(i, t), stableDeps -> {
                            adapter.persist(node, executor, tracker.topologies(), route, ballot, CoordinationFlags.none(), txnId, txn, executeAt, stableDeps, acceptOrCommitNotTruncated.writes, acceptOrCommitNotTruncated.result, (i, t) -> node.agent().acceptAndWrap(i, t));
                        });
                        accept(acceptOrCommitNotTruncated.result, null);
                        return;
                    }

                    case Stable:
                    {
                        if (tracing != null)
                            tracing.trace(null, "Recover found Stable; executing.");

                        withStableDeps(merge, executeAt, this, stableDeps -> {
                            adapter.execute(node, executor, tracker.topologies(), route, ballot, RECOVER, CoordinationFlags.none(), txnId, txn, executeAt, stableDeps, stableDeps, this);
                        });
                        return;
                    }

                    case PreCommitted:
                    case Committed:
                    {
                        if (tracing != null)
                            tracing.trace(null, "Recover found Committed; stabilising.");

                        withCommittedDeps(merge, executeAt, this, committedDeps -> {
                            adapter.stabilise(node, executor, tracker.topologies(), route, ballot, txnId, txn, executeAt, committedDeps, this);
                        });
                        return;
                    }

                    case AcceptedSlow:
                    case AcceptedMedium:
                    {
                        if (tracing != null)
                            tracing.trace(null, "Recover found Accepted; re-proposing.");

                        // TODO (desired): if we have a quorum of Accept with matching ballot or proposal we can go straight to Commit
                        // TODO (desired): if we didn't find Accepted in *every* shard, consider invalidating for consistency of behaviour
                        //     however, note that we may have taken the fast path and recovered, so we can only do this if acceptedOrCommitted=Ballot.ZERO
                        //     (otherwise recovery was attempted and did not invalidate, so it must have determined it needed to complete)
                        Deps proposeDeps = merge.mergeProposal();
                        propose(SLOW, acceptOrCommitNotTruncated.executeAt, proposeDeps);
                    }
                }
            });
            return;
        }

        if (acceptOrCommit != null && acceptOrCommit != acceptOrCommitNotTruncated)
        {
            // TODO (required): match logic in Invalidate; we need to know which keys have been invalidated
            Topologies topologies = tracker.topologies();
            boolean allShardsTruncated = true;
            for (int topologyIndex = 0 ; topologyIndex < topologies.size() ; ++topologyIndex)
            {
                Topology topology = topologies.get(topologyIndex);
                for (Shard shard : topology.shards())
                {
                    RecoverOk maxReply = maxAccepted(recoverOks.select(shard.nodes));
                    allShardsTruncated &= maxReply.status == Status.Truncated;
                }
                if (allShardsTruncated)
                {
                    if (tracing != null)
                        tracing.trace(null, "Recover found all shards truncated; terminating.");
                    // TODO (required, correctness): this is not a safe inference in the case of an ErasedOrInvalidOrVestigial response.
                    //   We need to tighten up the inference and spread of truncation/invalid outcomes.
                    //   In this case, at minimum this can lead to liveness violations as the home shard stops coordinating
                    //   a command that it hasn't invalidated, but nor is it possible to recover. This happens because
                    //   when the home shard shares all of its replicas with another shard that has autonomously invalidated
                    //   the transaction, so that all received InvalidateReply show truncation (when in fact this is only partial).
                    //   We could paper over this, but better to revisit and provide stronger invariants we can rely on.
                    isDone = true;
                    callback.accept(TRUNCATED_DURABLE_OR_INVALIDATED, null);
                    return;
                }
            }
        }

        Invariants.require(committedExecuteAt == null || committedExecuteAt.equals(txnId));
        Invariants.require(!isFastPathDecided);

        boolean coordinatorInRecoveryQuorum = recoverOks.get(txnId.node) != null;
        Participants<?> extraCoordVotes = extraCoordinatorVotes(txnId, coordinatorInRecoveryQuorum, recoverOkList);
        Participants<?> extraRejects = Deps.merge(recoverOkList, recoverOkList.size(), List::get, ok -> ok.laterCoordRejects)
                                           .intersecting(route, id -> !recoverOks.containsKey(id.node));
        InferredFastPath fastPath;
        if (txnId.hasPrivilegedCoordinator() && coordinatorInRecoveryQuorum) fastPath = Reject;
        else if (txnId.isSyncPoint()) fastPath = Reject;
        else fastPath = merge(
            supersedingRejects(recoverOkList) ? Reject : Unknown,
            tracker.inferFastPathDecision(txnId, extraCoordVotes, extraRejects)
        );

        switch (fastPath)
        {
            case Reject:
            {
                if (tracing != null)
                    tracing.trace(null, "Recover found fast path rejection; invoking Invalidate.");

                invalidate(recoverOks);
                return;
            }
            case Accept:
            {
                // we still have to wait for earlier transactions to decide themselves so we don't accidentally include
                // a non-fastpath transaction in our dependencies and permit it to conclude it is safe to execute.
                // So, we fall-through to Unknown condition - though we don't in principle need to wait for any future transactions
            }
            case Unknown:
            {
                // should all be PreAccept
                Deps earlierWait = Deps.merge(recoverOkList, recoverOkList.size(), List::get, ok -> ok.earlierWait);
                Deps earlierNoWait = Deps.merge(recoverOkList, recoverOkList.size(), List::get, ok -> ok.earlierNoWait);
                earlierWait = earlierWait.without(earlierNoWait);
                Deps laterWitnessedCoordRejects = Deps.merge(recoverOks, recoverOks.domainSize(), (map, i) -> selectCoordinatorReplies(map.getKey(i), map.getValue(i)), Function.identity());

                if (!earlierWait.isEmpty() || !laterWitnessedCoordRejects.isEmpty())
                {
                    // If there exist commands that were proposed a later execution time than us that have not witnessed us,
                    // we have to be certain these commands have not successfully committed without witnessing us (thereby
                    // ruling out a fast path decision for us and changing our recovery decision).
                    // So, we wait for these commands to commit and recompute supersedingRejects for them.
                    AsyncChainCombiner.reduce(awaitEarlier(node, earlierWait, HasCommittedDeps),
                                              awaitLater(node, laterWitnessedCoordRejects, CommittedOrNotFastPathCommit, extraCoordVotes),
                                              InferredFastPath::merge)
                                      .begin((inferred, failure) -> {
                                          if (failure != null) accept(null, failure);
                                          else
                                          {
                                              switch (inferred)
                                              {
                                                  default: throw new UnhandledEnum(inferred);
                                                  case Accept:
                                                  {
                                                      if (tracing != null)
                                                          tracing.trace(null, "Recover found accepted fast path; proposing.");
                                                      propose(SLOW, txnId, recoverOkList);
                                                      break;
                                                  }
                                                  case Unknown:
                                                  {
                                                      if (tracing != null)
                                                          tracing.trace(null, "Recover found unknown fast path decision; retrying.");
                                                      retry(committedExecuteAt);
                                                      break;
                                                  }
                                                  case Reject:
                                                  {
                                                      if (tracing != null)
                                                          tracing.trace(null, "Recover found fast path rejection; invoking Invalidate.");

                                                      invalidate(recoverOks);
                                                      break;
                                                  }
                                              }
                                          }
                                      });
                }
                else
                {
                    if (tracing != null)
                        tracing.trace(null, "Recover found unknown fast path decision, but no preceding or superseding transactions awaiting decisions; proposing.");
                    propose(SLOW, txnId, recoverOkList);
                }
            }
        }
    }

    private static LatestDeps.Merge mergeDeps(List<RecoverOk> nullableRecoverOkList)
    {
        return LatestDeps.merge(nullableRecoverOkList, ok -> ok == null ? null : ok.deps);
    }

    private void awaitPartialEarlier(List<RecoverOk> nullableRecoverOkList, Participants<?> participants, Runnable whenReady)
    {
        Deps earlierWait = Deps.merge(nullableRecoverOkList, nullableRecoverOkList.size(), List::get, ok -> ok.earlierWait);
        Deps earlierNoWait = Deps.merge(nullableRecoverOkList, nullableRecoverOkList.size(), List::get, ok -> ok.earlierNoWait);
        earlierWait = earlierWait.without(earlierNoWait);
        earlierWait = earlierWait.intersecting(participants);
        awaitEarlier(node, earlierWait, HasDecidedExecuteAt).begin((success, fail) -> {
            if (fail != null) accept(null, fail);
            else whenReady.run();
        });
    }

    private static boolean supersedingRejects(List<RecoverOk> oks)
    {
        for (RecoverOk ok : oks)
        {
            if (ok != null && ok.supersedingRejects)
                return true;
        }
        return false;
    }

    private static InferredFastPath merge(InferredFastPath a, InferredFastPath b)
    {
        if (a == Unknown || b == Unknown)
            return a == Unknown ? b : a;

        // we CAN encounter both Reject AND Accept in the event we are a stale recovery attempt, and a "later" recovery
        // has already invalidated us, due to witnessing a different quorum
        // (e.g. witnessing the privileged coordinator so knew we did not take fast path, even if we could have done).
        // So, we just take the Reject unless both are Accept
        return a == b ? a : Reject;
    }

    private static Participants<?> extraCoordinatorVotes(TxnId txnId, boolean coordinatorInQuorum, List<RecoverOk> oks)
    {
        if (!txnId.hasPrivilegedCoordinator())
            return null;

        Participants<?> result = Participants.empty(txnId);
        if (coordinatorInQuorum)
            return result;

        for (RecoverOk ok : oks)
        {
            if (ok != null && ok.coordinatorAcceptsFastPath != null)
                result = Participants.merge(result, (Participants) ok.coordinatorAcceptsFastPath);
        }
        return result;
    }

    private static Deps selectCoordinatorReplies(Id from, RecoverOk ok)
    {
        if (ok == null)
            return null;

        return ok.laterCoordRejects.with(id -> from.equals(id.node));
    }

    private void withCommittedDeps(LatestDeps.Merge merge, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        LatestDeps.withCommitted(adapter, node, executor, merge, route, ballot, txnId, executeAt, txn, failureCallback, withDeps);
    }

    private void withStableDeps(LatestDeps.Merge merge, Timestamp executeAt, BiConsumer<?, Throwable> failureCallback, Consumer<Deps> withDeps)
    {
        LatestDeps.withStable(adapter, node, executor, merge, Deps.NONE, route, null, null, route, ballot, txnId, executeAt, txn, failureCallback, withDeps);
    }

    private void invalidate(SortedListMap<Id, RecoverOk> recoverOks)
    {
        Timestamp invalidateUntil = invalidateUntil(recoverOks);
        proposeInvalidate(node, executor, ballot, txnId, route.homeKey(), (success, fail) -> {
            if (fail != null) accept(null, fail);
            else commitInvalidate(invalidateUntil);
        });
    }

    private Timestamp invalidateUntil(SortedListMap<Id, RecoverOk> recoverOks)
    {
        // If not accepted then the executeAt is not consistent cross the peers and likely different on every node.  There is also an edge case
        // when ranges are removed from the topology, during this case the executeAt won't know the ranges and the invalidate commit will fail.
        return recoverOks.valuesAsNullableStream()
                         .map(ok -> ok == null ? null : ok.status.hasBeen(AcceptedMedium) ? ok.executeAt : ok.txnId)
                         .reduce(txnId, Timestamp::nonNullOrMax);
    }

    private void commitInvalidate(Timestamp invalidateUntil)
    {
        node.withEpochAtLeast(invalidateUntil.epoch(), executor, node.agent(), () -> {
            Commit.Invalidate.commitInvalidate(node, txnId, route, invalidateUntil);
        });
        isDone = true;
        locallyInvalidateAndCallback(node, txnId, reportTo.refine(txnId, null, route), route, ProgressToken.INVALIDATED, callback, null);
    }

    private void propose(Accept.Kind kind, Timestamp executeAt, List<RecoverOk> recoverOkList)
    {
        Deps proposeDeps = mergeDeps(recoverOkList).mergeProposal();
        propose(kind, executeAt, proposeDeps);
    }

    private void propose(Accept.Kind kind, Timestamp executeAt, Deps deps)
    {
        node.withEpochAtLeast(executeAt.epoch(), executor, this, () -> {
            adapter.propose(node, executor, null, route, kind, ballot, txnId, txn, executeAt, deps, this);
        });
    }

    private void retry(Timestamp executeAt)
    {
        Topologies topologies = tracker.topologies();
        if (executeAt != null && executeAt.epoch() != (this.committedExecuteAt == null ? txnId : this.committedExecuteAt).epoch())
            topologies = node.topology().select(route, txnId, executeAt, SHARE, QuorumEpochIntersections.recover);

        Ballot ballot = node.uniqueTimestamp(Ballot::fromValues);
        Tracing tracing = node.agent().trace(txnId, TraceEventType.RECOVER);
        Recover.recover(node, topologies, ballot, txnId, txn, route, executeAt, isFastPathDecided, reportTo, callback, tracing);
    }

    AsyncResult<InferredFastPath> awaitEarlier(Node node, Deps waitOn, BlockedUntil blockedUntil)
    {
        if (tracing != null)
            tracing.trace(null, "Recover awaiting earlier decisions: " + waitOn.txnIds());

        long requireEpoch = waitOn.maxTxnId(txnId).epoch();
        return node.withEpochAtLeast(requireEpoch, executor, () -> {
            TxnId recoverId = this.txnId;
            List<AsyncResult<InferredFastPath>> requests = new ArrayList<>();
            for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
            {
                TxnId awaitId = waitOn.txnId(i);
                Invariants.require(awaitId.compareTo(recoverId) < 0);
                Participants<?> participants = waitOn.participants(awaitId);

                Topologies topologies;
                if (tracker.topologies().containsEpoch(awaitId.epoch())) topologies = tracker.topologies().selectEpoch(participants, awaitId.epoch(), SHARE);
                else topologies = node.topology().forEpochAtLeast(participants, awaitId.epoch(), SHARE);
                requests.add(SynchronousRecoverAwait.awaitAny(node, executor, topologies, awaitId, blockedUntil, true, participants, recoverId));
            }
            if (requests.isEmpty())
                return AsyncResults.success(InferredFastPath.Accept);
            return AsyncChainCombiner.reduce(requests, InferredFastPath::merge).beginAsResult();
        }).beginAsResult();
    }

    AsyncResult<InferredFastPath> awaitLater(Node node, Deps waitOn, BlockedUntil blockedUntil, @Nullable Participants<?> selfCoordVotes)
    {
        if (tracing != null)
            tracing.trace(null, "Recover awaiting later decisions or recoveries: " + waitOn.txnIds());

        if (waitOn.isEmpty())
            return AsyncResults.success(InferredFastPath.Accept);

        Participants<?> reliesOnAwaitIdCoordVote;
        Topology topology = tracker.topologies().current();
        switch (route.domain())
        {
            default: throw new UnhandledEnum(route.domain());
            case Key:
                try (BufferList<RoutingKey> tmp = new BufferList<>())
                {
                    for (int j = 0 ; j < route.size() ; ++j)
                    {
                        RoutingKey key = (RoutingKey)route.get(j);
                        RecoveryTracker.RecoveryShardTracker shardTracker = tracker.get(0, topology.indexForKey(key));
                        if (shardTracker.fastPathReliesOnUnwitnessedCoordinatorVote(txnId, selfCoordVotes))
                            tmp.add(key);
                    }
                    reliesOnAwaitIdCoordVote = RoutingKeys.ofSortedUnique(tmp);
                }
                break;
            case Range:
                try (BufferList<Range> tmp = new BufferList<>())
                {
                    for (int j = 0 ; j < route.size() ; ++j)
                    {
                        Range range = (Range)route.get(j);
                        for (int k = topology.indexForRange(range, CEIL), maxk = topology.indexForRange(range, FLOOR); k <= maxk ; k++)
                        {
                            RecoveryTracker.RecoveryShardTracker shardTracker = tracker.get(0, k);
                            if (shardTracker.fastPathReliesOnUnwitnessedCoordinatorVote(txnId, selfCoordVotes))
                                tmp.add(range.slice(shardTracker.shard.range));
                        }
                    }
                    reliesOnAwaitIdCoordVote = Ranges.ofSortedAndDeoverlapped(tmp.toArray(Range[]::new));
                }
        }

        if (reliesOnAwaitIdCoordVote.isEmpty())
            return AsyncResults.success(InferredFastPath.Accept);

        long requireEpoch = waitOn.maxTxnId(txnId).epoch();
        return node.withEpochExact(requireEpoch, executor, () -> {
            TxnId recoverId = this.txnId;
            List<AsyncResult<InferredFastPath>> requests = new ArrayList<>();
            for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
            {
                TxnId awaitId = waitOn.txnId(i);
                Invariants.require(awaitId.is(PrivilegedCoordinatorWithDeps));
                Invariants.require(awaitId.compareTo(recoverId) > 0);
                Participants<?> participants = waitOn.participants(awaitId)
                                                     .intersecting(reliesOnAwaitIdCoordVote, Minimal);
                if (participants.isEmpty())
                    continue;

                Topologies topologies;
                if (tracker.topologies().containsEpoch(awaitId.epoch())) topologies = tracker.topologies().selectEpoch(participants, awaitId.epoch(), SHARE);
                else topologies = node.topology().forEpoch(participants, awaitId.epoch(), SHARE);
                requests.add(SynchronousRecoverAwait.awaitAny(node, executor, topologies, awaitId, blockedUntil, true, participants, recoverId));
            }
            if (requests.isEmpty())
                return AsyncResults.success(InferredFastPath.Accept);
            return AsyncChainCombiner.reduce(requests, InferredFastPath::merge).beginAsResult();
        }).beginAsResult();
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.recordFailure(from) == Failed)
            accept(null, new Timeout(txnId, route.homeKey()));
    }

    @Override
    public boolean onCallbackFailure(Id from, Throwable failure)
    {
        if (isDone) return false;
        accept(null, failure);
        return true;
    }
}
