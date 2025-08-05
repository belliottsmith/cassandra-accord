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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.api.AsyncExecutor;
import accord.api.Result;
import accord.coordinate.tracking.DurabilityTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.SetShardDurable;
import accord.primitives.Range;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListMap;
import accord.utils.UnhandledEnum;
import accord.utils.WrappableException;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.coordinate.CoordinationAdapter.Adapters.exclusiveSyncPoint;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

public class ExecuteSyncPoint extends SettableResult<DurabilityResult> implements Callback<ReadReply>
{
    public static class SyncPointErased extends Throwable implements WrappableException<SyncPointErased>
    {
        public SyncPointErased() {}
        public SyncPointErased(Throwable cause) { super(cause); }
        @Override public SyncPointErased wrap() { return new SyncPointErased(this); }
    }

    final Node node;
    final SyncPoint<Range> syncPoint;

    final DurabilityResult partialResult;
    final Set<Node.Id> excludeSuccess;
    final DurabilityTracker tracker;
    final @Nullable AsyncExecutor executor;
    final Map<Node.Id, Object> debug;
    final SettableResult<DurabilityResult> onQuorum = new SettableResult<>();
    final int attempt;
    private Throwable failures;
    boolean reportedQuorum, reportedMinorityQuorum;
    long retryInFutureEpoch;

    protected ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Set<Node.Id> excludeSuccess, AsyncExecutor executor, int attempt)
    {
        this(node, syncPoint, exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), excludeSuccess, executor, attempt, null);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Function<Topologies, Set<Node.Id>> excludeSuccess, AsyncExecutor executor, int attempt)
    {
        this(node, syncPoint, exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), excludeSuccess, executor, attempt);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Topologies topologies, Function<Topologies, Set<Node.Id>> excludeSuccess, AsyncExecutor executor, int attempt)
    {
        this(node, syncPoint, topologies, excludeSuccess.apply(topologies), executor, attempt, null);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Topologies topologies, Set<Node.Id> excludeSuccess, AsyncExecutor executor, int attempt, DurabilityResult partialResult)
    {
        this.node = node;
        this.syncPoint = syncPoint;
        this.partialResult = partialResult;
        this.excludeSuccess = excludeSuccess;
        this.attempt = attempt;
        this.tracker = new DurabilityTracker(topologies, excludeSuccess);
        this.debug = Invariants.debug() ? new SortedListMap<>(tracker.nodes(), Object[]::new) : null;
        this.executor = executor;
    }

    public AsyncResult<DurabilityResult> onQuorum()
    {
        return onQuorum;
    }

    protected void start()
    {
        node.agent().coordinatorEvents().onExecuting(syncPoint.syncId, null, syncPoint.waitFor, null);
        SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        // TODO (desired): special Apply message that doesn't resend deps if path=MEDIUM
        Txn txn = node.agent().emptySystemTxn(syncPoint.syncId.kind(), syncPoint.syncId.domain());
        Result result = txn.result(syncPoint.syncId, syncPoint.executeAt, null);
        if (contact == null) tryFailure(new Exhausted(syncPoint.syncId, syncPoint.route.homeKey(), null));
        else node.send(contact, to -> new ApplyThenWaitUntilApplied(to, tracker.topologies(), syncPoint.executeAt, tracker.topologies().currentEpoch(), syncPoint.route, syncPoint.syncId, txn, syncPoint.waitFor, syncPoint.route, null, result), executor, this);
    }

    @Override
    public void onSuccess(Node.Id from, ReadReply reply)
    {
        if (isDone()) return;
        if (debug != null)
            debug.put(from, reply);

        if (reply instanceof ReadData.ReadOkWithFutureEpoch)
            retryInFutureEpoch = Math.max(retryInFutureEpoch, ((ReadData.ReadOkWithFutureEpoch) reply).futureEpoch);

        if (!reply.isOk())
        {
            switch ((CommitOrReadNack)reply)
            {
                default: throw new UnhandledEnum((CommitOrReadNack)reply);

                case Insufficient:
                    sendApply(from);
                    return;

                case Redundant:
                    tryFailure(new SyncPointErased());
                    return;

                case Waiting:
            }
        }
        else
        {
            update(tracker.recordSuccess(from));
        }
    }

    @Override
    public boolean trySuccess(DurabilityResult success)
    {
        onQuorum.trySuccess(success);
        return super.trySuccess(success);
    }

    @Override
    public boolean tryFailure(Throwable failure)
    {
        onQuorum.tryFailure(failure);
        return super.tryFailure(failure);
    }

    protected void sendApply(Node.Id to)
    {
        CoordinateSyncPoint.sendApply(node, to, syncPoint, tracker.topologies());
    }

    @Override
    public void onFailure(Node.Id from, Throwable failure)
    {
        if (isDone()) return;
        if (debug != null)
            debug.put(from, failure);

        failures = FailureAccumulator.append(failures, failure);
        update(tracker.recordFailure(from));
    }

    private void maybeReportQuorums()
    {
        if (!reportedQuorum)
        {
            if (tracker.hasQuorumSuccess())
            {
                DurabilityResult current = current();
                onQuorum.trySuccess(current);
                node.durability().report(current);
                reportedQuorum = true;
            }
            else if (!reportedMinorityQuorum && tracker.hasMinorityQuorumSuccess())
            {
                DurabilityResult current = current();
                node.durability().report(current);
                reportedMinorityQuorum = true;
            }
        }
    }

    private void update(RequestStatus status)
    {
        if (status == RequestStatus.NoChange)
        {
            maybeReportQuorums();
            return;
        }

        Collection<Node.Id> failedNodes = tracker.failures();
        if (status == RequestStatus.Failed)
            this.failures = FailureAccumulator.createFailure(failures, syncPoint.syncId, syncPoint.route.homeKey(), syncPoint.route.toRanges(), failedNodes);

        if (retryInFutureEpoch > tracker.topologies().currentEpoch())
        {
            node.withEpochAtLeast(retryInFutureEpoch, executor, (ignore, failure) -> tryFailure(WrappableException.wrap(failure)), () -> {
                ExecuteSyncPoint continuation = new ExecuteSyncPoint(node, syncPoint, node.topology().preciseEpochs(syncPoint.route(), tracker.topologies().currentEpoch(), retryInFutureEpoch, SHARE), excludeSuccess, executor, attempt, current());
                continuation.invoke((success, failure) -> {
                    if (failure == null) trySuccess(success);
                    else tryFailure(failure);
                });
                continuation.start();
            });
        }
        else
        {
            DurabilityResult result = current();
            if (result.achievedRemote == SyncRemote.All)
            {
                node.configService().reportEpochRetired(syncPoint.route.toRanges(), syncPoint.syncId.epoch() - 1);
                node.send(tracker.nodes(), new SetShardDurable(syncPoint, Universal));
            }
            else if (result.achievedRemote == SyncRemote.Quorum)
            {
                node.send(tracker.nodes(), new SetShardDurable(syncPoint, Quorum));
            }
            else
            {
                maybeReportQuorums();
            }
            node.durability().report(result);
            trySuccess(result);
        }
    }

    @Override
    public boolean onCallbackFailure(Node.Id from, Throwable failure)
    {
        return tryFailure(failure);
    }

    DurabilityResult current()
    {
        DurabilityResult cur = new DurabilityResult(syncPoint, tracker.achievedLocal(node.id()), tracker.achievedRemote(), tracker.failures(), this.failures);
        if (partialResult == null)
            return cur;
        return partialResult.merge(cur);
    }

    public static ExecuteSyncPoint coordinate(Node node, SyncPoint<Range> exclusiveSyncPoint, int attempt)
    {
        return coordinate(node, ignore -> Collections.emptySet(), exclusiveSyncPoint, attempt);
    }

    public static ExecuteSyncPoint coordinateIncluding(Node node, SyncPoint<Range> exclusiveSyncPoint, @Nullable Collection<Node.Id> including, int attempt)
    {
        return coordinateIncluding(node, exclusiveSyncPoint, including, null, attempt);
    }

    public static ExecuteSyncPoint coordinateIncluding(Node node, SyncPoint<Range> exclusiveSyncPoint, @Nullable Collection<Node.Id> including, AsyncExecutor executor, int attempt)
    {
        return coordinate(node, including == null ? ignore -> Collections.emptySet() : topologies -> topologies.nodes().without(including::contains), exclusiveSyncPoint, executor, attempt);
    }

    public static ExecuteSyncPoint coordinate(Node node, Function<Topologies, Set<Node.Id>> excludeSuccess, SyncPoint<Range> exclusiveSyncPoint, int attempt)
    {
        return coordinate(node, excludeSuccess, exclusiveSyncPoint, null, attempt);
    }

    public static ExecuteSyncPoint coordinate(Node node, Function<Topologies, Set<Node.Id>> excludeSuccess, SyncPoint<Range> syncPoint, AsyncExecutor executor, int attempt)
    {
        try
        {
            ExecuteSyncPoint coordinate = new ExecuteSyncPoint(node, syncPoint, excludeSuccess, executor, attempt);
            coordinate.start();
            return coordinate;
        }
        catch (Throwable t)
        {
            ExecuteSyncPoint fail = new ExecuteSyncPoint(node, syncPoint, new Topologies.Single(node.topology().sorter(), node.topology().current()), excludeSuccess, executor, attempt);
            fail.tryFailure(t);
            fail.onQuorum.tryFailure(t);
            return fail;
        }
    }
}
