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

import accord.coordinate.tracking.DurabilityTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.SetShardDurable;
import accord.messages.WaitUntilApplied;
import accord.primitives.Range;
import accord.primitives.SyncPoint;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListMap;
import accord.utils.UnhandledEnum;
import accord.utils.WrappableException;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.coordinate.CoordinationAdapter.Adapters.exclusiveSyncPoint;
import static accord.primitives.Status.Durability.Majority;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
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
    final @Nullable AgentExecutor executor;
    final Map<Node.Id, Object> debug;
    final int attempt;
    private Throwable failures = null;
    boolean reportedQuorum;
    long retryInFutureEpoch;

    protected ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Set<Node.Id> excludeSuccess, AgentExecutor executor, int attempt)
    {
        this(node, syncPoint, exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), excludeSuccess, executor, attempt, null);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Function<Topologies, Set<Node.Id>> excludeSuccess, AgentExecutor executor, int attempt)
    {
        this(node, syncPoint, exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor), excludeSuccess, executor, attempt);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Topologies topologies, Function<Topologies, Set<Node.Id>> excludeSuccess, AgentExecutor executor, int attempt)
    {
        this(node, syncPoint, topologies, excludeSuccess.apply(topologies), executor, attempt, null);
    }

    ExecuteSyncPoint(Node node, SyncPoint<Range> syncPoint, Topologies topologies, Set<Node.Id> excludeSuccess, AgentExecutor executor, int attempt, DurabilityResult partialResult)
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

    private void update(RequestStatus status)
    {
        if (status == RequestStatus.NoChange)
        {
            if (tracker.hasQuorumSuccess() && !reportedQuorum)
            {
                reportedQuorum = true;
                node.durability().report(current());
            }
            return;
        }

        Collection<Node.Id> failedNodes = tracker.failures();
        if (status == RequestStatus.Failed)
            this.failures = FailureAccumulator.createFailure(failures, syncPoint.syncId, syncPoint.route.homeKey(), syncPoint.route.toRanges(), failedNodes);

        if (retryInFutureEpoch > tracker.topologies().currentEpoch())
        {
            node.withEpoch(retryInFutureEpoch, (ignore, failure) -> tryFailure(WrappableException.wrap(failure)), () -> {
                ExecuteSyncPoint continuation = new ExecuteSyncPoint(node, syncPoint, node.topology().preciseEpochs(syncPoint.route(), tracker.topologies().currentEpoch(), retryInFutureEpoch, SHARE), excludeSuccess, executor, attempt, current());
                continuation.addCallback((success, failure) -> {
                    if (failure == null) trySuccess(success);
                    else tryFailure(failure);
                });
                continuation.start();
            });
        }
        else
        {
            DurabilityResult result = new DurabilityResult(syncPoint, tracker.achievedLocal(node.id()), tracker.achievedRemote(), tracker.failures(), null);
            if (result.achievedRemote == SyncRemote.All)
            {
                node.configService().reportEpochRetired(syncPoint.route.toRanges(), syncPoint.syncId.epoch() - 1);
                node.send(tracker.nodes(), new SetShardDurable(syncPoint, UniversalOrInvalidated));
            }
            else if (result.achievedRemote == SyncRemote.Quorum)
            {
                node.send(tracker.nodes(), new SetShardDurable(syncPoint, Majority));
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

    public static AsyncResult<DurabilityResult> coordinate(Node node, SyncPoint<Range> exclusiveSyncPoint, int attempt)
    {
        return coordinate(node, ignore -> Collections.emptySet(), exclusiveSyncPoint, attempt);
    }

    public static AsyncResult<DurabilityResult> coordinateIncluding(Node node, SyncPoint<Range> exclusiveSyncPoint, @Nullable Collection<Node.Id> including, int attempt)
    {
        return coordinateIncluding(node, exclusiveSyncPoint, including, null, attempt);
    }

    public static AsyncResult<DurabilityResult> coordinateIncluding(Node node, SyncPoint<Range> exclusiveSyncPoint, @Nullable Collection<Node.Id> including, AgentExecutor executor, int attempt)
    {
        return coordinate(node, including == null ? ignore -> Collections.emptySet() : topologies -> topologies.nodes().without(including::contains), exclusiveSyncPoint, executor, attempt);
    }

    public static AsyncResult<DurabilityResult> coordinate(Node node, Function<Topologies, Set<Node.Id>> excludeSuccess, SyncPoint<Range> exclusiveSyncPoint, int attempt)
    {
        return coordinate(node, excludeSuccess, exclusiveSyncPoint, null, attempt);
    }

    public static AsyncResult<DurabilityResult> coordinate(Node node, Function<Topologies, Set<Node.Id>> excludeSuccess, SyncPoint<Range> exclusiveSyncPoint, AgentExecutor executor, int attempt)
    {
        try
        {
            ExecuteSyncPoint coordinate = new ExecuteSyncPoint(node, exclusiveSyncPoint, excludeSuccess, executor, attempt);
            coordinate.start();
            return coordinate;
        }
        catch (Throwable t)
        {
            return AsyncResults.failure(t);
        }
    }

    protected void start()
    {
        SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null) tryFailure(new Exhausted(syncPoint.syncId, syncPoint.route.homeKey(), null));
        else node.send(contact, to -> new WaitUntilApplied(to, tracker.topologies(), syncPoint.syncId, syncPoint.route, syncPoint.syncId.epoch()), executor, this);
    }
}
