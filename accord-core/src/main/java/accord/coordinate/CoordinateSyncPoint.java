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

import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.CoordinationAdapter.Adapters.SyncPointAdapter;
import accord.local.Node;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.Propose.NotAccept.proposeAndCommitInvalidate;
import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.participates;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint<R> extends CoordinatePreAccept<R>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinateSyncPoint.class);

    final CoordinationAdapter<R> adapter;

    private CoordinateSyncPoint(Node node, TxnId txnId, Topologies topologies, Txn txn, FullRoute<?> route, SyncPointAdapter<R> adapter)
    {
        super(node, txnId, txn, route, topologies, adapter.preacceptTrackerFactory);
        this.adapter = adapter;
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> exclusiveSyncPoint(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> exclusiveSyncPoint(Node node, TxnId txnId, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, txnId, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> exclusiveSyncPoint(Node node, TxnId txnId, FullRoute<U> route)
    {
        return coordinate(node, txnId, route, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusive(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, Adapters.inclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusive(Node node, FullRoute<U> route)
    {
        return coordinate(node, Kind.SyncPoint, route, Adapters.inclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusive(Node node, TxnId txnId, FullRoute<U> route)
    {
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, route, Adapters.inclusiveSyncPoint())).beginAsResult();
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusiveAndAwaitQuorum(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, Adapters.inclusiveSyncPointBlocking());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusiveAndAwaitQuorum(Node node, FullRoute<U> route)
    {
        return coordinate(node, Kind.SyncPoint, route, Adapters.inclusiveSyncPointBlocking());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusiveAndAwaitQuorum(Node node, TxnId txnId, FullRoute<U> route)
    {
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, route, Adapters.inclusiveSyncPointBlocking())).beginAsResult();
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, Kind kind, Unseekables<U> keysOrRanges, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnId(kind, keysOrRanges.domain());
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, keysOrRanges, adapter)).beginAsResult();
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, Kind kind, FullRoute<U> route, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnId(kind, route.domain());
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, route, adapter)).beginAsResult();
    }

    private static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, TxnId txnId, Unseekables<U> keysOrRanges, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(txnId.isSyncPoint());
        FullRoute<U> route = (FullRoute<U>) node.computeRoute(txnId, keysOrRanges);
        return coordinate(node, txnId, route, adapter);
    }

    private static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, TxnId txnId, FullRoute<U> route, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(txnId.isSyncPoint());
        TopologyMismatch mismatch = txnId.kind() == ExclusiveSyncPoint
                                    ? TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), route)
                                    : TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), route);
        if (mismatch != null)
            return AsyncResults.failure(mismatch);
        CoordinateSyncPoint<SyncPoint<U>> coordinate = new CoordinateSyncPoint<>(node, txnId, adapter.forDecision(node, route, txnId), node.agent().emptySystemTxn(txnId.kind(), txnId.domain()), route, adapter);
        coordinate.start();
        return coordinate;
    }

    @Override
    long executeAtEpoch()
    {
        return txnId.epoch();
    }

    @Override
    void onPreAccepted(Topologies topologies, Timestamp executeAt, SortedListMap<Node.Id, PreAcceptOk> oks)
    {
        if (executeAt.is(REJECTED))
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt, this);
        }
        else
        {
            TxnId withFlags = txnId;
            if (txnId.is(ExclusiveSyncPoint) && txnId.epoch() == executeAt.epoch())
                withFlags = txnId.addFlag(HLC_BOUND);
            Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
            if (tracker.hasFastPathAccepted())
                adapter.execute(node, topologies, route, FAST, txnId, txn, withFlags, deps, this);
            else if (tracker.hasMediumPathAccepted())
                adapter.propose(node, topologies, route, Accept.Kind.MEDIUM, Ballot.ZERO, txnId, txn, withFlags, deps, this);
            else
                adapter.propose(node, topologies, route, Accept.Kind.SLOW, Ballot.ZERO, txnId, txn, executeAt, deps, this);
        }
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint)
    {
        // TODO (expected): consider, document and add invariants checking if this topologies is correct in all cases
        //  (notably ExclusiveSyncPoints should execute in earlier epochs for durability, but not for fetching)
        sendApply(node, to, syncPoint, participates(node, syncPoint.route, syncPoint.syncId, syncPoint.executeAt));
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, Topologies participates)
    {
        sendApply(node, to, syncPoint, participates, null);
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, Topologies participates, @Nullable Callback<Apply.ApplyReply> callback)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = syncPoint.executeAt;
        Txn txn = node.agent().emptySystemTxn(txnId.kind(), txnId.domain());
        Deps deps = syncPoint.waitFor;
        FullRoute<?> route = syncPoint.route;
        Result result = txn.result(txnId, executeAt, null);
        Apply apply = Apply.FACTORY.create(Maximal, to, participates, txnId, route, txn, executeAt, deps, null, result, route);
        if (callback == null) node.send(to, apply);
        else node.send(to, apply, callback);
    }
}
