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
import java.util.List;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.GetLatestDeps;
import accord.messages.GetLatestDeps.GetLatestDepsOk;
import accord.messages.GetLatestDeps.GetLatestDepsReply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListMap;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.Routables.Slice.Minimal;

public class CollectLatestDeps implements Callback<GetLatestDepsReply>
{
    final Node node;
    final TxnId txnId;
    final Route<?> route;
    final Timestamp executeAt;
    final @Nullable Ballot ballot;

    private final SortedListMap<Id, GetLatestDepsOk> oks;
    private final QuorumTracker tracker;
    private final BiConsumer<List<LatestDeps>, Throwable> callback;
    private boolean isDone;
    private Throwable failure;

    CollectLatestDeps(Node node, Topologies topologies, TxnId txnId, Route<?> route, @Nullable Ballot ballot, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.executeAt = executeAt;
        this.ballot = ballot;
        this.callback = callback;
        this.oks = new SortedListMap<>(topologies.nodes(), GetLatestDepsOk[]::new);
        this.tracker = new QuorumTracker(topologies);
    }

    public static void withLatestDeps(Node node, TxnId txnId, FullRoute<?> fullRoute, Unseekables<?> collectFrom, @Nullable Ballot ballot, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        Route<?> route = fullRoute.intersecting(collectFrom, Minimal);
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        CollectLatestDeps collect = new CollectLatestDeps(node, topologies, txnId, route, ballot, executeAt, callback);
        CommandStore store = CommandStore.currentOrElseSelect(node, fullRoute);

        SortedArrayList<Id> contact = collect.tracker.filterAndRecordFaulty();
        if (contact == null) callback.accept(null, new Exhausted(txnId, route.homeKey(), null));
        else node.send(contact, to -> new GetLatestDeps(to, topologies, route, txnId, ballot, executeAt), store, collect);
    }

    @Override
    public void onSuccess(Id from, GetLatestDepsReply ok)
    {
        if (isDone)
            return;

        if (ok.isOk())
        {
            oks.put(from, (GetLatestDepsOk) ok);
            if (tracker.recordSuccess(from) == Success)
                onQuorum();
        }
        else
        {
            onFailure(from, null);
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (failure != null)
            this.failure = FailureAccumulator.append(this.failure, failure);

        if (tracker.recordFailure(from) == Failed)
        {
            isDone = true;
            if (this.failure == null)
                this.failure = new Exhausted(txnId, route.homeKey(), null);
            callback.accept(null, this.failure);
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

    private void onQuorum()
    {
        if (isDone)
            return;

        isDone = true;
        List<LatestDeps> result = new ArrayList<>(oks.size());
        for (GetLatestDepsOk ok : oks.values())
            result.add(ok.deps);
        callback.accept(result, null);
    }
}
