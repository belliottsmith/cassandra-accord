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

import accord.api.RoutingKey;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.GetLatestDeps;
import accord.messages.GetLatestDeps.GetLatestDepsOk;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListMap;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

public class CollectLatestDeps implements Callback<GetLatestDepsOk>
{
    final Node node;
    final TxnId txnId;
    final RoutingKey homeKey;
    final Timestamp executeAt;

    private final SortedListMap<Id, GetLatestDepsOk> oks;
    private final QuorumTracker tracker;
    private final BiConsumer<List<LatestDeps>, Throwable> callback;
    private boolean isDone;

    CollectLatestDeps(Node node, Topologies topologies, TxnId txnId, RoutingKey homeKey, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.callback = callback;
        this.oks = new SortedListMap<>(topologies.nodes(), GetLatestDepsOk[]::new);
        this.tracker = new QuorumTracker(topologies);
    }

    public static void withLatestDeps(Node node, TxnId txnId, FullRoute<?> fullRoute, Unseekables<?> collectFrom, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncedEpochs(collectFrom, txnId, executeAt);
        CollectLatestDeps collect = new CollectLatestDeps(node, topologies, txnId, fullRoute.homeKey(), executeAt, callback);
        CommandStore store = CommandStore.currentOrElseSelect(node, fullRoute);

        SortedArrayList<Id> contact = collect.tracker.filterAndRecordFaulty();
        if (contact == null) callback.accept(null, new Exhausted(txnId, collect.homeKey, null));
        else node.send(contact, to -> new GetLatestDeps(to, topologies, fullRoute, txnId, executeAt), store, collect);
    }

    @Override
    public void onSuccess(Id from, GetLatestDepsOk ok)
    {
        if (isDone)
            return;

        oks.put(from, ok);
        if (tracker.recordSuccess(from) == Success)
            onQuorum();
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.recordFailure(from) == Failed)
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, homeKey));
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
