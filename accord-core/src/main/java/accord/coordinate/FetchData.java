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

import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import accord.coordinate.Infer.InvalidIf;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.CommandStores.StoreSelector;
import accord.local.Node;
import accord.primitives.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.Propagate;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;

/**
 * Find data and persist locally
 *
 * TODO (expected): avoid multiple command stores performing duplicate queries to other shards
 */
public class FetchData extends CheckShards<Route<?>>
{
    public static class FetchResult
    {
        public final @Nonnull Known achieved;
        public final Unseekables<?> achievedTarget;
        public final @Nullable Unseekables<?> didNotAchieveTarget;

        public FetchResult(Known achieved, Unseekables<?> achievedTarget, @Nullable Unseekables<?> didNotAchieveTarget)
        {
            this.achieved = Invariants.nonNull(achieved);
            this.achievedTarget = achievedTarget;
            this.didNotAchieveTarget = didNotAchieveTarget == null || didNotAchieveTarget.isEmpty() ? null : didNotAchieveTarget;
        }
    }

    // TODO (expected): separate keys we fetch deps and txns for
    public static class FetchRequest
    {
        final Known fetch;
        final TxnId txnId;
        final InvalidIf invalidIf;
        final @Nullable Timestamp executeAt;
        final long srcEpoch;
        // known participants, a subset of which we may fetch from
        final Participants<?> contactable;
        final Participants<?> requestedFor;
        final StoreSelector reportTo;
        final BiConsumer<? super FetchResult, Throwable> callback;

        public FetchRequest(Known fetch, TxnId txnId, InvalidIf invalidIf, @Nullable Timestamp executeAt, Participants<?> contactable, Participants<?> requestedFor, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
        {
            this.fetch = fetch;
            this.invalidIf = invalidIf;
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.callback = callback;
            this.srcEpoch = fetch.fetchEpoch(txnId, executeAt);
            this.contactable = contactable;
            this.requestedFor = requestedFor;
            this.reportTo = reportTo;
        }
    }

    /**
     * Do not make an attempt to discern what keys need to be contacted; fetch from only the specific remote keys that were requested.
     */
    public static void fetchSpecific(Known fetch, Node node, TxnId txnId, @Nullable Timestamp executeAt, Route<?> query, Route<?> maxRoute, Participants<?> requestedFor, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        fetchSpecific(fetch, node, txnId, NotKnownToBeInvalid, executeAt, query, maxRoute, requestedFor, reportTo, callback);
    }

    /**
     * Do not make an attempt to discern what keys need to be contacted; fetch from only the specific remote keys that were requested.
     */
    public static void fetchSpecific(Known fetch, Node node, TxnId txnId, InvalidIf invalidIf, @Nullable Timestamp executeAt, Route<?> query, Route<?> maxRoute, Participants<?> requestedFor, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        fetchSpecific(node, query, maxRoute, new FetchRequest(fetch, txnId, invalidIf, executeAt, maxRoute, requestedFor, reportTo, callback));
    }

    public static void fetchSpecific(Node node, Route<?> query, Route<?> maxRoute, FetchRequest request)
    {
        long srcEpoch = request.srcEpoch;
        if (!node.topology().hasAtLeastEpoch(srcEpoch))
        {
            node.withEpochAtLeast(srcEpoch, request.callback, () -> fetchSpecific(node, query, maxRoute, request));
            return;
        }

        fetchData(node, query, maxRoute, request);
    }

    final BiConsumer<? super FetchResult, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known target;
    final Route<?> maxRoute;
    final Participants<?> requestedFor;

    // to support cases where a later epoch that ultimately does not participate in execution has a vestigial entry
    // (i.e. if preaccept/accept contact a later epoch than execution is decided for)
    final LatentStoreSelector reportTo;

    private FetchData(Node node, Known target, TxnId txnId, InvalidIf invalidIf, Route<?> route, Route<?> maxRoute, Participants<?> requestedFor, long sourceEpoch, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        this(node, target, txnId, invalidIf, route, route.withHomeKey(), maxRoute, requestedFor, sourceEpoch, reportTo, callback);
    }

    private FetchData(Node node, Known target, TxnId txnId, InvalidIf invalidIf, Route<?> route, Route<?> routeWithHomeKey, Route<?> maxRoute, Participants<?> requestedFor, long sourceEpoch, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, sourceEpoch, CheckStatus.IncludeInfo.All, null, invalidIf);
        this.reportTo = reportTo;
        this.maxRoute = maxRoute;
        this.requestedFor = requestedFor;
        Invariants.requireArgument(routeWithHomeKey.contains(route.homeKey()), "route %s does not contain %s", routeWithHomeKey, route.homeKey());
        this.target = target;
        this.callback = callback;
    }

    private static FetchData fetchData(Node node, Route<?> route, Route<?> maxRoute, FetchRequest req)
    {
        Invariants.require(!req.contactable.isEmpty());
        FetchData fetch = new FetchData(node, req.fetch, req.txnId, req.invalidIf, route, maxRoute, req.requestedFor, req.srcEpoch, req.reportTo, req.callback);
        fetch.start();
        return fetch;
    }

    private static FetchData fetchData(Node node, Known fetch, TxnId txnId, InvalidIf invalidIf, Route<?> route, Route<?> maxRoute, Participants<?> requestedFor, long sourceEpoch, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        FetchData fetchData = new FetchData(node, fetch, txnId, invalidIf, route, maxRoute, requestedFor, sourceEpoch, reportTo, callback);
        fetchData.start();
        return fetchData;
    }

    protected Route<?> query()
    {
        return query;
    }

    @Override
    protected boolean isSufficient(Node.Id from, CheckStatus.CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().computeRangesForNode(from);
        Route<?> scope = this.query.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatus.CheckStatusOk ok)
    {
        return isSufficient(query, ok);
    }

    protected boolean isSufficient(Route<?> scope, CheckStatus.CheckStatusOk ok)
    {
        return target.isSatisfiedBy(ok.knownFor(txnId, scope.participants(), scope.participants()));
    }

    @Override
    protected void onDone(ReadCoordinator.Success success, Throwable failure)
    {
        Invariants.require((success == null) != (failure == null));
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            if (success == ReadCoordinator.Success.Success)
                Invariants.require(isSufficient(merged), "Status %s is not sufficient", merged);

            // TODO (expected): should we automatically trigger a new fetch if we find executeAt but did not request enough information? would be more robust
            Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query(), requestedFor, reportTo, target, (CheckStatusOkFull) merged, callback);
        }
    }
}
