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

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.local.Status.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.WithQuorum;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import javax.annotation.Nullable;

import static accord.coordinate.Infer.InvalidIfNot.NotKnownToBeInvalid;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Phase.Cleanup;
import static accord.local.Status.PreApplied;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.messages.CheckStatus.WithQuorum.NoQuorum;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

/**
 * Find data and persist locally
 *
 * TODO (expected): avoid multiple command stores performing duplicate queries to other shards
 */
public class FetchData extends CheckShards<Route<?>>
{
    public static Object fetch(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, BiConsumer<? super Known, Throwable> callback)
    {
        return fetch(fetch, node, txnId, someUnseekables, null, null, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        if (someUnseekables.kind().isRoute()) return fetch(fetch, node, txnId, castToRoute(someUnseekables), forLocalEpoch, executeAt, callback);
        else return fetchViaSomeRoute(fetch, node, txnId, someUnseekables, forLocalEpoch, executeAt, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Route<?> route, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        if (!node.topology().hasEpoch(srcEpoch))
            return node.topology().awaitEpoch(srcEpoch).map(ignore -> fetch(fetch, node, txnId, route, forLocalEpoch, executeAt, callback)).beginAsResult();

        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        long toLocalEpoch = Math.max(srcEpoch, forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
        Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), toLocalEpoch);
        if (!route.covers(ranges))
        {
            return fetchWithIncompleteRoute(fetch, node, txnId, route, forLocalEpoch, executeAt, callback);
        }
        else
        {
            return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, callback);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object fetchViaSomeRoute(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        return FindSomeRoute.findSomeRoute(node, txnId, someUnseekables, (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute.route == null)
            {
                reportRouteNotFound(node, txnId, someUnseekables, executeAt, foundRoute.known, callback);
            }
            else if (isRoute(someUnseekables) && someUnseekables.containsAll(foundRoute.route))
            {
                // this is essentially a reentrancy check; we can only reach this point if we have already tried once to fetchSomeRoute
                // (as a user-provided Route is used to fetchRoute, not fetchSomeRoute)
                reportRouteNotFound(node, txnId, someUnseekables, executeAt, foundRoute.known, callback);
            }
            else
            {
                Route<?> route = foundRoute.route;
                if (isRoute(someUnseekables))
                    route = Route.merge(route, (Route)someUnseekables);
                fetch(fetch, node, txnId, route, forLocalEpoch, executeAt, callback);
            }
        });
    }

    private static void reportRouteNotFound(Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable Timestamp executeAt, Known found, BiConsumer<? super Known, Throwable> callback)
    {
        Invariants.checkState(executeAt == null);
        switch (found.outcome)
        {
            default: throw new AssertionError("Unknown outcome: " + found.outcome);
            case Invalidated:
                locallyInvalidateAndCallback(node, txnId, someUnseekables, found, callback);
                break;

            case Unknown:
                if (found.canProposeInvalidation())
                {
                    Invalidate.invalidate(node, txnId, someUnseekables, (outcome, throwable) -> {
                        Known known = throwable != null ? null : outcome.asProgressToken().status == Status.Invalidated ? Known.Invalidated : Known.Nothing;
                        callback.accept(known, throwable);
                    });
                    break;
                }
            case Erased:
            case WasApply:
            case Apply:
                // TODO (required): we may now be stale
                callback.accept(found, null);
        }
    }

    private static Object fetchWithIncompleteRoute(Known fetch, Node node, TxnId txnId, Route<?> someRoute, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        return FindRoute.findRoute(node, txnId, someRoute.withHomeKey(), (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) fetchViaSomeRoute(fetch, node, txnId, someRoute, forLocalEpoch, executeAt, callback);
            else fetch(fetch, node, txnId, foundRoute.route, forLocalEpoch, foundRoute.executeAt, callback);
        });
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, FullRoute<?> route, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        return node.awaitEpoch(executeAt).map(ignore -> {
            long toEpoch = Math.max(fetch.fetchEpoch(txnId, executeAt), forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
            Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);
            return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, callback);
        }).beginAsResult();
    }

    private static Object fetchInternal(Ranges ranges, Known target, Node node, TxnId txnId, PartialRoute<?> route, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = target.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        PartialRoute<?> fetch = route.sliceStrict(ranges);
        return fetchData(target, node, txnId, fetch, srcEpoch, (sufficientFor, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else callback.accept(sufficientFor, null);
        });
    }

    final BiConsumer<Known, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known target;

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, long sourceEpoch, BiConsumer<Known, Throwable> callback)
    {
        this(node, target, txnId, route, route.withHomeKey(), sourceEpoch, callback);
    }

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, Route<?> routeWithHomeKey, long sourceEpoch, BiConsumer<Known, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, sourceEpoch, CheckStatus.IncludeInfo.All);
        Invariants.checkArgument(routeWithHomeKey.contains(route.homeKey()), "route %s does not contain %s", routeWithHomeKey, route.homeKey());
        this.target = target;
        this.callback = callback;
    }

    private static FetchData fetchData(Known sufficientStatus, Node node, TxnId txnId, Route<?> route, long epoch, BiConsumer<Known, Throwable> callback)
    {
        FetchData fetch = new FetchData(node, sufficientStatus, txnId, route, epoch, callback);
        fetch.start();
        return fetch;
    }

    protected Route<?> route()
    {
        return route;
    }

    @Override
    protected boolean isSufficient(Node.Id from, CheckStatus.CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().computeRangesForNode(from);
        PartialRoute<?> scope = this.route.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatus.CheckStatusOk ok)
    {
        return isSufficient(route, ok);
    }

    protected boolean isSufficient(Route<?> scope, CheckStatus.CheckStatusOk ok)
    {
        return target.isSatisfiedBy(((CheckStatusOkFull)ok).sufficientFor(scope.participants(), NoQuorum));
    }

    @Override
    protected void onDone(ReadCoordinator.Success success, Throwable failure)
    {
        Invariants.checkState((success == null) != (failure == null));
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            if (success == ReadCoordinator.Success.Success)
                Invariants.checkState(isSufficient(merged), "Status %s is not sufficient", merged);

            // TODO (expected): should we automatically trigger a new fetch if we find executeAt but did not request enough information? would be more rob ust
            OnDone.propagate(node, txnId, sourceEpoch, success.withQuorum, route(), target, (CheckStatusOkFull) merged, callback);
        }
    }

    static class OnDone implements MapReduceConsume<SafeCommandStore, Void>, EpochSupplier
    {
        final Node node;
        final TxnId txnId;
        final Route<?> route;
        final RoutingKey progressKey;
        final CheckStatusOkFull full;
        // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
        final Known achieved;
        final boolean isTruncated;
        final WithQuorum withQuorum;
        final PartialTxn partialTxn;
        final PartialDeps committedDeps;
        final long toEpoch;
        final BiConsumer<Known, Throwable> callback;

        OnDone(Node node, TxnId txnId, Route<?> route, RoutingKey progressKey, CheckStatusOkFull full, Known achieved, boolean isTruncated, WithQuorum withQuorum, PartialTxn partialTxn, PartialDeps committedDeps, long toEpoch, BiConsumer<Known, Throwable> callback)
        {
            this.node = node;
            this.txnId = txnId;
            this.route = route;
            this.progressKey = progressKey;
            this.full = full;
            this.achieved = achieved;
            this.isTruncated = isTruncated;
            this.withQuorum = withQuorum;
            this.partialTxn = partialTxn;
            this.committedDeps = committedDeps;
            this.toEpoch = toEpoch;
            this.callback = callback;
        }

        @SuppressWarnings({"rawtypes"})
        public static void propagate(Node node, TxnId txnId, long sourceEpoch, WithQuorum withQuorum, Route route, @Nullable Known target, CheckStatusOkFull full, BiConsumer<Known, Throwable> callback)
        {
            if (full.maxKnowledgeSaveStatus.status == NotDefined && full.maxInvalidIfNot() == NotKnownToBeInvalid)
            {
                callback.accept(Known.Nothing, null);
                return;
            }

            Invariants.checkState(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()) || full.maxKnowledgeSaveStatus == SaveStatus.Erased);

            full = full.merge(route).withQuorum(withQuorum);
            route = Invariants.nonNull(full.route);

            // TODO (required): permit individual shards that are behind to catch up by themselves
            long toEpoch = sourceEpoch;
            Ranges sliceRanges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);

            RoutingKey progressKey = node.trySelectProgressKey(txnId, route);

            Ranges covering = route.sliceCovering(sliceRanges, Minimal);
            Participants<?> participatingKeys = route.participants().slice(covering, Minimal);
            Known achieved = full.sufficientFor(participatingKeys, withQuorum);
            if (achieved.executeAt.isDecided() && full.executeAt.epoch() > toEpoch)
            {
                Ranges acceptRanges;
                if (!node.topology().hasEpoch(full.executeAt.epoch()) ||
                    (!route.covers(acceptRanges = node.topology().localRangesForEpochs(txnId.epoch(), full.executeAt.epoch()))))
                {
                    // we don't know what the execution epoch requires, so we cannot be sure we can replicate it locally
                    // we *could* wait until we have the local epoch before running this
                    Status.Outcome outcome = achieved.outcome.propagatesBetweenShards() ? achieved.outcome : Status.Outcome.Unknown;
                    achieved = new Known(achieved.route, achieved.definition, achieved.executeAt, Status.KnownDeps.DepsUnknown, outcome);
                }
                else
                {
                    // TODO (expected): this should only be the two precise epochs, not the full range of epochs
                    sliceRanges = acceptRanges;
                    covering = route.sliceCovering(sliceRanges, Minimal);
                    participatingKeys = route.participants().slice(covering, Minimal);
                    Known knownForExecution = full.sufficientFor(participatingKeys, withQuorum);
                    if ((target != null && target.isSatisfiedBy(knownForExecution)) || achieved.isSatisfiedBy(knownForExecution))
                    {
                        achieved = knownForExecution;
                        toEpoch = full.executeAt.epoch();
                    }
                    else
                    {   // TODO (expected): does downgrading this ever block progress?
                        Invariants.checkState(sourceEpoch == txnId.epoch(), "%d != %d", sourceEpoch, txnId.epoch());
                        achieved = new Known(achieved.route, achieved.definition, achieved.executeAt, knownForExecution.deps, knownForExecution.outcome);
                    }
                }
            }

            boolean isTruncated = withQuorum == HasQuorum && full.isTruncatedResponse(covering);

            PartialTxn partialTxn = null;
            if (achieved.definition.isKnown())
                partialTxn = full.partialTxn.slice(sliceRanges, true).reconstitutePartial(covering);

            PartialDeps partialDeps = null;
            if (achieved.deps.hasDecidedDeps())
                partialDeps = full.committedDeps.slice(sliceRanges).reconstitutePartial(covering);

            new OnDone(node, txnId, route, progressKey, full, achieved, isTruncated, withQuorum, partialTxn, partialDeps, toEpoch, callback).start();
        }

        void start()
        {
            Seekables<?, ?> keys = Keys.EMPTY;
            if (achieved.definition.isKnown())
                keys = partialTxn.keys();
            else if (achieved.deps.hasDecidedDeps())
                keys = committedDeps.keyDeps.keys();

            PreLoadContext loadContext = contextFor(txnId, keys);
            node.mapReduceConsumeLocal(loadContext, route, txnId.epoch(), toEpoch, this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.get(txnId, this, route);
            Command command = safeCommand.current();

            PartialTxn partialTxn = this.partialTxn;
            PartialDeps partialDeps = this.committedDeps;
            switch (command.saveStatus().phase)
            {
                // Already know the outcome, waiting on durability so maybe update with new durability information which can also trigger cleanup
                case Persist: return updateDurability(safeStore, safeCommand);
                case Cleanup: return null;
            }

            Known achieved = this.achieved;
            if (isTruncated)
            {
                achieved = applyOrUpgradeTruncated(safeStore, safeCommand, command);
                if (achieved == null)
                    return null;

                if (achieved.executeAt.isDecided())
                {
                    Timestamp executeAt = command.executeAtIfKnown(full.executeAt);
                    if (partialTxn == null && this.full.maxKnowledgeSaveStatus.known.definition.isKnown())
                    {
                        Ranges needed = safeStore.ranges().allBetween(txnId.epoch(), executeAt.epoch());
                        PartialTxn existing = command.partialTxn();
                        if (existing != null)
                            needed = needed.subtract(existing.covering());
                        partialTxn = full.partialTxn.slice(needed, true).reconstitutePartial(needed);
                    }

                    if (partialDeps == null && !safeCommand.current().known().deps.hasDecidedDeps())
                    {
                        Ranges needed = safeStore.ranges().allBetween(txnId.epoch(), executeAt.epoch());
                        // we don't subtract existing partialDeps, as they cannot be committed deps; we only permit committing deps covering all participating ranges
                        partialDeps = full.committedDeps.slice(needed).reconstitutePartial(needed);
                    }
                }
            }

            Status propagate = achieved.atLeast(command.known()).propagatesStatus();
            if (command.hasBeen(propagate))
            {
                if (full.maxSaveStatus.phase == Cleanup && full.durability.isDurableOrInvalidated() && Infer.safeToCleanup(safeStore, command, route, full.executeAt))
                    Commands.setTruncatedApply(safeStore, safeCommand);

                // TODO (expected): maybe stale?
                return updateDurability(safeStore, safeCommand);
            }
            Timestamp executeAt = command.executeAtIfKnown(full.executeAt);

            switch (propagate)
            {
                default: throw new IllegalStateException("Unexpected status: " + propagate);
                case Truncated: throw new IllegalStateException("Status expected to be handled elsewhere: " + propagate);
                case Accepted:
                case AcceptedInvalidate:
                    // we never "propagate" accepted statuses as these are essentially votes,
                    // and contribute nothing to our local state machine
                    throw new IllegalStateException("Invalid states to propagate: " + achieved.propagatesStatus());

                case Invalidated:
                    Commands.commitInvalidate(safeStore, safeCommand, route);
                    break;

                case Applied:
                case PreApplied:
                    Invariants.checkState(executeAt != null);
                    if (toEpoch >= executeAt.epoch())
                    {
                        confirm(Commands.apply(safeStore, safeCommand, txnId, route, progressKey, executeAt, partialDeps, partialTxn, full.writes, full.result));
                        break;
                    }

                case Committed:
                case ReadyToExecute:
                    confirm(Commands.commit(safeStore, safeCommand, txnId, route, progressKey, partialTxn, executeAt, partialDeps));
                    break;

                case PreCommitted:
                    Commands.precommit(safeStore, safeCommand, txnId, executeAt, route);
                    if (!achieved.definition.isKnown())
                        break;

                case PreAccepted:
                    // only preaccept if we coordinate the transaction
                    if (safeStore.ranges().coordinates(txnId).intersects(route) && Route.isFullRoute(route))
                        Commands.preaccept(safeStore, safeCommand, txnId, txnId.epoch(), partialTxn, Route.castToFullRoute(route), progressKey);
                    break;

                case NotDefined:
                    break;
            }

            return updateDurability(safeStore, safeCommand);
        }

        // if can only propagate Truncated, we might be stale; try to upgrade for this command store only, even partially if necessary
        // note: this is invoked if the command is truncated for ANY local command store - we might
        private Known applyOrUpgradeTruncated(SafeCommandStore safeStore, SafeCommand safeCommand, Command command)
        {
            Invariants.checkState(!full.maxKnowledgeSaveStatus.is(Status.Invalidated));

            if (safeCommand.current().saveStatus().isUninitialised())
                return null; // the command has already been cleaned up locally - don't recreate it

            if (Infer.safeToCleanup(safeStore, command, route, full.executeAt))
            {
                // don't create a new Erased record if we're already cleaned up
                Commands.setErased(safeStore, safeCommand);
                return null;
            }

            Timestamp executeAt = command.executeAtIfKnown(full.executeAtIfKnown());
            Ranges ranges = safeStore.ranges().allBetween(txnId.epoch(), (executeAt == null ? txnId : executeAt).epoch());
            Participants<?> participants = route.participants(ranges, Minimal);
            Invariants.checkState(!participants.isEmpty()); // we shouldn't be fetching data for transactions we only coordinate
            boolean isLocallyTruncated = full.isTruncatedResponse(participants);

            if (!isLocallyTruncated)
            {
                // we're truncated *somewhere* but not locally; whether we have the executeAt is immaterial to this calculus,
                // as we're either ready to go or we're waiting on the coordinating shard to complete this transaction, so pick
                // the maximum we can achieve and return that
                return full.sufficientFor(participants, withQuorum);
            }

            // if our peers have truncated this command, then either:
            // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
            // we're now at least partially stale, but let's first see if we can progress this shard, or we can do so in part
            if (executeAt == null)
            {
                ranges = safeStore.commandStore().redundantBefore().everExpectToExecute(txnId, ranges);
                if (!ranges.isEmpty())
                {
                    // TODO (now): check if the transaction is redundant, as if so it's definitely invalidated (and perhaps we didn't witness this remotely)
                    // we don't even know the execution time, so we cannot possibly proceed besides erasing the command state and marking ourselves stale
                    // TODO (required): we could in principle be stale for future epochs we haven't witnessed yet. Ensure up to date epochs before finalising this application, or else fetch a maximum possible epoch
                    safeStore.commandStore().markShardStale(safeStore, txnId, participants.toRanges().slice(ranges, Minimal), false);
                }
                Commands.setErased(safeStore, safeCommand);
                return null;
            }

            // compute the ranges we expect to execute - i.e. those we own, and are not stale or pre-bootstrap
            ranges = safeStore.commandStore().redundantBefore().expectToExecute(txnId, executeAt, ranges);
            if (ranges.isEmpty())
            {
                // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
                Commands.setTruncatedApply(safeStore, safeCommand, route);
                return null;
            }

            // if the command has been truncated globally, then we should expect to apply it
            // if we cannot obtain enough information from a majority to do so then we have been left behind
            Known required = PreApplied.minKnown;
            Known requireExtra = required.subtract(command.known()); // the extra information we need to reach pre-applied
            Ranges achieveRanges = full.sufficientFor(requireExtra, ranges); // the ranges for which we can successfully achieve this

            if (participants.isEmpty())
            {
                // we only coordinate this transaction, so being unable to retrieve its state does not imply any staleness
                // TODO (now): double check this doesn't stop us coordinating the transaction (it shouldn't, as doesn't imply durability)
                Commands.setTruncatedApply(safeStore, safeCommand, route);
                return null;
            }

            // any ranges we execute but cannot achieve the pre-applied status for have been left behind and are stale
            Ranges staleRanges = ranges.subtract(achieveRanges);
            Participants<?> staleParticipants = participants.slice(staleRanges, Minimal);
            staleRanges = staleParticipants.toRanges();

            if (staleRanges.isEmpty())
            {
                Invariants.checkState(achieveRanges.containsAll(participants));
                return required;
            }

            safeStore.commandStore().markShardStale(safeStore, executeAt, staleRanges, true);
            if (!staleRanges.containsAll(participants))
                return required;

            // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
            Commands.setTruncatedApply(safeStore, safeCommand, route);
            return null;
        }

        /*
         *  If there is new information about the command being durable and we are in the coordination shard in the coordination epoch then update the durability information and possibly cleanup
         */
        private Void updateDurability(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            // TODO (expected): Infer durability status from cleanup/truncation
            RoutingKey homeKey = full.homeKey;
            if (!full.durability.isDurable() || homeKey == null)
                return null;

            if (!safeStore.ranges().coordinates(txnId).contains(homeKey))
                return null;

            Timestamp executeAt = full.executeAtIfKnown();
            Commands.setDurability(safeStore, safeCommand, full.durability, route, executeAt);
            return null;
        }

        @Override
        public Void reduce(Void o1, Void o2)
        {
            return null;
        }

        @Override
        public void accept(Void result, Throwable failure)
        {
            callback.accept(failure  == null ? achieved.propagates() : null, failure);
        }

        @Override
        public long epoch()
        {
            return toEpoch;
        }
    }

    private static void confirm(Commands.CommitOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

    private static void confirm(Commands.ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

}
