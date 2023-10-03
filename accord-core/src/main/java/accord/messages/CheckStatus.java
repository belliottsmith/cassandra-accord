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

import java.util.Objects;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Infer;
import accord.coordinate.Infer.InvalidIfNot;
import accord.coordinate.Infer.IsPreempted;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.AbstractKeys;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidIfNot.NotKnownToBeInvalid;
import static accord.coordinate.Infer.IsPreempted.NotPreempted;
import static accord.coordinate.Infer.IsPreempted.Preempted;
import static accord.local.Status.Committed;
import static accord.local.Status.Durability;
import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.Durability.ShardUniversal;
import static accord.local.Status.Durability.Universal;
import static accord.local.Status.Known;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Truncated;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.messages.CheckStatus.WithQuorum.NoQuorum;
import static accord.messages.TxnRequest.computeScope;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class CheckStatus extends AbstractEpochRequest<CheckStatus.CheckStatusReply>
        implements Request, PreLoadContext, MapReduceConsume<SafeCommandStore, CheckStatus.CheckStatusReply>, EpochSupplier
{
    public enum WithQuorum { HasQuorum, NoQuorum }

    public static class SerializationSupport
    {
        public static CheckStatusOk createOk(CheckStatusMap map, SaveStatus maxKnowledgeStatus, SaveStatus maxStatus, Ballot promised, Ballot accepted, @Nullable Timestamp executeAt,
                                             boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            return new CheckStatusOk(map, maxKnowledgeStatus, maxStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey);
        }
        public static CheckStatusOk createOk(CheckStatusMap map, SaveStatus maxKnowledgeStatus, SaveStatus maxStatus,
                                             Ballot promised, Ballot accepted, @Nullable Timestamp executeAt,
                                             boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey,
                                             PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            return new CheckStatusOkFull(map, maxKnowledgeStatus, maxStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey,
                                         partialTxn, committedDeps, writes, result);
        }
    }

    // order is important
    public enum IncludeInfo
    {
        No, Route, All
    }

    // query is usually a Route
    public final Unseekables<?> query;
    public final long sourceEpoch;
    public final IncludeInfo includeInfo;

    public CheckStatus(TxnId txnId, Unseekables<?> query, long sourceEpoch, IncludeInfo includeInfo)
    {
        super(txnId);
        this.query = query;
        this.sourceEpoch = sourceEpoch;
        this.includeInfo = includeInfo;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    public CheckStatus(Id to, Topologies topologies, TxnId txnId, Unseekables<?> query, long sourceEpoch, IncludeInfo includeInfo)
    {
        super(txnId);
        if (isRoute(query)) this.query = computeScope(to, topologies, castToRoute(query), 0, Route::slice, PartialRoute::union);
        else this.query = computeScope(to, topologies, (Unseekables) query, 0, Unseekables::slice, Unseekables::with);
        this.sourceEpoch = sourceEpoch;
        this.includeInfo = includeInfo;
    }

    @Override
    public void process()
    {
        // TODO (expected): only contact sourceEpoch
        node.mapReduceConsumeLocal(this, query, txnId.epoch(), sourceEpoch, this);
    }

    @Override
    public long epoch()
    {
        return sourceEpoch;
    }

    @Override
    public CheckStatusReply apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, this, query);
        Command command = safeCommand.current();
        // TODO (expected): do we want to force ourselves to serialise these?
        if (!command.has(Known.DefinitionOnly) && Route.isRoute(query) && safeStore.ranges().allAt(txnId.epoch()).contains(Route.castToRoute(query).homeKey()))
            Commands.informHome(safeStore, safeCommand, Route.castToRoute(query));

        Ranges ranges = safeStore.ranges().allBetween(command.txnId().epoch(), command.executeAtOrTxnId().epoch());
        InvalidIfNot invalidIfNotAtLeast = invalidIfNotAtLeast(safeStore);
        boolean isCoordinating = isCoordinating(node, command);
        Durability durability = command.durability();
        Route<?> route = command.route();
        if (Route.isFullRoute(route))
            durability = Durability.mergeAtLeast(durability, safeStore.commandStore().durableBefore().min(txnId, route));

        switch (includeInfo)
        {
            default: throw new IllegalStateException();
            case No:
            case Route:
                Route<?> respondWithRoute = includeInfo == IncludeInfo.No ? null : route;
                return new CheckStatusOk(ranges, isCoordinating, invalidIfNotAtLeast, respondWithRoute, durability, command);
            case All:
                return new CheckStatusOkFull(ranges, isCoordinating, invalidIfNotAtLeast, durability, command);
        }
    }

    private static boolean isCoordinating(Node node, Command command)
    {
        return node.isCoordinating(command.txnId(), command.promised());
    }

    @Override
    public CheckStatusReply reduce(CheckStatusReply r1, CheckStatusReply r2)
    {
        if (r1.isOk() && r2.isOk())
            return ((CheckStatusOk)r1).merge((CheckStatusOk) r2);
        if (r1.isOk() != r2.isOk())
            return r1.isOk() ? r2 : r1;
        CheckStatusNack nack1 = (CheckStatusNack) r1;
        CheckStatusNack nack2 = (CheckStatusNack) r2;
        return nack1.compareTo(nack2) <= 0 ? nack1 : nack2;
    }

    @Override
    public void accept(CheckStatusReply ok, Throwable failure)
    {
        if (failure != null) node.reply(replyTo, replyContext, ok, failure);
        else if (ok == null) node.reply(replyTo, replyContext, CheckStatusNack.NotOwned, null);
        else node.reply(replyTo, replyContext, ok, null);
    }

    private InvalidIfNot invalidIfNotAtLeast(SafeCommandStore safeStore)
    {
        return Infer.invalidIfNotAtLeast(safeStore, txnId, query);
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }

    public static class EnrichedKnown extends Known
    {
        public static final EnrichedKnown Nothing = new EnrichedKnown(Known.Nothing, NotKnownToBeInvalid, NotPreempted);

        public final InvalidIfNot invalidIfNot;
        public final IsPreempted isPreempted;

        public EnrichedKnown(Known known, InvalidIfNot invalidIfNot, IsPreempted isPreempted)
        {
            super(known);
            this.invalidIfNot = invalidIfNot;
            this.isPreempted = isPreempted;
        }

        public EnrichedKnown atLeast(EnrichedKnown with)
        {
            Known known = super.atLeast(with);
            if (known == this)
                return this;
            return new EnrichedKnown(known, invalidIfNot.merge(with.invalidIfNot), isPreempted.merge(with.isPreempted));
        }

        public EnrichedKnown validForBoth(EnrichedKnown with)
        {
            Known known = super.validForBoth(with);
            if (known == this)
                return this;
            return new EnrichedKnown(known, invalidIfNot.validForBoth(with.invalidIfNot), isPreempted.validForBoth(with.isPreempted));
        }

        public boolean inferInvalid(WithQuorum withQuorum)
        {
            return invalidIfNot.inferInvalid(withQuorum, isPreempted, this);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EnrichedKnown that = (EnrichedKnown) o;
            return route == that.route && definition == that.definition && executeAt == that.executeAt && deps == that.deps && outcome == that.outcome && invalidIfNot == that.invalidIfNot && isPreempted == that.isPreempted;
        }
    }

    public static class CheckStatusMap extends ReducingRangeMap<EnrichedKnown>
    {
        private transient final EnrichedKnown validForAll;

        private CheckStatusMap()
        {
            this.validForAll = EnrichedKnown.Nothing;
        }

        private CheckStatusMap(boolean inclusiveEnds, RoutingKey[] ends, EnrichedKnown[] values)
        {
            super(inclusiveEnds, ends, values);
            EnrichedKnown validForAll = EnrichedKnown.Nothing;
            for (EnrichedKnown value : values)
            {
                if (value != null)
                    validForAll = validForAll.validForBoth(value);
            }
            this.validForAll = validForAll;
            if (!validForAll.equals(EnrichedKnown.Nothing))
            {
                for (int i = 0 ; i < values.length ; ++i)
                {
                    if (values[i] != null)
                        values[i] = values[i].atLeast(validForAll);
                }
            }
        }

        public static CheckStatusMap create(Unseekables<?> keysOrRanges, SaveStatus saveStatus, InvalidIfNot invalidIfNot, Ballot promised)
        {
            EnrichedKnown known = new EnrichedKnown(saveStatus.known, invalidIfNot, promised.equals(Ballot.ZERO) ? NotPreempted : Preempted);
            if (keysOrRanges.isEmpty())
                return new CheckStatusMap();

            switch (keysOrRanges.domain())
            {
                default: throw new AssertionError("Unhandled domain type: " + keysOrRanges.domain());
                case Range:
                {
                    Ranges ranges = (Ranges)keysOrRanges;
                    Builder builder = new Builder(ranges.get(0).endInclusive(), 2 * ranges.size());
                    for (Range range : ranges)
                    {
                        builder.append(range.start(), known, EnrichedKnown::atLeast);
                        builder.append(range.end(), null, EnrichedKnown::atLeast);
                    }
                    return builder.build();
                }
                case Key:
                {
                    AbstractKeys<RoutingKey> keys = (AbstractKeys<RoutingKey>) keysOrRanges;
                    Builder builder = new Builder(keys.get(0).asRange().endInclusive(), 2 * keys.size());
                    for (RoutingKey key : keys)
                    {
                        Range range = key.asRange();
                        builder.append(range.start(), known, EnrichedKnown::atLeast);
                        builder.append(range.end(), null, EnrichedKnown::atLeast);
                    }
                    return builder.build();
                }
            }
        }

        public static CheckStatusMap merge(CheckStatusMap a, CheckStatusMap b)
        {
            return ReducingRangeMap.merge(a, b, EnrichedKnown::atLeast, Builder::new);
        }

        public boolean hasTruncated(Routables<?> routables)
        {
            return foldlWithDefault(routables, (known, prev) -> known.outcome.isTruncated(), EnrichedKnown.Nothing, false, i -> i);
        }

        public boolean hasTruncated()
        {
            return foldl((known, prev) -> known.outcome.isTruncated(), false, i -> i);
        }

        public boolean isInvalidated(Participants<?> participants, WithQuorum withQuorum)
        {
            if (validForAll.isInvalidated())
                return true;

            return foldlWithDefault(participants, CheckStatusMap::inferInvalidated, EnrichedKnown.Nothing, withQuorum, Objects::isNull) == withQuorum;
        }

        private static WithQuorum inferInvalidated(EnrichedKnown known, WithQuorum withQuorum)
        {
            if (known.inferInvalid(withQuorum))
                return null;
            return withQuorum;
        }

        public Known inferredOrKnown(Routables<?> routables, WithQuorum withQuorum)
        {
            Known result;
            switch (withQuorum)
            {
                default: throw new AssertionError("Unhandled withQuorum: " + withQuorum);
                case NoQuorum: result = foldlWithDefault(routables, CheckStatusMap::inferredWithoutQuorumOrValidForBoth, EnrichedKnown.Nothing, null, i -> false); break;
                case HasQuorum: result = foldlWithDefault(routables, CheckStatusMap::inferredWithQuorumOrValidForBoth, EnrichedKnown.Nothing, null, i -> false);
            }
            return result.atLeast(validForAll); // only logically necessary in case we matched nothing above
        }

        public Ranges matchingRanges(Predicate<EnrichedKnown> match)
        {
            return foldlWithBounds((known, ranges, start, end) -> match.test(known) ? ranges.with(Ranges.of(start.asRange().newRange(start, end))) : ranges, Ranges.EMPTY, i -> false);
        }

        private static Known inferredWithQuorumOrValidForBoth(EnrichedKnown enrichedKnown, @Nullable Known prev)
        {
            return inferredOrValidForBoth(enrichedKnown, prev, HasQuorum);
        }

        private static Known inferredWithoutQuorumOrValidForBoth(EnrichedKnown enrichedKnown, @Nullable Known prev)
        {
            return inferredOrValidForBoth(enrichedKnown, prev, NoQuorum);
        }

        private static Known inferredOrValidForBoth(EnrichedKnown enrichedKnown, @Nullable Known prev, WithQuorum withQuorum)
        {
            if (enrichedKnown.inferInvalid(withQuorum))
                return Known.Invalidated;

            if (prev == null)
                return enrichedKnown;

            return prev.validForBoth(enrichedKnown);
        }

        public Ranges sufficientFor(Known required, Ranges expect)
        {
            // TODO (desired): implement and use foldlWithDefaultAndBounds so can subtract rather than add
            return foldlWithBounds(expect, (known, prev, start, end) -> {
                if (!required.isSatisfiedBy(known))
                    return prev;

                return prev.with(Ranges.of(start.asRange().newRange(start, end)));
            }, Ranges.EMPTY, i -> false);
        }

        static class Builder extends ReducingIntervalMap.Builder<RoutingKey, EnrichedKnown, CheckStatusMap>
        {
            protected Builder(boolean inclusiveEnds, int capacity)
            {
                super(inclusiveEnds, capacity);
            }

            @Override
            protected CheckStatusMap buildInternal()
            {
                return new CheckStatusMap(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new EnrichedKnown[0]));
            }
        }
    }

    public static class CheckStatusOk implements CheckStatusReply
    {
        public final CheckStatusMap map;
        // TODO (required): tighten up constraints here to ensure we only report truncated when the range is Durable
        // TODO (expected): stop using saveStatus and maxSaveStatus - move to only Known
        //   care needed when merging Accepted and AcceptedInvalidate; might be easier to retain saveStatus only for merging these cases
        public final SaveStatus maxKnowledgeSaveStatus, maxSaveStatus;
        public final Ballot promised, accepted;
        public final @Nullable Timestamp executeAt; // not set if invalidating or invalidated
        public final boolean isCoordinating;
        public final Durability durability;
        public final @Nullable Route<?> route;
        public final @Nullable RoutingKey homeKey;

        public CheckStatusOk(Ranges ranges, boolean isCoordinating, InvalidIfNot invalidIfNot, Durability durability, Command command)
        {
            this(ranges, isCoordinating, invalidIfNot, command.route(), durability, command);
        }

        public CheckStatusOk(Ranges ranges, boolean isCoordinating, InvalidIfNot invalidIfNot, Route<?> route, Durability durability, Command command)
        {
            this(ranges, invalidIfNot, command.saveStatus(), command.promised(), command.accepted(), command.executeAt(),
                 isCoordinating, durability, route, command.homeKey());
        }

        private CheckStatusOk(Ranges ranges, InvalidIfNot invalidIfNot, SaveStatus saveStatus, Ballot promised,
                              Ballot accepted, @Nullable Timestamp executeAt,
                              boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            this(CheckStatusMap.create(ranges, saveStatus, invalidIfNot, promised), saveStatus, saveStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey);
        }

        private CheckStatusOk(CheckStatusMap map, SaveStatus maxKnowledgeSaveStatus, SaveStatus maxSaveStatus, Ballot promised, Ballot accepted,
                              @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            this.map = map;
            this.maxSaveStatus = maxSaveStatus;
            this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
            this.promised = promised;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.isCoordinating = isCoordinating;
            this.durability = durability;
            this.route = route;
            this.homeKey = homeKey;
        }

        public ProgressToken toProgressToken()
        {
            Status status = maxSaveStatus.status;
            return new ProgressToken(durability, status, promised, accepted);
        }

        public Timestamp executeAtIfKnown()
        {
            if (maxKnown().executeAt.isDecided())
                return executeAt;
            return null;
        }

        /**
         * NOTE: if the response is *incomplete* this does not detect possible truncation, it only indicates if the
         * combination of the responses we received represents truncation
         */
        public boolean isTruncatedResponse()
        {
            return map.hasTruncated();
        }

        public boolean isTruncatedResponse(Routables<?> routables)
        {
            return map.hasTruncated(routables);
        }

        public Ranges truncatedResponse()
        {
            return map.matchingRanges(Known::isTruncated);
        }

        public boolean inferInvalidated(Participants<?> participants, WithQuorum withQuorum)
        {
            return map.isInvalidated(participants, withQuorum);
        }

        public Known inferredOrKnown(Routables<?> route, WithQuorum withQuorum)
        {
            return map.inferredOrKnown(route, withQuorum);
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "map:" + map +
                   "maxNotTruncatedSaveStatus:" + maxKnowledgeSaveStatus +
                   "maxSaveStatus:" + maxSaveStatus +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", durability:" + durability +
                   ", isCoordinating:" + isCoordinating +
                   ", route:" + route +
                   ", homeKey:" + homeKey +
                   '}';
        }

        boolean preferSelf(CheckStatusOk that)
        {
            if ((this.maxKnowledgeSaveStatus.is(Truncated) && !this.maxKnowledgeSaveStatus.is(NotDefined)) || (that.maxKnowledgeSaveStatus.is(Truncated) && !that.maxKnowledgeSaveStatus.is(NotDefined)))
                return this.maxKnowledgeSaveStatus.compareTo(that.maxKnowledgeSaveStatus) <= 0;

            return this.maxKnowledgeSaveStatus.compareTo(that.maxKnowledgeSaveStatus) >= 0;
        }

        public CheckStatusOk merge(CheckStatusOk that)
        {
            if (!preferSelf(that))
            {
                Invariants.checkState(that.preferSelf(this));
                return that.merge(this);
            }

            // preferentially select the one that is coordinating, if any
            CheckStatusOk prefer = this.isCoordinating ? this : that;
            CheckStatusOk defer = prefer == this ? that : this;

            // then select the max along each criteria, preferring the coordinator
            CheckStatusMap mergeMap = CheckStatusMap.merge(prefer.map, defer.map);
            CheckStatusOk maxStatus = SaveStatus.max(prefer, prefer.maxKnowledgeSaveStatus, prefer.accepted, defer, defer.maxKnowledgeSaveStatus, defer.accepted, true);
            SaveStatus mergeMaxKnowledgeStatus = SaveStatus.merge(prefer.maxKnowledgeSaveStatus, prefer.accepted, defer.maxKnowledgeSaveStatus, defer.accepted, true);
            SaveStatus mergeMaxStatus = SaveStatus.merge(prefer.maxSaveStatus, prefer.accepted, defer.maxSaveStatus, defer.accepted, false);
            CheckStatusOk maxPromised = prefer.promised.compareTo(defer.promised) >= 0 ? prefer : defer;
            CheckStatusOk maxAccepted = prefer.accepted.compareTo(defer.accepted) >= 0 ? prefer : defer;
            CheckStatusOk maxHomeKey = prefer.homeKey != null || defer.homeKey == null ? prefer : defer;
            CheckStatusOk maxExecuteAt = prefer.maxKnown().executeAt.compareTo(defer.maxKnown().executeAt) >= 0 ? prefer : defer;
            Route<?> mergedRoute = Route.merge(prefer.route, (Route)defer.route);
            Durability mergedDurability = Durability.merge(prefer.durability, defer.durability);

            // if the maximum (or preferred equal) is the same on all dimensions, return it
            if (mergeMaxKnowledgeStatus == maxStatus.maxKnowledgeSaveStatus
                && mergeMaxStatus == maxStatus.maxSaveStatus
                && maxStatus == maxPromised && maxStatus == maxAccepted
                && maxStatus == maxHomeKey && maxStatus == maxExecuteAt
                && maxStatus.route == mergedRoute
                && maxStatus.map.equals(mergeMap)
                && maxStatus.durability == mergedDurability)
            {
                return maxStatus;
            }

            // otherwise assemble the maximum of each, and propagate isCoordinating from the origin we selected the promise from
            boolean isCoordinating = maxPromised == prefer ? prefer.isCoordinating : defer.isCoordinating;
            return new CheckStatusOk(mergeMap, mergeMaxKnowledgeStatus, mergeMaxStatus, maxPromised.promised, maxAccepted.accepted, maxExecuteAt.executeAt,
                                     isCoordinating, mergedDurability, mergedRoute, maxHomeKey.homeKey);
        }

        public Known maxKnown()
        {
            return map.foldl(Known::atLeast, Known.Nothing, i -> false);
        }

        public InvalidIfNot maxInvalidIfNot()
        {
            return map.foldl((known, prev) -> known.invalidIfNot.merge(prev), NotKnownToBeInvalid, InvalidIfNot::isMax);
        }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }
    }

    public static class CheckStatusOkFull extends CheckStatusOk
    {
        public final PartialTxn partialTxn;
        public final PartialDeps committedDeps; // only set if status >= Committed, so safe to merge
        public final Writes writes;
        public final Result result;

        public CheckStatusOkFull(Ranges ranges, boolean isCoordinating, InvalidIfNot invalidIfNot, Durability durability, Command command)
        {
            super(ranges, isCoordinating, invalidIfNot, durability, command);
            this.partialTxn = command.partialTxn();
            this.committedDeps = command.status().compareTo(Committed) >= 0 ? command.partialDeps() : null;
            this.writes = command.writes();
            this.result = command.result();
        }

        protected CheckStatusOkFull(CheckStatusMap map, SaveStatus maxNotTruncatedSaveStatus, SaveStatus maxSaveStatus, Ballot promised, Ballot accepted, Timestamp executeAt,
                                  boolean isCoordinating, Durability durability, Route<?> route,
                                  RoutingKey homeKey, PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            super(map, maxNotTruncatedSaveStatus, maxSaveStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey);
            this.partialTxn = partialTxn;
            this.committedDeps = committedDeps;
            this.writes = writes;
            this.result = result;
        }

        /**
         * This method assumes parameter is of the same type and has the same additional info (modulo partial replication).
         * If parameters have different info, it is undefined which properties will be returned.
         *
         * This method is NOT guaranteed to return CheckStatusOkFull unless the parameter is also CheckStatusOkFull.
         * This method is NOT guaranteed to return either parameter: it may merge the two to represent the maximum
         * combined info, (and in this case if the parameter were not CheckStatusOkFull, and were the higher status
         * reply, the info would potentially be unsafe to act upon when given a higher status
         * (e.g. Accepted executeAt is very different to Committed executeAt))
         */
        @Override
        public CheckStatusOk merge(CheckStatusOk that)
        {
            CheckStatusOk max = super.merge(that);
            CheckStatusOk maxSrc = preferSelf(that) ? this : that;
            if (!(maxSrc instanceof CheckStatusOkFull))
                return max;

            CheckStatusOkFull fullMax = (CheckStatusOkFull) maxSrc;
            CheckStatusOk minSrc = maxSrc == this ? that : this;
            if (!(minSrc instanceof CheckStatusOkFull))
            {
                return new CheckStatusOkFull(max.map, max.maxKnowledgeSaveStatus, max.maxSaveStatus, max.promised, max.accepted, fullMax.executeAt, max.isCoordinating, max.durability, max.route,
                                             max.homeKey, fullMax.partialTxn, fullMax.committedDeps, fullMax.writes, fullMax.result);
            }

            CheckStatusOkFull fullMin = (CheckStatusOkFull) minSrc;

            PartialTxn partialTxn = PartialTxn.merge(fullMax.partialTxn, fullMin.partialTxn);
            PartialDeps committedDeps;
            if (fullMax.committedDeps == null) committedDeps = fullMin.committedDeps;
            else if (fullMin.committedDeps == null) committedDeps = fullMax.committedDeps;
            else committedDeps = fullMax.committedDeps.with(fullMin.committedDeps);
            Writes writes = (fullMax.writes != null ? fullMax : fullMin).writes;
            Result result = (fullMax.result != null ? fullMax : fullMin).result;

            return new CheckStatusOkFull(max.map, max.maxKnowledgeSaveStatus, max.maxSaveStatus, max.promised, max.accepted, max.executeAt, max.isCoordinating, max.durability, max.route,
                                         max.homeKey, partialTxn, committedDeps, writes, result);
        }

        public CheckStatusOkFull merge(@Nonnull Route<?> route)
        {
            Route<?> mergedRoute = Route.merge((Route)this.route, route);
            if (mergedRoute == this.route)
                return this;
            return new CheckStatusOkFull(map, maxKnowledgeSaveStatus, maxSaveStatus, promised, accepted, executeAt,
                                         isCoordinating, durability, mergedRoute, homeKey, partialTxn, committedDeps, writes, result);
        }

        public CheckStatusOkFull merge(@Nonnull Durability durability)
        {
            durability = Durability.merge(durability, this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOkFull(map, maxKnowledgeSaveStatus, maxSaveStatus, promised, accepted, executeAt,
                                         isCoordinating, durability, route, homeKey, partialTxn, committedDeps, writes, result);
        }

        public CheckStatusOkFull withQuorum()
        {
            Durability durability = this.durability;
            if (durability == Local) durability = Majority;
            else if (durability == ShardUniversal) durability = Universal;
            return merge(durability);
        }

        public CheckStatusOkFull withQuorum(WithQuorum withQuorum)
        {
            return withQuorum == NoQuorum ? this : withQuorum();
        }

        // TODO (required): harden markShardStale against unnecessary actions by utilising inferInvalidated==MAYBE and performing a global query
        public Known sufficientFor(Participants<?> participants, WithQuorum withQuorum)
        {
            Known known = map.inferredOrKnown(participants, withQuorum);
            // TODO (desired): make sure these match identically, rather than only ensuring Route.isFullRoute (either by coercing it here or by ensuring it at callers)
            Invariants.checkState(!known.hasFullRoute() || Route.isFullRoute(route));
            Invariants.checkState(!known.hasDefinition() || (partialTxn != null && partialTxn.covering().containsAll(participants)));
            Invariants.checkState(!known.hasDecidedDeps() || (committedDeps != null && committedDeps.covering.containsAll(participants)));
            Invariants.checkState(!known.outcome.isInvalidated() || (!maxKnowledgeSaveStatus.known.isDecidedToExecute() && !maxSaveStatus.known.isDecidedToExecute()));
            Invariants.checkState(!(maxSaveStatus.known.outcome.isInvalidated() || maxKnowledgeSaveStatus.known.outcome.isInvalidated()) || !known.isDecidedToExecute());
            return known;
        }

        // it is assumed that we are invoking this for a transaction that will execute;
        // the result may be erroneous if the transaction is invalidated, as logically this can apply to all ranges
        public Ranges sufficientFor(Known required, Ranges expect)
        {
            Invariants.checkState(maxSaveStatus != SaveStatus.Invalidated);
            return map.sufficientFor(required, expect);
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "map:" + map +
                   ", maxSaveStatus:" + maxSaveStatus +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", durability:" + durability +
                   ", isCoordinating:" + isCoordinating +
                   ", deps:" + committedDeps +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }
    }

    public enum CheckStatusNack implements CheckStatusReply
    {
        NotOwned;

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "CheckStatusNack{" + name() + '}';
        }
    }

    @Override
    public String toString()
    {
        return "CheckStatus{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.CHECK_STATUS_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return sourceEpoch;
    }
}
