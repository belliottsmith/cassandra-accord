/*
 * Licensed to the Apache Software ation (ASF) under one
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

import accord.api.ProtocolModifiers;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.FetchData.FetchResult;
import accord.coordinate.Infer.InvalidIf;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.RedundantStatus;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Known;
import accord.local.StoreParticipants;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.primitives.KnownMap;
import accord.primitives.WithQuorum;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import static accord.coordinate.Infer.InvalidIf.IfUncommitted;
import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.local.Cleanup.ERASE;
import static accord.local.Cleanup.VESTIGIAL;
import static accord.local.Commands.purge;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_SYNCED;
import static accord.local.StoreParticipants.Filter.UPDATE;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.Nothing;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.WithQuorum.HasQuorum;
import static accord.utils.Invariants.illegalState;

// TODO (required): detect propagate loops where we don't manage to update anything but should
public class Propagate implements PreLoadContext, MapReduceConsume<SafeCommandStore, Void>
{
    final Node node;
    final TxnId txnId;
    final Route<?> route;
    final Unseekables<?> propagateTo;
    final Known target;
    final InvalidIf invalidIf;

    // TODO (desired): remove dependency on these two SaveStatus
    final SaveStatus maxKnowledgeSaveStatus;
    final SaveStatus maxSaveStatus;
    final Ballot ballot;
    final Status.Durability durability;
    @Nullable final RoutingKey homeKey;
    // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
    final KnownMap known;
    final WithQuorum withQuorum;
    @Nullable final PartialTxn partialTxn;
    @Nullable final PartialDeps stableDeps;
    final long lowEpoch, highEpoch;
    @Nullable final Timestamp committedExecuteAt;
    @Nullable final Writes writes;
    @Nullable final Result result;
    final BiConsumer<? super FetchResult, Throwable> callback;

    private transient volatile FetchResult fetchResult;
    private static final AtomicReferenceFieldUpdater<Propagate, FetchResult> fetchResultUpdater = AtomicReferenceFieldUpdater.newUpdater(Propagate.class, FetchResult.class, "fetchResult");

    Propagate(
    Node node, TxnId txnId,
    Route<?> route,
    Unseekables<?> propagateTo, Known target, InvalidIf invalidIf,
    SaveStatus maxKnowledgeSaveStatus,
    SaveStatus maxSaveStatus,
    Ballot ballot,
    Status.Durability durability,
    @Nullable RoutingKey homeKey,
    KnownMap known, WithQuorum withQuorum,
    @Nullable PartialTxn partialTxn,
    @Nullable PartialDeps stableDeps,
    long lowEpoch,
    long highEpoch,
    @Nullable Timestamp committedExecuteAt,
    @Nullable Writes writes,
    @Nullable Result result,
    BiConsumer<? super FetchResult, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.propagateTo = propagateTo;
        this.target = target;
        this.invalidIf = invalidIf;
        this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
        this.maxSaveStatus = maxSaveStatus;
        this.ballot = ballot;
        this.durability = durability;
        this.homeKey = homeKey;
        this.known = known;
        this.withQuorum = withQuorum;
        this.partialTxn = partialTxn;
        this.stableDeps = stableDeps;
        this.lowEpoch = lowEpoch;
        this.highEpoch = highEpoch;
        this.committedExecuteAt = committedExecuteAt;
        this.writes = writes;
        this.result = result;
        this.callback = callback;
    }

    public static void propagate(Node node, TxnId txnId, InvalidIf previouslyKnownToBeInvalidIf, long sourceEpoch, long lowEpoch, long highEpoch, WithQuorum withQuorum, Route<?> queried, Unseekables<?> propagateTo, @Nullable Known target, CheckStatusOkFull full, BiConsumer<? super FetchResult, Throwable> callback)
    {
        if (full.maxKnowledgeSaveStatus.status == NotDefined && full.invalidIf == NotKnownToBeInvalid)
        {
            callback.accept(new FetchResult(Nothing, propagateTo.slice(0, 0), propagateTo), null);
            return;
        }

        Invariants.require(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()) || full.maxSaveStatus == SaveStatus.Erased || full.maxSaveStatus == SaveStatus.Vestigial);

        // TODO (required): consider and document whether it is safe to infer that we are stale if we have not received responses from all shards we know of
        //  (in principle, we should at least require responses from our own shard, and the home shard if we know it); if we only hear from a remote shard it may have fully Erased
        full = full.finish(queried, propagateTo, queried.with((Unseekables) propagateTo), withQuorum, previouslyKnownToBeInvalidIf);
        Route<?> route = Invariants.nonNull(full.route);

        Propagate propagate =
            new Propagate(node, txnId, route, propagateTo, target, full.invalidIf, full.maxKnowledgeSaveStatus, full.maxSaveStatus, full.acceptedOrCommitted, full.durability, full.homeKey, full.map, withQuorum, full.partialTxn, full.stableDeps, lowEpoch, highEpoch, full.executeAtIfKnown(), full.writes, full.result, callback);

        if (full.executeAt != null && full.executeAt.epoch() > highEpoch)
            highEpoch = full.executeAt.epoch();
        long untilEpoch = full.executeAt == null ? highEpoch : Math.max(highEpoch, full.executeAt.epoch());

        Route<?> finalRoute = queried;
        node.withEpoch(highEpoch, propagate, () -> node.mapReduceConsumeLocal(propagate, finalRoute, lowEpoch, untilEpoch, propagate));
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        long executeAtEpoch = committedExecuteAt == null ? txnId.epoch() : committedExecuteAt.epoch();
        StoreParticipants participants = StoreParticipants.update(safeStore, route, lowEpoch, txnId, executeAtEpoch, highEpoch, committedExecuteAt != null);
        // TODO (expected): can we come up with a better more universal pattern for avoiding updating a command we don't intersect with?
        //   ideally integrated with safeStore.get()
        if (participants.owns().isEmpty() && safeStore.ifInitialised(txnId) == null)
            return null;

        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();

        Timestamp executeAtIfKnown = command.executeAtIfKnown(committedExecuteAt);
        if (participants.executes() == null && executeAtIfKnown != null)
        {
            executeAtEpoch = executeAtIfKnown.epoch();
            participants = StoreParticipants.update(safeStore, route, lowEpoch, txnId, executeAtEpoch, highEpoch, true);
        }

        switch (command.saveStatus().phase)
        {
            // Already know the outcome, waiting on durability so maybe update with new durability information which can also trigger cleanup
            case Persist: return updateDurability(safeStore, safeCommand, participants);
            case Cleanup:
            case Invalidate:
                return null;
        }

        participants = participants.supplement(command.participants())
                                   .filter(UPDATE, safeStore, txnId, executeAtIfKnown);
        Known found = known.knownFor(participants.stillOwns(), participants.stillTouches());

        PartialTxn partialTxn = null;
        if (found.hasDefinition())
            partialTxn = this.partialTxn.intersecting(participants.stillOwns(), true).reconstitutePartial(participants.stillOwns());

        PartialDeps stableDeps = null;
        if (found.hasDecidedDeps())
            stableDeps = this.stableDeps.intersecting(participants.stillTouches()).reconstitutePartial(participants.stillTouches());

        boolean isShardTruncated = withQuorum == HasQuorum && known.hasAnyFullyTruncated(participants.stillTouches());
        if (isShardTruncated)
        {
            found = tryUpgradeTruncated(safeStore, safeCommand, participants, command, executeAtIfKnown);
            if (found == null)
            {
                // TODO (expected): should be ownsOrExecutes()?
                updateFetchResult(Nothing, participants.owns());
                return null;
            }

            if (command.known().is(DepsKnown))
            {
                // keep the deps we already have
                participants = command.participants().supplement(participants);
            }
            else
            {
                Participants<?> depsNeeds = participants.stillTouches();
                if (found.hasDecidedDeps() && stableDeps == null && this.stableDeps != null)
                {
                    Invariants.require(executeAtIfKnown != null);
                    // we don't subtract existing partialDeps, as they cannot be committed deps; we only permit committing deps covering all participating ranges
                    stableDeps = this.stableDeps.intersecting(depsNeeds).reconstitutePartial(depsNeeds);
                }
            }

            Participants<?> txnNeeds = participants.stillOwnsOrMayExecute(txnId);
            if (found.isDefinitionKnown() && partialTxn == null && this.partialTxn != null)
            {
                PartialTxn existing = command.partialTxn();
                Participants<?> neededExtra = txnNeeds;
                if (existing != null) neededExtra = neededExtra.without(existing.keys().toParticipants());
                partialTxn = this.partialTxn.intersecting(neededExtra, true).reconstitutePartial(neededExtra);
            }
        }

        SaveStatus propagate = found.atLeast(command.known()).propagatesSaveStatus();
        if (propagate.known.isSatisfiedBy(command.known()))
        {
            updateFetchResult(found, participants.owns());
            return updateDurability(safeStore, safeCommand, participants);
        }

        switch (propagate.status)
        {
            default: throw illegalState("Unexpected status: " + propagate);
            case Truncated: throw illegalState("Status expected to be handled elsewhere: " + propagate);
            case AcceptedMedium:
            case AcceptedSlow:
            case AcceptedInvalidate:
                // we never "propagate" accepted statuses as these are essentially votes,
                // and contribute nothing to our local state machine
                throw illegalState("Invalid states to propagate: " + propagate);

            case Invalidated:
                Commands.commitInvalidate(safeStore, safeCommand, route);
                break;

            case Applied:
            case PreApplied:
                Invariants.require(committedExecuteAt != null);
                // we must use the remote executeAt, as it might have a uniqueHlc we aren't aware of at commit
                confirm(Commands.apply(safeStore, safeCommand, participants, txnId, route, committedExecuteAt, stableDeps, partialTxn, writes, result));
                break;

            case Stable:
                confirm(Commands.commit(safeStore, safeCommand, participants, Stable, ballot, txnId, route, partialTxn, executeAtIfKnown, stableDeps, null));
                break;

            case Committed:
                // TODO (expected): we can propagate Committed as Stable if we have any other Stable result AND a quorum of committedDeps
            case PreCommitted:
                confirm(Commands.precommit(safeStore, safeCommand, participants, txnId, executeAtIfKnown));
                // TODO (desired): would it be clearer to yield a SaveStatus so we can have PreCommittedWithDefinition
                if (!found.definition().isKnown())
                    break;

            case PreAccepted:
                // only preaccept if we coordinate the transaction
                if (safeStore.ranges().coordinates(txnId).intersects(route) && Route.isFullRoute(route))
                    Commands.preaccept(safeStore, safeCommand, participants, txnId, partialTxn, null, false, Route.castToFullRoute(route));

            case NotDefined:
                if (invalidIf == IfUncommitted)
                    safeStore.progressLog().invalidIfUncommitted(txnId);
                break;
        }

        updateFetchResult(found.propagates(), participants.owns());
        return updateDurability(safeStore, safeCommand, participants);
    }

    private void updateFetchResult(Known achieved, Participants<?> owns)
    {
        achieved = achieved.propagates();
        Unseekables<?> achievedTarget = owns;
        Unseekables<?> didNotAchieveTarget = null;
        if (target != null && !target.isSatisfiedBy(achieved))
        {
            achievedTarget = owns.slice(0, 0);
            didNotAchieveTarget = owns;
        }

        while (true)
        {
            FetchResult current = fetchResult;
            FetchResult next = current == null ? new FetchResult(achieved, achievedTarget, didNotAchieveTarget)
                               : new FetchResult(achieved.reduce(current.achieved),
                                                 achievedTarget.with((Unseekables)current.achievedTarget),
                                                 Unseekables.merge(current.didNotAchieveTarget, (Unseekables) didNotAchieveTarget));

            if (fetchResultUpdater.compareAndSet(this, current, next))
                return;
        }
    }

    private FetchResult finaliseFetchResult()
    {
        FetchResult current = fetchResult;
        if (current == null)
            return new FetchResult(Nothing, propagateTo.slice(0, 0), propagateTo);

        Unseekables<?> missed = propagateTo.without(current.achievedTarget);
        if (missed.isEmpty())
            return current;

        return new FetchResult(Nothing, current.achievedTarget, Unseekables.merge(missed, (Unseekables) current.didNotAchieveTarget));
    }

    // if can only propagate Truncated, we might be stale; try to upgrade for this command store only, even partially if necessary
    private Known tryUpgradeTruncated(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Command command, Timestamp executeAtIfKnown)
    {
        // if our peers have truncated this command, then either:
        // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
        Invariants.require(!maxKnowledgeSaveStatus.is(Status.Invalidated));

        Participants<?> stillTouches = participants.stillTouches();
        if (stillTouches.isEmpty())
            return known.knownForAny();

        RedundantStatus status = safeStore.redundantBefore().status(txnId, null, stillTouches);
        // try to see if we can safely purge the full command
        if (tryPurge(safeStore, safeCommand, status))
            return null;

        // if the command has been truncated globally, then we should expect to apply it
        // if we cannot obtain enough information from a majority to do so then we have been left behind
        Known required = PreApplied.minKnown;
        Known requireExtra = required.subtract(command.known()); // the extra information we need to reach pre-applied

        Participants<?> stillOwnsOrMayExecute = participants.stillOwnsOrMayExecute(txnId);
        Participants<?> notStaleTouches = known.knownFor(Nothing.with(requireExtra.deps()), stillOwnsOrMayExecute); // the ranges for which we can already successfully achieve this
        Participants<?> notStaleOwnsOrMayExecutes = known.knownFor(requireExtra.with(DepsUnknown), stillOwnsOrMayExecute); // the ranges for which we can already successfully achieve this

        // any ranges we execute but cannot achieve the pre-applied status for have been left behind and are stale
        Participants<?> staleTouches = stillTouches.without(notStaleTouches);
        Participants<?> staleOwnsOrMayExecutes = stillOwnsOrMayExecute.without(notStaleOwnsOrMayExecutes);
        if (staleOwnsOrMayExecutes.isEmpty() && staleTouches.isEmpty())
        {
            Invariants.require(notStaleTouches.containsAll(stillTouches));
            Invariants.require(notStaleOwnsOrMayExecutes.containsAll(stillOwnsOrMayExecute));
            return required;
        }

        if (!known.hasFullyTruncated(staleOwnsOrMayExecutes) || !known.hasFullyTruncated(staleTouches))
            return null;

        Participants<?> stale = staleTouches.with((Participants) staleOwnsOrMayExecutes);
        // TODO (expected): trigger a refresh of redundantBefore; should be available on a peer
        // wait until we know the shard is ahead and we are behind
        if (!safeStore.redundantBefore().isShardOnlyRedundant(txnId, stale))
            return null;

        // TODO (expected): if the above last ditch doesn't work, see if only the stale ranges can't apply and so some shenanigans to apply partially and move on
        if (ProtocolModifiers.Toggles.markStaleIfCannotExecute(txnId.kind()))
        {
            safeStore.commandStore().markShardStale(safeStore, executeAtIfKnown == null ? txnId : executeAtIfKnown, stale.toRanges(), true);
            if (!stale.containsAll(stillTouches) || !stale.containsAll(stillOwnsOrMayExecute))
                return required;
        }

        // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
        Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, participants, executeAtIfKnown);
        return null;
    }

    private boolean tryPurge(SafeCommandStore safeStore, SafeCommand safeCommand, RedundantStatus status)
    {
        if (!status.all(SHARD_APPLIED_AND_LOCALLY_SYNCED))
            return false;

        Cleanup cleanup = status.all(LOCALLY_DEFUNCT) ? VESTIGIAL : ERASE;
        purge(safeStore, safeCommand, cleanup, true, true);
        return true;
    }

    /*
     *  If there is new information about the command being durable and we are in the coordination shard in the coordination epoch then update the durability information and possibly cleanup
     */
    private Void updateDurability(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        if (!durability.isDurable() || homeKey == null)
            return null;

        Commands.setDurability(safeStore, safeCommand, participants, durability, committedExecuteAt);
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
        if (null != callback)
            callback.accept(failure != null ? null : finaliseFetchResult(), failure);
    }

    private static void confirm(Commands.CommitOutcome outcome)
    {
        switch (outcome)
        {
            default: throw illegalState("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw illegalState("Should have enough information");
        }
    }

    private static void confirm(Commands.ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw illegalState("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw illegalState("Should have enough information");
        }
    }

    @Override
    public String toString()
    {
        return "Propagate{txnId: " + txnId +
               ", saveStatus: " + maxKnowledgeSaveStatus +
               ", deps: " + stableDeps +
               ", txn: " + partialTxn +
               ", executeAt: " + committedExecuteAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }

}
