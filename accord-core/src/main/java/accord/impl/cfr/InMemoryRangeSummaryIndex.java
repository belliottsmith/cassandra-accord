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

package accord.impl.cfr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandSummaries.Summary;
import accord.local.CommandSummaries.SummaryLoader;
import accord.local.CommandSummaries.Relevance;
import accord.local.RedundantBefore;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.SemiSyncIntervalTree;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;
import accord.utils.btree.BTree;
import accord.utils.btree.IntervalBTree;
import accord.utils.btree.IntervalBTree.FastIntervalTreeBuilder;

import static accord.api.ProtocolModifiers.RangeSpec.isEndInclusive;
import static accord.impl.cfr.ListenerEntry.LISTENER_ENTRIES;
import static accord.impl.cfr.ListenerEntry.LISTENER_WITH_KEYS;
import static accord.impl.cfr.ListenerEntry.LISTENER_WITH_RANGES;
import static accord.impl.cfr.RangeEntry.ENTRIES;
import static accord.impl.cfr.RangeEntry.WITH_KEY;
import static accord.impl.cfr.RangeEntry.WITH_RANGE;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.UNREADY;

/**
 * An implementation filling the same niche as CommandsForKey, only we do not retain
 * complete information, and may need to load a command to decide if it is relevant.
 * We also don't consume this structure directly, instead building a set of summary records to work with.
 */
public class InMemoryRangeSummaryIndex extends SemiSyncIntervalTree<IdEntry>
{
    private final Map<TxnId, IdEntry> byId = new HashMap<>();
    private Object[] listeners = IntervalBTree.empty();

    public InMemoryRangeSummaryIndex()
    {
        super(ENTRIES);
        Invariants.require(isEndInclusive(), "Need to implement range-exclusive IntervalComparators");
    }

    private boolean update(Command command)
    {
        TxnId txnId = command.txnId();
        SaveStatus saveStatus = command.saveStatus();
        AbstractRanges participants = (AbstractRanges) command.participants().stillTouches();
        if (saveStatus.compareTo(SaveStatus.TruncatedUnapplied) >= 0 || participants.isEmpty())
        {
            IdEntry entry = byId.remove(txnId);
            if (entry != null)
                pushEdit(entry, null, entry);
            return false;
        }

        IdEntry cur = byId.get(txnId);
        IdEntry next = cur;
        if (participants.size() == 1)
        {
            Range range = participants.get(0).asRange();
            if (cur == null || cur.getClass() != IdSingleEntry.class || !((IdSingleEntry) cur).range.equals(range))
                next = new IdSingleEntry(txnId, range);
        }
        else
        {
            if (cur == null || cur.getClass() != IdMultiEntry.class || !((IdMultiEntry) cur).ranges.hasSameRanges(participants))
                next = new IdMultiEntry(txnId, participants.toRanges());
        }
        if (next != cur)
        {
            if (cur != null)
                byId.remove(cur, cur);
            byId.put(next, next);
            pushEdit(next, next, cur);
        }
        return next.update(saveStatus, command.durability(), command.executeAtIfKnown()) || next != cur;
    }

    private static Object[] toMultiTree(IdMultiEntry entry)
    {
        try (FastIntervalTreeBuilder<RangeEntry> builder = IntervalBTree.fastBuilder(ENTRIES))
        {
            for (Range range : entry.ranges)
                builder.add(new RangeMultiEntry(range, entry));
            return builder.build();
        }
    }

    public <A> A foldl(Unseekables<?> participants, BiFunction<IdEntry, A, A> f, A accumulator)
    {
        Object[] tree = get();
        switch (participants.domain())
        {
            case Key:
                for (RoutingKey key : (AbstractUnseekableKeys)participants)
                    accumulator = IntervalBTree.accumulate(tree, WITH_KEY, key, InMemoryRangeSummaryIndex::foldl, f, participants, accumulator);
                break;
            case Range:
                for (Range range : (AbstractRanges)participants)
                    accumulator = IntervalBTree.accumulate(tree, WITH_RANGE, range, InMemoryRangeSummaryIndex::foldl, f, participants, accumulator);
                break;
        }
        return accumulator;
    }

    public void populateMinFutureRx(SummaryLoader loader)
    {
        foldl(loader.participants(), (e, l) -> {
            if (e.isSyncPoint() && l.shouldRecordFutureRx(e, e.saveStatus().summary))
                l.recordFutureRx(e.plainTxnId(), e.ranges());
            return l;
        }, loader);
    }

    /**
     * If we have enough information, simply directly build the Summary object and return a map of these objects;
     * otherwise invoke the provided Consumer so that the implementation may build the summary
     */
    public void search(SummaryLoader loader, @Nullable BiConsumer<TxnId, Summary> found, @Nullable Consumer<TxnId> mustLoad)
    {
        foldl(loader.participants(), (e, l) -> {
            Ranges ranges = e.ranges();
            Relevance relevance = l.relevance(e, e.saveStatus(), e.durability(), e.maybeExecuteAt(), ranges);
            if (relevance == Relevance.IRRELEVANT)
                return l;

            TxnId txnId = e.plainTxnId();
            switch (relevance)
            {
                default: throw new UnhandledEnum(relevance);
                case ACTIVE:
                    if (found != null)
                    {
                        Summary summary = l.ifRelevant(e.plainTxnId(), e.maybeExecuteAt(), e.saveStatus(), e.durability(), ranges, null);
                        Invariants.nonNull(summary);
                        // TODO (expected): we can post-filter collected txnId after recording future rx to remove those that are no longer needed
                        //    (or, we can retain summary information to permit evicting them as we collect)
                        found.accept(txnId, summary);
                    }
                    break;
                case SUPERSEDING:
                case BOTH:
                    if (mustLoad != null)
                        mustLoad.accept(txnId);
            }
            return l;
        }, loader);
    }

    static < A> A foldl(BiFunction<IdEntry, A, A> f, Unseekables<?> find, RangeEntry e, A accumulator)
    {
        if (e.getClass() == IdSingleEntry.class)
        {
            return f.apply((IdSingleEntry)e, accumulator);
        }
        else
        {
            IdEntry id = e.id();
            IdMultiEntry mid = (IdMultiEntry) id;
            if (mid.ranges.size() > 1)
            {
                int i = (int) (find.findFirstIntersection(mid.ranges) >>> 32);
                if (mid.ranges.get(i) != e.range())
                    return accumulator;
            }
            return f.apply(id, accumulator);
        }
    }

    public void update(Command prev, Command updated, boolean force)
    {
        if (!force
             && updated.saveStatus() == prev.saveStatus()
             && updated.participants().stillTouches().equals(prev.participants().touches())
             && Objects.equals(updated.executeAt(), prev.executeAt()))
            return;

        if (update(updated))
        {
            // invoke listeners
            Participants<?> stillTouches = updated.participants().stillTouches();
            switch (stillTouches.domain())
            {
                default: throw new UnhandledEnum(stillTouches.domain());
                case Key:
                    for (RoutingKey key : (AbstractUnseekableKeys)stillTouches)
                        IntervalBTree.accumulate(listeners, LISTENER_WITH_KEYS, key, InMemoryRangeSummaryIndex::visitListeners, stillTouches, updated, null);
                    break;
                case Range:
                    for (Range range : (AbstractRanges)stillTouches)
                        IntervalBTree.accumulate(listeners, LISTENER_WITH_RANGES, range, InMemoryRangeSummaryIndex::visitListeners, stillTouches, updated, null);
                    break;
            }
        }
    }

    public Cancellable registerListener(Listener listener)
    {
        Object[] tree;
        try (IntervalBTree.FastIntervalTreeBuilder<ListenerEntry> builder = IntervalBTree.fastBuilder(LISTENER_ENTRIES))
        {
            for (Unseekable u : listener.participants())
                builder.add(new ListenerEntry(u.asRange(), listener));
            tree = builder.build();
        }
        listeners = IntervalBTree.update(listeners, tree, LISTENER_ENTRIES);
        return () -> listeners = IntervalBTree.subtract(listeners, tree, LISTENER_ENTRIES);
    }

    private static Object visitListeners(Participants participants, Command command, ListenerEntry entry, Object v)
    {
        Unseekables<?> searchingFor = entry.listener.participants();
        if (searchingFor.size() > 1)
        {
            int i = (int) searchingFor.findNextIntersection(0, participants, 0);
            if (!searchingFor.get(i).asRange().equals(entry.range))
                return v;
        }
        entry.listener.accept(command);
        return v;
    }

    public void prune(CommandStore commandStore)
    {
        prune(commandStore.unsafeGetRangesForEpoch().all(), commandStore.unsafeGetRedundantBefore());
    }

    public void prune(Ranges ranges, RedundantBefore redundantBefore)
    {
        prune(TxnId.MAX, ranges, redundantBefore);
    }

    public void prune(TxnId lessThan, Ranges ranges, RedundantBefore redundantBefore)
    {
        drainPendingEdits();
        List<IdEntry> remove = new ArrayList<>();
        foldl(ranges, (id, es) -> {
            if (id.compareTo(lessThan) < 0 && redundantBefore.foldl(id.ranges(), InMemoryRangeSummaryIndex::prune, true, id))
                es.add(id);
            return es;
        }, remove);
        for (IdEntry e : remove)
        {
            byId.remove(e);
            value = applyMultiple(value, null, tree(e));
        }
    }

    private static Boolean prune(RedundantBefore.Bounds bounds, Boolean prune, TxnId txnId)
    {
        if (!prune) return false;
        if (bounds.maxBound(LOCALLY_APPLIED).compareTo(txnId) <= 0) return false;
        if (!txnId.isSyncPoint()) return bounds.maxBound(GC_BEFORE).compareTo(txnId) > 0;
        if (bounds.maxBound(SHARD_APPLIED).compareTo(txnId) > 0) return true;
        return bounds.maxBound(UNREADY).compareTo(txnId) < 0;
    }

    @Override
    protected Object[] tree(IdEntry edit)
    {
        if (edit.getClass() == IdMultiEntry.class)
            return toMultiTree((IdMultiEntry) edit);
        return BTree.singleton(edit);
    }

    public void restore(List<IdEntry> idEntries)
    {
        for (IdEntry original : idEntries)
        {
            IdEntry copy = original.copy();
            Invariants.require(null == byId.put(copy, copy));
            value = applyMultiple(value, tree(copy), null);
        }
    }

    public List<IdEntry> snapshot()
    {
        drainPendingEdits();
        ImmutableList.Builder<IdEntry> builder = ImmutableList.builder();
        for (IdEntry e : byId.values())
            builder.add(e.copy());
        return builder.build();
    }

    public void clear()
    {
        drainPendingEdits();
        byId.clear();
        value = IntervalBTree.empty();
    }

    @Override
    public boolean tryDrainPendingEdits()
    {
        return super.tryDrainPendingEdits();
    }

    @Override
    public Object[] drainPendingEdits()
    {
        return super.drainPendingEdits();
    }
}
