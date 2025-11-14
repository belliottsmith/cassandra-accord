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

package accord.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.coordinate.FetchDurableBefore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Reduce;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.primitives.Routables.Slice.Minimal;

public class Catchup
{
    private static final Logger logger = LoggerFactory.getLogger(Catchup.class);
    static class CommandStoreListener extends SettableResult<Void> implements SyncPointListener
    {
        final DurableBefore durableBefore;
        Ranges waitingOn;

        CommandStoreListener(DurableBefore durableBefore)
        {
            this.durableBefore = durableBefore;
        }

        void register(SafeCommandStore safeStore)
        {
            waitingOn = safeStore.ranges().all().slice(durableBefore.ranges(Objects::nonNull), Minimal);
            updateWaitingOn(safeStore);

            if (waitingOn.isEmpty()) setSuccess(null);
            else safeStore.register(this);
        }

        private void updateWaitingOn(SafeCommandStore safeStore)
        {
            RedundantBefore redundantBefore = safeStore.redundantBefore();
            Ranges newWaitingOn = redundantBefore.removeLostOrStale(waitingOn);
            if (newWaitingOn != waitingOn)
            {
                Ranges retiredOrStale = waitingOn.without(newWaitingOn);
                if (!retiredOrStale.isEmpty())
                    logger.info("{}: {} are retired (or stale)", safeStore.commandStore(), retiredOrStale);
            }

            newWaitingOn = durableBefore.foldlWithBounds(newWaitingOn, (DurableBefore.Entry entry, Ranges ranges, RoutingKey entryStart, RoutingKey entryEnd) -> {
                Ranges entryRanges = Ranges.of(Range.create(entryStart, entryEnd));
                return redundantBefore.foldlWithBounds(entryRanges, (RedundantBefore.Bounds bounds, Ranges rs, RoutingKey boundStart, RoutingKey boundEnd) -> {
                    TxnId locallyApplied = bounds.maxBound(LOCALLY_APPLIED);
                    if (locallyApplied.compareTo(entry.quorumBefore) >= 0)
                    {
                        Ranges boundRanges = Ranges.of(Range.create(boundStart, boundEnd));
                        Ranges caughtUp = rs.slice(boundRanges, Minimal);
                        if (!caughtUp.isEmpty())
                        {
                            rs = rs.without(boundRanges);
                            logger.info("{}: caught-up with quorum for {}; {} remaining", safeStore.commandStore(), caughtUp, rs);
                        }
                    }
                    return rs;
                }, ranges, ignore -> false);
            }, newWaitingOn, ignore -> false);

            waitingOn = newWaitingOn;
        }

        @Override
        public void update(SafeCommandStore safeStore, Command command)
        {
            if (command.saveStatus().compareTo(SaveStatus.Applied) < 0 || command.saveStatus().compareTo(SaveStatus.TruncatedUnapplied) >= 0)
                return;

            if (!command.participants().touches().intersects(waitingOn))
                return;

            updateWaitingOn(safeStore);

            if (!waitingOn.isEmpty())
                return;

            logger.info("{}: fully caught-up with quorums", safeStore.commandStore());
            setSuccess(null);
            safeStore.unregister(this);
        }
    }

    public static AsyncChain<Void> catchup(Node node)
    {
        return FetchDurableBefore.catchup(node).flatMap(durableBefore -> {
            List<AsyncResult<Void>> results = new CopyOnWriteArrayList<>();
            return node.commandStores().forAll("Catchup", safeStore -> {
                CommandStoreListener commandStoreListener = new CommandStoreListener(durableBefore);
                commandStoreListener.register(safeStore);
                results.add(commandStoreListener);
            }).flatMap(ignore -> {
                List<AsyncResult<Void>> list = new ArrayList<>(results);
                if (list.isEmpty())
                    return AsyncChains.success(null);
                return AsyncResults.reduce(list, Reduce.toNull()).chain();
            });
        });
    }
}
