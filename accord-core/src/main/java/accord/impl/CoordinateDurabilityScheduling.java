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

package accord.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.CoordinateGloballyDurable;
import accord.coordinate.CoordinateShardDurable;
import accord.coordinate.CoordinateSyncPoint;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.topology.Topology;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;

/**
 * Helper methods and classes to invoke coordination to propagate information about durability
 *
 * // TODO review will the scheduler shut down on its own or do the individual tasks need to be canceled manually?
 * Didn't go with recurring because it doesn't play well with async execution of these tasks
 */
public class CoordinateDurabilityScheduling
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinateDurabilityScheduling.class);

    /*
     * Divide each range into N steps and then every interval coordinate shard durable for the range step subrange of each range
     */
    static final int RANGE_STEPS = 10;

    /*
     * How often this node will attempt to do a coordinate shard durable step
     */
    static final int COORDINATE_SHARD_DURABLE_INTERVAL_MS = 100;

    /*
     * Every node will independently attempt to invoke CoordinateGloballyDurable
     * with a target gap between invocations of COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MS.
     *
     * This is done by nodes taking turns for each scheduled attempt that is due by calculating what # attempt is
     * next for the current node ordinal in the cluster since the unix epoch and attempting to invoke then. If all goes
     * well they end up doing it periodically in a timely fashion with the target gap achieved.
     *
     */
    static final long COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS = TimeUnit.MILLISECONDS.toMicros(1000);

    public static void scheduleDurabilityPropagation(Node node)
    {
        scheduleCoordinateShardDurable(node, 0);
        scheduleCoordinateGloballyDurable(node);
    }

    private static void scheduleCoordinateShardDurable(Node node, long attemptIndex)
    {
        node.scheduler().once(getCoordinateShardDurableRunnable(node, attemptIndex), COORDINATE_SHARD_DURABLE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private static Runnable getCoordinateShardDurableRunnable(Node node, long attemptIndex)
    {
        return () -> {
            try
            {
                rangesDurable(node, attemptIndex);
            }
            catch (Exception e)
            {
                logger.error("Exception coordinating local shard durability", e);
            }
        };
    }

    private static void rangesDurable(Node node, long attemptIndex)
    {
        Topology topology = node.topology().current();
        CommandStores commandStores = node.commandStores();
        ShardDistributor distributor = commandStores.shardDistributor();
        Ranges nodeRanges = topology.rangesForNode(node.id());

        // During startup or when there is nothing to do just skip
        // since it generates a lot of noisy errors if you move forward
        // with no shards
        if (nodeRanges.isEmpty() || commandStores.count() == 0)
        {
            scheduleCoordinateShardDurable(node, attemptIndex);
            return;
        }

        int currentStep = Ints.checkedCast(attemptIndex % RANGE_STEPS);

        // In each step coordinate shard durability for a subset of every range on the node.
        Range[] slices = new Range[nodeRanges.size()];
        for (int i = 0; i < nodeRanges.size(); i++)
        {
            Range r = nodeRanges.get(i);
            slices[i] = distributor.splitRange(r, currentStep, RANGE_STEPS);
        }

        // Sorted and deoverlapped property should be preserved after splitting
        CoordinateSyncPoint.exclusive(node, Ranges.ofSortedAndDeoverlapped(slices))
                .addCallback((success, fail) -> {
                    if (fail != null)
                    {
                        logger.error("Exception coordinating exclusive sync point for local shard durability", fail);
                        // On failure don't increment attempt index
                        scheduleCoordinateShardDurable(node, attemptIndex);
                    }
                    else if (success != null)
                    {
                        coordinateDurable(node, success, attemptIndex);
                    }
                });
    }

    private static void coordinateDurable(Node node, SyncPoint exclusiveSyncPoint, long attemptIndex)
    {
        CoordinateShardDurable.coordinate(node, exclusiveSyncPoint, Collections.emptySet())
                .addCallback((success0, fail0) -> {
                    if (fail0 != null)
                    {
                        logger.error("Exception coordinating local shard durability", fail0);
                        coordinateDurable(node, exclusiveSyncPoint, attemptIndex);
                    }
                    else
                    {
                        // Schedule the next one with the next index to do the next set of ranges
                        scheduleCoordinateShardDurable(node, attemptIndex + 1);
                    }
                });
    }

    private static void scheduleCoordinateGloballyDurable(Node node)
    {
        node.scheduler().once(getNextCoordinateGloballyDurableRunnable(node), getNextCoordinateGloballyDurableWaitMicros(node), TimeUnit.MICROSECONDS);
    }

    private static Runnable getNextCoordinateGloballyDurableRunnable(Node node)
    {
        return () -> {
            try
            {
                // During startup or when there is nothing to do just skip
                // since it generates a lot of noisy errors if you move forward
                // with no shards
                Ranges nodeRanges = node.topology().current().rangesForNode(node.id());
                if (nodeRanges.isEmpty() || node.commandStores().count() == 0)
                {
                    scheduleCoordinateGloballyDurable(node);
                    return;
                }

                long epoch = node.epoch();
                // TODO review Wanted this to execute async and not block the scheduler
                AsyncChain<AsyncResult<Void>> resultChain = node.withEpoch(epoch, () -> node.commandStores().any().submit(() -> CoordinateGloballyDurable.coordinate(node, epoch)));
                // TODO This has the potential to schedule multiple if both callbacks run such as coordination starts, and something causees failure after
                AtomicBoolean scheduledFollowup = new AtomicBoolean(false);
                resultChain.begin((success, failure) -> {
                    if (failure != null)
                    {
                        logger.error("Exception coordinating global durability", failure);
                        if (scheduledFollowup.compareAndSet(false, true))
                            scheduleCoordinateGloballyDurable(node);
                    }
                    else
                    {
                        success.addCallback((success2, failure2) -> {
                            if (failure2 != null)
                                logger.error("Exception coordinating global durability", failure2);
                            if (scheduledFollowup.compareAndSet(false, true))
                                scheduleCoordinateGloballyDurable(node);
                        });
                    }
                });
            }
            catch (Exception e)
            {
                logger.error("Exception coordinating global durability", e);
            }
        };
    }

    private static long getNextCoordinateGloballyDurableWaitMicros(Node node)
    {
        // Calculate an ordinal using the current topology
        // TODO cache this? Can it be stored in Topology?
        List<Id> ids = new ArrayList<>(node.topology().current().nodes());
        // If initialization isn't done check again in 1 second
        if (ids.isEmpty())
            return TimeUnit.SECONDS.toMicros(1);
        Collections.sort(ids);
        int ourIndex = ids.indexOf(node.id());

        long nowMicros = node.now();
        // How long it takes for all nodes to go once
        long totalRoundDuration = ids.size() * COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS;
        long startOfCurrentRound = (nowMicros / totalRoundDuration) * totalRoundDuration;

        // In a given round at what time in the round should this node take its turn
        long ourOffsetInRound = ourIndex * COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS;

        long targetTimeInCurrentRound = startOfCurrentRound + ourOffsetInRound;
        long targetTime = targetTimeInCurrentRound;
        // If our time to run in the current round already passed then schedule it in the next round
        if (targetTimeInCurrentRound < nowMicros)
            targetTime += totalRoundDuration;

        return targetTime - nowMicros;
    }
}