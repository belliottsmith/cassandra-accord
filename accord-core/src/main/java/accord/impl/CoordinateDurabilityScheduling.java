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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.CoordinateGloballyDurable;
import accord.coordinate.CoordinateShardDurable;
import accord.coordinate.CoordinateSyncPoint;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.topology.Topology;
import accord.topology.TopologyManager.NodeIndexAndCountInCurrentEpoch;
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
     * Divide each range into N steps and then every interval coordinate local shard durable for the range step subrange of every range on the node
     */
    static final int RANGE_STEPS = 10;

    /*
     * How often this node will attempt to do a coordinate shard durable step
     */
    static final int COORDINATE_SHARD_DURABLE_INTERVAL_MS = 100;

    /*
     * Every node will independently attempt to invoke CoordinateGloballyDurable
     * with a target gap between invocations of COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS.
     *
     * This is done by nodes taking turns for each scheduled attempt that is due by calculating what # attempt is
     * next for the current node ordinal in the cluster and time since the unix epoch and attempting to invoke then. If all goes
     * well they end up doing it periodically in a timely fashion with the target gap achieved.
     *
     */
    static final long COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS = TimeUnit.MILLISECONDS.toMicros(1000);

    /**
     * Schedule regular invocations of CoordinateShardDurable and CoordinateGloballyDurable
     */
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
                coordinateExclusiveSyncPointForCoordinateShardDurable(node, attemptIndex);
            }
            catch (Exception e)
            {
                logger.error("Exception in initial range calculation and initiation of exclusive sync point for local shard durability", e);
            }
        };
    }

    /**
     * The first step for coordinating shard durable is to run an exclusive sync point
     * the result of which can then be used to run
     */
    private static void coordinateExclusiveSyncPointForCoordinateShardDurable(Node node, long attemptIndex)
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
                        // On failure don't increment attemptIndex
                        // TODO review is it better to increment the index so if there is a stuck portion we will move past it and at least
                        // make some progress?
                        scheduleCoordinateShardDurable(node, attemptIndex);
                    }
                    else if (success != null)
                    {
                        coordinateShardDurableAfterExclusiveSyncPoint(node, success, attemptIndex);
                    }
                });
    }

    private static void coordinateShardDurableAfterExclusiveSyncPoint(Node node, SyncPoint exclusiveSyncPoint, long attemptIndex)
    {
        CoordinateShardDurable.coordinate(node, exclusiveSyncPoint, Collections.emptySet())
                .addCallback((success, fail) -> {
                    if (fail != null)
                    {
                        logger.error("Exception coordinating local shard durability", fail);
                        // On failure don't increment attempt index
                        // TODO review is it better to increment the index so if there is a stuck portion we will move past it and at least
                        // make some progress?
                        coordinateShardDurableAfterExclusiveSyncPoint(node, exclusiveSyncPoint, attemptIndex);
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
        node.scheduler().once(getCoordinateGloballyDurableRunnable(node), getNextCoordinateGloballyDurableWaitMicros(node), TimeUnit.MICROSECONDS);
    }

    private static Runnable getCoordinateGloballyDurableRunnable(Node node)
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
                resultChain.begin((success, fail) -> {
                    if (fail != null)
                    {
                        logger.error("Exception initiating coordination in withEpoch for global durability", fail);
                        scheduleCoordinateGloballyDurable(node);
                    }
                    else
                    {
                        success.addCallback(coordinateGloballyDurableCallback(node));
                    }
                });
            }
            catch (Exception e)
            {
                logger.error("Exception invoking withEpoch to start coordination for global durability", e);
            }
        };
    }

    private static BiConsumer<Void, Throwable> coordinateGloballyDurableCallback(Node node)
    {
        return (success, fail) -> {
            if (fail != null)
                logger.error("Exception coordinating global durability", fail);
            // Reschedule even on failure because we never stop need to propagate global durability
            scheduleCoordinateGloballyDurable(node);
        };
    }

    private static long getNextCoordinateGloballyDurableWaitMicros(Node node)
    {
        return getNextTurn(node, COORDINATE_GLOBALLY_DURABLE_TARGET_GAP_MICROS);
    }

    /**
     * Based on the current unix time (simulated or otherwise) calculate the wait time in microseconds until the next turn of this
     * node for some activity with a target gap between nodes doing the activity.
     *
     * This is done by taking the index of the node in the current topology and the total number of nodes
     * and then using the target gap between invocations to calculate a "round" duration and point of time each node
     * should have its turn in each round based on its index and calculating the time to the next turn for that node will occur.
     *
     * It's assumed it is fine if nodes overlap or reorder or skip for whatever activity we are picking turns for as long as it is approximately
     * the right pacing.
     */
    private static long getNextTurn(Node node, long targetGapMicros)
    {
        NodeIndexAndCountInCurrentEpoch nodeIndexAndCountInCurrentEpoch = node.topology().nodeIndexAndNodeCountInCurrentEpoch();
        // Empty epochs happen during node removal, startup, and tests so check again in 1 second
        if (nodeIndexAndCountInCurrentEpoch == null)
            return TimeUnit.SECONDS.toMicros(1);

        int ourIndex = nodeIndexAndCountInCurrentEpoch.ourIndex;
        long nowMicros = node.unix(TimeUnit.MICROSECONDS);
        // How long it takes for all nodes to go once
        long totalRoundDuration = nodeIndexAndCountInCurrentEpoch.numNodesInEpoch * targetGapMicros;
        long startOfCurrentRound = (nowMicros / totalRoundDuration) * totalRoundDuration;

        // In a given round at what time in the round should this node take its turn
        long ourOffsetInRound = ourIndex * targetGapMicros;

        long targetTimeInCurrentRound = startOfCurrentRound + ourOffsetInRound;
        long targetTime = targetTimeInCurrentRound;
        // If our time to run in the current round already passed then schedule it in the next round
        if (targetTimeInCurrentRound < nowMicros)
            targetTime += totalRoundDuration;

        return targetTime - nowMicros;
    }
}