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

package accord.burn;

import javax.annotation.Nullable;

import accord.api.ProtocolModifiers.ReplicaExecution;

/**
 * Optional overrides of the burn test's randomly chosen workload, read from system properties (see
 * {@link HappyBurnTest} for the defaults it supplies). Each override is applied after the corresponding random
 * choice is made, so that the random stream is not perturbed and runs remain comparable for a given seed.
 * <ul>
 *   <li>{@code accord.burn.keys}: the number of keys per prefix</li>
 *   <li>{@code accord.burn.prefixes}: the number of prefixes; only the first is in the initial topology, so
 *       this matters only if the topology changes</li>
 *   <li>{@code accord.burn.concurrency}: the number of concurrent client requests</li>
 *   <li>{@code accord.burn.range_ratio}: the proportion of transactions that are range reads</li>
 *   <li>{@code accord.burn.replica_execution}: see {@link accord.api.ProtocolModifiers#executeAtReplica}</li>
 *   <li>{@code accord.burn.hot_keys} and {@code accord.burn.hot_ratio}: a proportion {@code hot_ratio} of key
 *       transactions draw all of their keys from the first {@code hot_keys} keys, so that these keys are
 *       frequently accessed together, producing long chains of conflicting transactions spanning these keys (and
 *       typically shards). Since clients pick coordinators at random, consecutive links of these chains are
 *       typically coordinated in different DCs.</li>
 * </ul>
 */
class BurnWorkload
{
    final @Nullable Integer keys = Integer.getInteger("accord.burn.keys");
    final @Nullable Integer prefixes = Integer.getInteger("accord.burn.prefixes");
    final @Nullable Integer concurrency = Integer.getInteger("accord.burn.concurrency");
    final @Nullable Float rangeRatio = getFloat("accord.burn.range_ratio");
    final @Nullable ReplicaExecution replicaExecution = System.getProperty("accord.burn.replica_execution") == null ? null
                                                         : ReplicaExecution.valueOf(System.getProperty("accord.burn.replica_execution"));
    final int hotKeys = Integer.getInteger("accord.burn.hot_keys", 0);
    final float hotRatio = hotKeys == 0 ? 0f : getFloat("accord.burn.hot_ratio", 0.5f);

    BurnWorkload()
    {
        // a transaction may draw two distinct keys from the hot set
        if (hotKeys == 1)
            throw new IllegalArgumentException("accord.burn.hot_keys must be 0 or at least 2");
    }

    int keys(int random) { return keys == null ? random : Math.max(keys, Math.max(2, hotKeys)); }
    int prefixes(int random) { return prefixes == null ? random : prefixes; }
    int concurrency(int random) { return concurrency == null ? random : concurrency; }
    float rangeRatio(float random) { return rangeRatio == null ? random : rangeRatio; }
    ReplicaExecution replicaExecution(ReplicaExecution random) { return replicaExecution == null ? random : replicaExecution; }

    private static @Nullable Float getFloat(String property)
    {
        String value = System.getProperty(property);
        return value == null ? null : Float.parseFloat(value);
    }

    private static float getFloat(String property, float defaultValue)
    {
        Float value = getFloat(property);
        return value == null ? defaultValue : value;
    }

    @Override
    public String toString()
    {
        return "keys=" + (keys == null ? "random" : keys)
               + ", prefixes=" + (prefixes == null ? "random" : prefixes)
               + ", concurrency=" + (concurrency == null ? "random" : concurrency)
               + ", rangeRatio=" + (rangeRatio == null ? "random" : rangeRatio)
               + ", replicaExecution=" + (replicaExecution == null ? "random" : replicaExecution)
               + (hotKeys == 0 ? "" : ", hotKeys=" + hotKeys + ", hotRatio=" + hotRatio);
    }
}
