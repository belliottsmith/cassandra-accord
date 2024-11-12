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

import java.util.EnumMap;

import accord.utils.Invariants;

public enum RedundantStatus
{
    /**
     * None of the relevant ranges are owned by the command store
     */
    NOT_OWNED,

    /**
     * None of the relevant ranges are owned by the command store anymore
     */
    WAS_OWNED,

    /**
     * None of the relevant ranges are owned by the command store anymore, and all of those ranges are closed
     * (i.e. we should know all participating commands already)
     */
    WAS_OWNED_CLOSED,

    /**
     * None of the relevant ranges are owned by the command store anymore, and all of those ranges are retired
     * (i.e. we should not participate in any decisions)
     */
    WAS_OWNED_RETIRED,

    /**
     * Some of the relevant ranges are owned by the command store and valid for execution
     */
    LIVE,

    /**
     * The relevant owned ranges are part live and part pre-bootstrap or stale
     */
    PARTIALLY_PRE_BOOTSTRAP_OR_STALE,

    /**
     * The relevant owned ranges are ALL pre-bootstrap or stale, meaning we are either
     *  1) fetching the transaction's entire result from another node's snapshot already; or
     *  2) the range is stale and _must be bootstrapped_
     */
    PRE_BOOTSTRAP_OR_STALE,

    /**
     * Some of the relevant owned ranges are redundant, meaning any intersecting transaction is known to be either applied
     * or invalidated via a sync point that has applied locally, but other ranges are known to be pre-bootstrap or stale
     * so that we cannot treat the transaction as having been fully applied locally.
     *
     * Note that this status overrides PRE_BOOTSTRAP_OR_STALE, since it implies the transaction has applied.
     */
    REDUNDANT_PRE_BOOTSTRAP_OR_STALE,

    /**
     * The relevant owned ranges are redundant, meaning any intersecting transaction is known to be either applied or invalidated
     * via a sync point that has applied locally.
     *
     * Note that this status overrides PRE_BOOTSTRAP_OR_STALE, since it implies the transaction has applied.
     */
    LOCALLY_REDUNDANT,

    /**
     * The relevant owned ranges are redundant, meaning any intersecting transaction is known to be either applied or invalidated
     * via a sync point that has applied locally AND on all healthy shards. Note that this status CANNOT be taken if
     * we are not ALSO LOCALLY_REDUNDANT, so we can use this to cleanup transactions < PreCommitted (or Erased).
     *
     * Note that this status overrides PRE_BOOTSTRAP_OR_STALE, since it implies the transaction has applied.
     */
    SHARD_REDUNDANT,

    /**
     * The relevant owned ranges are redundant, meaning any intersecting transaction is known to be either applied or invalidated
     * via a sync point that has applied locally AND on all healthy shards. Note that this status CANNOT be taken if
     * we are not ALSO LOCALLY_REDUNDANT, so we can use this to cleanup local state.
     *
     * Note that this status overrides PRE_BOOTSTRAP_OR_STALE, since it implies the transaction has applied.
     */
    GC_BEFORE,
    ;

    private EnumMap<RedundantStatus, RedundantStatus> merge;

    static
    {
        NOT_OWNED.merge = new EnumMap<>(RedundantStatus.class);
        NOT_OWNED.merge.put(NOT_OWNED, NOT_OWNED);
        NOT_OWNED.merge.put(WAS_OWNED, WAS_OWNED);
        NOT_OWNED.merge.put(WAS_OWNED_CLOSED, WAS_OWNED_CLOSED);
        NOT_OWNED.merge.put(WAS_OWNED_RETIRED, WAS_OWNED_RETIRED);
        NOT_OWNED.merge.put(LIVE, LIVE);
        NOT_OWNED.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        NOT_OWNED.merge.put(PRE_BOOTSTRAP_OR_STALE, PRE_BOOTSTRAP_OR_STALE);
        NOT_OWNED.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        NOT_OWNED.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        NOT_OWNED.merge.put(SHARD_REDUNDANT, SHARD_REDUNDANT);
        NOT_OWNED.merge.put(GC_BEFORE, GC_BEFORE);
        WAS_OWNED.merge = new EnumMap<>(RedundantStatus.class);
        WAS_OWNED.merge.put(NOT_OWNED, NOT_OWNED);
        WAS_OWNED.merge.put(WAS_OWNED, WAS_OWNED);
        WAS_OWNED.merge.put(WAS_OWNED_CLOSED, WAS_OWNED);
        WAS_OWNED.merge.put(WAS_OWNED_RETIRED, WAS_OWNED);
        WAS_OWNED.merge.put(LIVE, LIVE);
        WAS_OWNED.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED.merge.put(PRE_BOOTSTRAP_OR_STALE, PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        WAS_OWNED.merge.put(SHARD_REDUNDANT, SHARD_REDUNDANT);
        WAS_OWNED.merge.put(GC_BEFORE, GC_BEFORE);
        WAS_OWNED_CLOSED.merge = new EnumMap<>(RedundantStatus.class);
        WAS_OWNED_CLOSED.merge.put(NOT_OWNED, NOT_OWNED);
        WAS_OWNED_CLOSED.merge.put(WAS_OWNED, WAS_OWNED);
        WAS_OWNED_CLOSED.merge.put(WAS_OWNED_CLOSED, WAS_OWNED_CLOSED);
        WAS_OWNED_CLOSED.merge.put(WAS_OWNED_RETIRED, WAS_OWNED_CLOSED);
        WAS_OWNED_CLOSED.merge.put(LIVE, LIVE);
        WAS_OWNED_CLOSED.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED_CLOSED.merge.put(PRE_BOOTSTRAP_OR_STALE, PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED_CLOSED.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED_CLOSED.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        WAS_OWNED_CLOSED.merge.put(SHARD_REDUNDANT, SHARD_REDUNDANT);
        WAS_OWNED_CLOSED.merge.put(GC_BEFORE, GC_BEFORE);        
        WAS_OWNED_RETIRED.merge = new EnumMap<>(RedundantStatus.class);
        WAS_OWNED_RETIRED.merge.put(NOT_OWNED, NOT_OWNED);
        WAS_OWNED_RETIRED.merge.put(WAS_OWNED, WAS_OWNED);
        WAS_OWNED_RETIRED.merge.put(WAS_OWNED_CLOSED, WAS_OWNED_CLOSED);
        WAS_OWNED_RETIRED.merge.put(WAS_OWNED_RETIRED, WAS_OWNED_RETIRED);
        WAS_OWNED_RETIRED.merge.put(LIVE, LIVE);
        WAS_OWNED_RETIRED.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED_RETIRED.merge.put(PRE_BOOTSTRAP_OR_STALE, PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED_RETIRED.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        WAS_OWNED_RETIRED.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        WAS_OWNED_RETIRED.merge.put(SHARD_REDUNDANT, SHARD_REDUNDANT);
        WAS_OWNED_RETIRED.merge.put(GC_BEFORE, GC_BEFORE);
        LIVE.merge = new EnumMap<>(RedundantStatus.class);
        LIVE.merge.put(NOT_OWNED, LIVE);
        LIVE.merge.put(WAS_OWNED, LIVE);
        LIVE.merge.put(WAS_OWNED_CLOSED, LIVE);
        LIVE.merge.put(WAS_OWNED_RETIRED, LIVE);
        LIVE.merge.put(LIVE, LIVE);
        LIVE.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        LIVE.merge.put(PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        LIVE.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LIVE.merge.put(LOCALLY_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LIVE.merge.put(SHARD_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LIVE.merge.put(GC_BEFORE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge = new EnumMap<>(RedundantStatus.class);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(NOT_OWNED, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED_CLOSED, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED_RETIRED, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(LIVE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(LOCALLY_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(SHARD_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PARTIALLY_PRE_BOOTSTRAP_OR_STALE.merge.put(GC_BEFORE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge = new EnumMap<>(RedundantStatus.class);
        PRE_BOOTSTRAP_OR_STALE.merge.put(NOT_OWNED, PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED, PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED_CLOSED, PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED_RETIRED, PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(LIVE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, PARTIALLY_PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(PRE_BOOTSTRAP_OR_STALE, PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(LOCALLY_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(SHARD_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        PRE_BOOTSTRAP_OR_STALE.merge.put(GC_BEFORE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge = new EnumMap<>(RedundantStatus.class);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(NOT_OWNED, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED_CLOSED, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(WAS_OWNED_RETIRED, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(LIVE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE); // TODO (expected): should this be an invalid combination?
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(LOCALLY_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(SHARD_REDUNDANT, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        REDUNDANT_PRE_BOOTSTRAP_OR_STALE.merge.put(GC_BEFORE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LOCALLY_REDUNDANT.merge = new EnumMap<>(RedundantStatus.class);
        LOCALLY_REDUNDANT.merge.put(NOT_OWNED, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(WAS_OWNED, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(WAS_OWNED_CLOSED, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(WAS_OWNED_RETIRED, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(LIVE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE); // TODO (expected): should this be an invalid combination?
        LOCALLY_REDUNDANT.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LOCALLY_REDUNDANT.merge.put(PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LOCALLY_REDUNDANT.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        LOCALLY_REDUNDANT.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(SHARD_REDUNDANT, LOCALLY_REDUNDANT);
        LOCALLY_REDUNDANT.merge.put(GC_BEFORE, LOCALLY_REDUNDANT);
        SHARD_REDUNDANT.merge = new EnumMap<>(RedundantStatus.class);
        SHARD_REDUNDANT.merge.put(NOT_OWNED, SHARD_REDUNDANT);
        SHARD_REDUNDANT.merge.put(WAS_OWNED, SHARD_REDUNDANT);
        SHARD_REDUNDANT.merge.put(WAS_OWNED_CLOSED, SHARD_REDUNDANT);
        SHARD_REDUNDANT.merge.put(WAS_OWNED_RETIRED, SHARD_REDUNDANT);
        SHARD_REDUNDANT.merge.put(LIVE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE); // TODO (expected): should this be an invalid combination?
        SHARD_REDUNDANT.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        SHARD_REDUNDANT.merge.put(PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        SHARD_REDUNDANT.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        SHARD_REDUNDANT.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        SHARD_REDUNDANT.merge.put(SHARD_REDUNDANT, SHARD_REDUNDANT);
        SHARD_REDUNDANT.merge.put(GC_BEFORE, SHARD_REDUNDANT);
        GC_BEFORE.merge = new EnumMap<>(RedundantStatus.class);
        GC_BEFORE.merge.put(NOT_OWNED, SHARD_REDUNDANT);
        GC_BEFORE.merge.put(WAS_OWNED, GC_BEFORE);
        GC_BEFORE.merge.put(WAS_OWNED_CLOSED, GC_BEFORE);
        GC_BEFORE.merge.put(WAS_OWNED_RETIRED, GC_BEFORE);
        GC_BEFORE.merge.put(LIVE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE); // TODO (expected): should this be an invalid combination?
        GC_BEFORE.merge.put(PARTIALLY_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        GC_BEFORE.merge.put(PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        GC_BEFORE.merge.put(REDUNDANT_PRE_BOOTSTRAP_OR_STALE, REDUNDANT_PRE_BOOTSTRAP_OR_STALE);
        GC_BEFORE.merge.put(LOCALLY_REDUNDANT, LOCALLY_REDUNDANT);
        GC_BEFORE.merge.put(SHARD_REDUNDANT, SHARD_REDUNDANT);
        GC_BEFORE.merge.put(GC_BEFORE, GC_BEFORE);
    }

    public RedundantStatus merge(RedundantStatus that)
    {
        RedundantStatus result = merge.get(that);
        Invariants.checkState(result != null, "Invalid RedundantStatus combination: " + this + " and " + that);
        return result;
    }
}
