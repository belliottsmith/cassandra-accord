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

import java.util.EnumSet;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.topology.Topology;

public class TopologyMismatch extends CoordinationFailed
{
    public final Topology topology;
    public final Seekables<?, ?> keysOrRanges;
    private final EnumSet<Reason> reasons;

    public enum Reason { HOME_KEY, KEYS_OR_RANGES }

    private TopologyMismatch(EnumSet<Reason> reasons, Topology topology, TxnId txnId, RoutingKey homeKey, Seekables<?, ?> keysOrRanges)
    {
        super(txnId, homeKey, buildMessage(reasons, topology, homeKey, keysOrRanges));
        this.reasons = reasons;
        this.topology = topology;
        this.keysOrRanges = keysOrRanges;
    }

    private static String buildMessage(EnumSet<Reason> reason, Topology topology, RoutingKey homeKey, Seekables<?, ?> keysOrRanges)
    {
        StringBuilder sb = new StringBuilder();
        if (reason.contains(Reason.KEYS_OR_RANGES))
            sb.append(String.format("Txn attempted to access keys or ranges %s that are not longer valid globally (%d -> %s)", keysOrRanges, topology.epoch(), topology.ranges()));
        if (reason.contains(Reason.HOME_KEY))
        {
            if (sb.length() != 0)
                sb.append('\n');
            sb.append(String.format("HomeKey %s exists for a range that is no longer globally valid (%d -> %s)", homeKey, topology.epoch(), topology.ranges()));
        }
        return sb.toString();
    }

    @Nullable
    public static TopologyMismatch checkForMismatch(Topology t, TxnId txnId, RoutingKey homeKey, Seekables<?, ?> keysOrRanges)
    {
        EnumSet<TopologyMismatch.Reason> reasons = null;
        if (!t.ranges().contains(homeKey))
        {
            if (reasons == null)
                reasons = EnumSet.noneOf(TopologyMismatch.Reason.class);
            reasons.add(TopologyMismatch.Reason.HOME_KEY);
        }
        if (!t.ranges().containsAll(keysOrRanges))
        {
            if (reasons == null)
                reasons = EnumSet.noneOf(TopologyMismatch.Reason.class);
            reasons.add(TopologyMismatch.Reason.KEYS_OR_RANGES);
        }
        return reasons == null ? null : new TopologyMismatch(reasons, t, txnId, homeKey, keysOrRanges);
    }

    public boolean hasReason(Reason reason)
    {
        return reasons.contains(reason);
    }
}
