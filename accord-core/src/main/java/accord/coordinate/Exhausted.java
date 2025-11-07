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

import java.util.Collection;
import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.Invariants;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class Exhausted extends CoordinationFailed
{
    final @Nullable Ranges failedRanges;
    final @Nullable Collection<Node.Id> failedNodes;

    public static Exhausted exhausted(Agent agent, @Nullable TxnId txnId, @Nullable RoutingKey homeKey, @Nullable Ranges failedRanges)
    {
        return exhausted(agent, txnId, homeKey, failedRanges, null);
    }

    public static Exhausted exhausted(Agent agent, @Nullable TxnId txnId, @Nullable RoutingKey homeKey, @Nullable Ranges failedRanges, @Nullable Collection<Node.Id> failedNodes)
    {
        agent.coordinatorEvents().onExhausted(txnId);
        return new Exhausted(txnId, homeKey, failedRanges, failedNodes);
    }

    private Exhausted(TxnId txnId, @Nullable RoutingKey homeKey, Ranges failedRanges, @Nullable Collection<Node.Id> failedNodes)
    {
        super(txnId, homeKey, getMessage(txnId, failedRanges, failedNodes));
        this.failedRanges = failedRanges;
        this.failedNodes = failedNodes;
    }

    Exhausted(TxnId txnId, @Nullable RoutingKey homeKey, Ranges failedRanges, @Nullable Collection<Node.Id> failedNodes, Exhausted cause)
    {
        super(txnId, homeKey, cause);
        this.failedRanges = failedRanges;
        this.failedNodes = failedNodes;
    }

    public @Nullable Collection<Node.Id> failedNodes()
    {
        return failedNodes;
    }

    public @Nullable Ranges failedRanges()
    {
        return failedRanges;
    }

    private static String getMessage(TxnId txnId, @Nullable Ranges unavailable, @Nullable Collection<Node.Id> failed)
    {
        String msg = "No more nodes to try for " + txnId;
        if (unavailable != null)
            msg += " for ranges " + unavailable;
        if (failed != null)
            msg += ". Failed to contact: " + failed;
        return msg;
    }

    @Override
    public Exhausted rethrowable()
    {
        Invariants.require(this.getClass() == Exhausted.class);
        return new Exhausted(txnId(), homeKey(), failedRanges, failedNodes, this);
    }
}
