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

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.RoutingKey;
import accord.primitives.TxnId;
import accord.utils.Invariants;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class Timeout extends CoordinationFailed
{
    public static Timeout timeout(Agent agent, @Nullable TxnId txnId, @Nullable RoutingKey homeKey)
    {
        agent.coordinatorEvents().onTimeout(txnId);
        return new Timeout(txnId, homeKey);
    }

    public static Timeout unsafeTimeout(@Nullable TxnId txnId, @Nullable RoutingKey homeKey)
    {
        return new Timeout(txnId, homeKey);
    }

    private Timeout(@Nullable TxnId txnId, @Nullable RoutingKey homeKey)
    {
        super(txnId, homeKey);
    }

    protected Timeout(@Nullable TxnId txnId, @Nullable RoutingKey homeKey, Throwable cause)
    {
        super(txnId, homeKey, cause);
    }

    protected Timeout(@Nullable TxnId txnId, @Nullable RoutingKey homeKey, String message)
    {
        super(txnId, homeKey, message);
    }

    @Override
    public Timeout rethrowable()
    {
        Invariants.require(this.getClass() == Timeout.class);
        return new Timeout(txnId(), homeKey(), this);
    }
}
