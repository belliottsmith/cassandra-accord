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

import accord.api.RoutingKey;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.utils.Invariants;

/**
 * Reported when an opportunistic coordination (e.g. backlog execution) abandons its attempt
 * because some replica it must read from is not yet ready to execute the transaction.
 * This is an expected outcome: the transaction will be executed by some other coordinator once it is ready.
 */
public class NotReady extends CoordinationFailed
{
    public static NotReady notReady(TxnId txnId, @Nullable Route<?> route)
    {
        return new NotReady(txnId, route == null ? null : route.homeKey());
    }

    private NotReady(TxnId txnId, @Nullable RoutingKey homeKey)
    {
        super(txnId, homeKey);
    }

    private NotReady(TxnId txnId, @Nullable RoutingKey homeKey, NotReady cause)
    {
        super(txnId, homeKey, cause);
    }

    @Override
    public NotReady rethrowable()
    {
        Invariants.require(this.getClass() == NotReady.class);
        return new NotReady(txnId(), homeKey(), this);
    }
}
