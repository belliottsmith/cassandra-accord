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

import accord.api.AsyncExecutorFactory;
import accord.api.Timeouts;
import accord.local.durability.DurabilityService;
import accord.primitives.Timestamp;
import accord.topology.TopologyManager;

public interface NodeCommandStoreService extends TimeService, UniqueTimeService, AsyncExecutorFactory
{
    long epoch();
    Node.Id id();
    Timeouts timeouts();
    DurableBefore durableBefore();
    DurabilityService durability();
    TopologyManager topology();
    long currentStamp();
    void updateStamp();

    default Timestamp uniqueTimestamp()
    {
        return uniqueTimestamp(Timestamp::fromValues);
    }

    default Timestamp uniqueTimestamp(Timestamp greaterThan)
    {
        return uniqueTimestamp(greaterThan, Timestamp::fromValues);
    }

    default <T extends Timestamp> T uniqueTimestamp(Timestamp.ValueFactory<T> factory)
    {
        long epoch = epoch();
        long now = uniqueNow();
        return factory.create(epoch, now, id());
    }

    default <T extends Timestamp> T uniqueTimestamp(Timestamp greaterThan, Timestamp.ValueFactory<T> factory)
    {
        long epoch = Math.max(epoch(), greaterThan.epoch());
        long now = uniqueNow(greaterThan.hlc());
        return factory.create(epoch, now, id());
    }
}
