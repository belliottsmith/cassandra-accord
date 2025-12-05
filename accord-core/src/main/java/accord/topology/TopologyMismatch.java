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

package accord.topology;

import javax.annotation.Nullable;

import accord.primitives.Routables;
import accord.primitives.Txn;

import static accord.topology.SelectShards.ALL;
import static accord.topology.SelectShards.LIVE;

public final class TopologyMismatch extends TopologyException
{
    public TopologyMismatch(String message)
    {
        super(message);
    }

    private TopologyMismatch(String message, Throwable cause)
    {
        super(message, cause);
    }

    @Override
    public TopologyMismatch rethrowable()
    {
        return new TopologyMismatch(getMessage(), this);
    }

    private enum Mismatch { NOT_KNOWN, PENDING_REMOVAL }

    @Nullable
    public static TopologyMismatch checkForMismatch(long epoch, ActiveEpochs active, Routables<?> keysOrRanges, Txn.Kind kind) throws TopologyException
    {
        ActiveEpoch e = active.get(epoch);
        Topology topology = e.get(kind.isSyncPoint() ? ALL : LIVE);
        if (topology.ranges.containsAll(keysOrRanges))
            return null;

        String message;
        if (kind.isSyncPoint() || !e.all.ranges.containsAll(keysOrRanges))
            message = String.format("Txn attempted to access keys or ranges that are not known in the epoch %d (%s)", topology.epoch(), keysOrRanges.without(topology.ranges));
        else
            message = String.format("Txn attempted to access keys or ranges that are being removed in epoch %d (%s)", topology.epoch(), keysOrRanges.without(e.all.ranges));

        return new TopologyMismatch(message);
    }
}
