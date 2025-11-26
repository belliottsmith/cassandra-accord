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
import accord.utils.UnhandledEnum;

public final class TopologyMismatch extends TopologyException
{
    private TopologyMismatch(String message)
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
    public static TopologyMismatch checkForMismatch(long epoch, Routables<?> keysOrRanges, ActiveEpochs active, Txn.Kind kind) throws TopologyException
    {
        Topology topology = active.globalForEpoch(epoch);
        Mismatch result = topology.foldlWithDefault(keysOrRanges, (shard, k, v, i) -> {
            if (shard == null)
                return Mismatch.NOT_KNOWN;
            if (shard.is(Shard.Flag.PENDING_REMOVAL) && !k.isSyncPoint())
                return Mismatch.PENDING_REMOVAL;
            return v;
        }, null, kind, null);

        if (result == null)
            return null;

        String message;
        switch (result)
        {
            default: throw new UnhandledEnum(result);
            case PENDING_REMOVAL:
                message = String.format("Txn attempted to access keys or ranges that are being removed in epoch %d (%s)", topology.epoch(), keysOrRanges);
                break;
            case NOT_KNOWN:
                message = String.format("Txn attempted to access keys or ranges that are not known in the epoch %d (%s)", topology.epoch(), keysOrRanges);
                break;
        }
        return new TopologyMismatch(message);
    }
}
