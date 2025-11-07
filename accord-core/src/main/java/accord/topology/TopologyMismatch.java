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

import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.utils.UnhandledEnum;

public final class TopologyMismatch extends TopologyException
{
    public enum TopologyMatch
    {
        /**
         * All participating topologies are expected to contain all participating keys.
         * This is for user transactions, which must operate at all times on topologies
         * consistent with the operation.
         */
        LATEST,

        /**
         * All participating keys are expected to be contained in SOME participating epoch.
         * This is used for sync points which may be run after some range has been removed.
         */
        ANY
    }

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

    @Nullable
    public static TopologyMismatch checkForMismatch(long epoch, Routables<?> keysOrRanges, ActiveEpochs active, TopologyMatch match) throws TopologyException
    {
        switch (match)
        {
            default: throw new UnhandledEnum(match);
            case ANY:
            {
                long e = Math.min(active.currentEpoch, epoch);
                while (e >= active.minEpoch())
                {
                    Ranges rs = active.getKnown(e).global.ranges();
                    if (rs.containsAll(keysOrRanges))
                        return null;

                    keysOrRanges = keysOrRanges.without(rs);
                    --e;
                }

                String message = String.format("Txn attempted to access keys or ranges that are not known in any epoch (%s)", keysOrRanges);
                return new TopologyMismatch(message);
            }
            case LATEST:
            {
                Topology topology = active.globalForEpoch(epoch);
                if (topology.ranges().containsAll(keysOrRanges))
                    return null;

                String message = String.format("Txn attempted to access keys or ranges that are not known in the epoch %d (%s)", topology.epoch(), keysOrRanges);
                return new TopologyMismatch(message);
            }
        }
    }
}
