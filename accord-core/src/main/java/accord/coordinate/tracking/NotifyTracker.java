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

package accord.coordinate.tracking;

import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;

public class NotifyTracker extends QuorumTracker implements ResponseTracker
{
    public static class NotifyShardTracker extends QuorumShardTracker
    {
        public NotifyShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcomes onSuccess(Object ignore)
        {
            if (++successes == shard.slowQuorumSize || hasNewFailureQuorum())
                return Success;
            return NoChange;
        }

        // return true iff hasFailed()
        public ShardOutcomes onFailure(Object ignore)
        {
            ++failures;
            return hasNewFailureQuorum() ? Success : NoChange;
        }

        private boolean hasNewFailureQuorum()
        {
            return successes + failures == shard.rf && successes < shard.slowQuorumSize;
        }

        public boolean hasReachedQuorum()
        {
            return successes >= shard.slowQuorumSize || successes + failures == shard.rf;
        }
    }

    public NotifyTracker(Topologies topologies)
    {
        super(topologies, NotifyShardTracker::new);
    }
}
