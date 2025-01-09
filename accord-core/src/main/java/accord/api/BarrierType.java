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

package accord.api;

public enum BarrierType
{
    // Only wait until the barrier is achieved locally, and possibly don't trigger the barrier remotely.
    // Local barriers are only on the `minEpoch` provided and have utility limited to establishing
    // no more transactions will occur in an epoch before minEpoch
    // Only keys (not ranges) are supported
    local(false, false),
    // Wait until the barrier has been achieved at a quorum globally and is also locally applied (global happens before semantics)
    global_sync(true, true),
    // Trigger the global barrier, but only block on creation of the barrier and local application (local happens before semantics)
    global_async(true, false);

    public final boolean global;

    // Whether to wait for persist to achieve quorum synchronously
    public final boolean waitOnGlobalQuorum;

    BarrierType(boolean global, boolean waitOnGlobalQuorum)
    {
        this.global = global;
        this.waitOnGlobalQuorum = waitOnGlobalQuorum;
    }
}
