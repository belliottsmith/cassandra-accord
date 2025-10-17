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

import javax.annotation.Nullable;

import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.ActiveEpoch;
import accord.topology.Topology;

public interface TopologyListener
{
    /**
     * Informs listeners of new topology. This is guaranteed to be called sequentially for each epoch after
     * the initial topology returned by `currentTopology` on startup.
     * <p>
     * TODO (desired): document what this Future represents, or maybe refactor it away - only used for testing
     */
    default void onReceived(Topology topology) {}

    /**
     * Informs listeners of new topology. This is guaranteed to be called sequentially for each epoch after
     * the initial topology returned by `currentTopology` on startup.
     * <p>
     * TODO (desired): document what this Future represents, or maybe refactor it away - only used for testing
     */
    default void onActive(ActiveEpoch epoch) {}

    /**
     * Called when accord data associated with a superseded epoch has been sync'd from previous replicas.
     * This should be invoked on each replica once EpochReady.coordination has returned on a replica.
     * <p>
     * Once a quorum of these notifications have been received, no new TxnId may be executed in this epoch
     * (though this is not a transitive property; earlier epochs may yet agree to execute TxnId if they have not been sync'd)
     */
    default void onRemoteReadyToCoordinate(Node.Id node, long epoch) {}

    /**
     * Called when no new TxnId may be agreed with an epoch less than or equal to the provided one.
     * This means future epochs are now aware of all TxnId with this epoch or earlier that may be executed
     * on this range.
     */
    default void onEpochClosed(Ranges ranges, long epoch, @Nullable Topology topology) {}

    /**
     * Called when all TxnId with an epoch equal to or before this that interact with this range have been executed,
     * in whatever epoch they execute in. Once the whole range is covered this epoch is redundant, and may be cleaned up.
     */
    default void onEpochRetired(Ranges ranges, long epoch, @Nullable Topology topology) {}

    default void onEpochRemoved(long epoch) {}

    default void onReadyToCoordinate(Topology topology) {}
}
