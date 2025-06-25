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

import accord.coordinate.ExecutePath;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Status.Durability;
import accord.primitives.TxnId;

// TODO (required): revisit the call-sites, and boundary with C*
public interface CoordinatorEventListener
{
    default void onPreAccepted(TxnId txnId, Deps deps, boolean isStable)
    {
    }

    default void onAccepted(TxnId txnId, Ballot ballot, Deps deps, boolean isStable)
    {
    }

    default void onStabilised(TxnId txnId, Ballot ballot, Deps deps)
    {
    }

    default void onExecuting(TxnId txnId, @Nullable Ballot ballot, @Nullable ExecutePath path)
    {
    }

    default void onExecuted(TxnId txnId, Ballot ballot)
    {
    }

    default void onDurable(Durability durability, @Nullable Ballot ballot, TxnId txnId)
    {
    }

    default void onRecoveryStarted(TxnId txnId, Ballot ballot)
    {
    }

    /**
     * For use by implementations to decide what to do about successfully recovered transactions.
     * Specifically intended to define if and how they should inform clients of the result.
     * e.g. in Maelstrom we send the full result directly, in other impls we may simply acknowledge success via the coordinator
     *
     * Note: may be invoked multiple times in different places
     */
    default void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result success, Throwable fail)
    {
    }

    default void onInvalidated(TxnId txnId)
    {
    }

    default void onRejected(TxnId txnId)
    {
    }

    CoordinatorEventListener NOOP = new CoordinatorEventListener()
    {
    };
}
