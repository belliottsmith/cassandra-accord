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

import java.util.Objects;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

public class MinimalCommand
{
    public final TxnId txnId;
    public final SaveStatus saveStatus;
    public final Status.Durability durability;
    public final StoreParticipants participants;
    public final Timestamp executeAt;

    public MinimalCommand(TxnId txnId, SaveStatus saveStatus, Status.Durability durability, StoreParticipants participants, Timestamp executeAt)
    {
        this.txnId = txnId;
        this.saveStatus = saveStatus;
        this.participants = participants;
        this.durability = durability;
        this.executeAt = executeAt != null && txnId.equalsStrict(executeAt) ? txnId : executeAt;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        MinimalCommand minimal = (MinimalCommand) object;
        return Objects.equals(txnId, minimal.txnId) && saveStatus == minimal.saveStatus && Objects.equals(participants, minimal.participants) && durability == minimal.durability && Objects.equals(executeAt, minimal.executeAt);
    }

    @Override
    public final int hashCode()
    {
        throw new UnsupportedOperationException();
    }


    public final TxnId txnId()
    {
        return txnId;
    }

    public final StoreParticipants participants()
    {
        return participants;
    }

    public final Status.Durability durability()
    {
        return durability;
    }

    public final SaveStatus saveStatus()
    {
        return saveStatus;
    }

    /**
     * We require that this is a FullRoute for all states where isDefinitionKnown().
     * In some cases, the home shard will contain an arbitrary slice of the Route where !isDefinitionKnown(),
     * i.e. when a non-home shard informs the home shards of a transaction to ensure forward progress.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     *
     * TODO (desired): caller should declare KnownRoute expectation (MaybeRoute, CoveringRoute, FullRoute) so it can be validated
     */
    @Nullable
    public final Route<?> route() { return participants().route(); }

    public Participants<?> maxParticipants()
    {
        return participants.max();
    }

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    @Nullable
    public RoutingKey homeKey()
    {
        Route<?> route = route();
        return route == null ? null : route.homeKey();
    }

    // TODO (expected): rename to e.g. timestamp(),
    //  and introduce executeAt() that first ensures it is >= precommitted,
    //  and applyAt() that confirms >= PreApplied
    public final Timestamp executeAt() { return executeAt; }

    public static abstract class MinimalWithDeps extends MinimalCommand
    {
        public MinimalWithDeps(TxnId txnId, SaveStatus saveStatus, Status.Durability durability, StoreParticipants participants, Timestamp executeAt)
        {
            super(txnId, saveStatus, durability, participants, executeAt);
        }

        abstract public PartialDeps partialDeps();
    }

    public static class MinimalWithConcreteDeps extends MinimalWithDeps
    {
        final PartialDeps partialDeps;
        public MinimalWithConcreteDeps(TxnId txnId, SaveStatus saveStatus, Status.Durability durability, StoreParticipants participants, Timestamp executeAt, PartialDeps partialDeps)
        {
            super(txnId, saveStatus, durability, participants, executeAt);
            this.partialDeps = partialDeps;
        }

        @Override
        public final PartialDeps partialDeps()
        {
            return partialDeps;
        }
    }
}
