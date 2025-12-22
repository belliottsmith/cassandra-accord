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

package accord.messages;

import accord.local.SafeCommandStore;
import accord.primitives.AbstractRanges;
import accord.primitives.Route;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.MinimalSyncPoint;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.SET_SHARD_DURABLE_REQ;
import static accord.messages.SimpleReply.Ok;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;

public class SetShardDurable extends NoWaitRequest<Route<?>, SimpleReply>
{
    public final MinimalSyncPoint syncPoint;
    public final HasOutcome durability;

    public SetShardDurable(MinimalSyncPoint syncPoint, HasOutcome durability)
    {
        super(syncPoint.syncId, syncPoint.route);
        this.syncPoint = syncPoint;
        this.durability = durability;
        Invariants.require(durability.compareTo(Quorum) >= 0);
    }

    private TxnId syncIdWithFlags()
    {
        return ((TxnId) syncPoint.executeAt).addFlag(SHARD_BOUND);
    }

    @Override
    public Cancellable submit()
    {
        Invariants.require(durability.compareTo(Quorum) >= 0);
        TxnId syncIdWithFlags = syncIdWithFlags();
        // TODO (required): does this need to strictly precede updating RedundantBefore? Because updating the global map is more expensive.
        node.markDurable(syncPoint.route.toRanges(), syncIdWithFlags, durability.compareTo(Universal) >= 0 ? syncIdWithFlags : TxnId.NONE)
        .invoke((success, fail) -> {
            if (fail != null) node.reply(replyTo, replyContext, null, fail);
            else node.commandStores().mapReduceConsume(waitForEpoch(), waitForEpoch(), this);
        });
        return null;
    }

    @Override
    public SimpleReply applyInternal(SafeCommandStore safeStore)
    {
        safeStore.commandStore().markShardDurable(safeStore, syncIdWithFlags(), ((AbstractRanges) syncPoint.route).toRanges(), durability);
        return Ok;
    }

    @Override
    protected SimpleReply refuseInternal(SafeCommandStore safeStore)
    {
        return applyInternal(safeStore);
    }

    @Override
    public SimpleReply reduce(SimpleReply r1, SimpleReply r2)
    {
        return r1.merge(r2);
    }

    @Override
    protected void acceptInternal(SimpleReply ok, Throwable failure)
    {
    }

    @Override
    public String toString()
    {
        return "SetShardDurable{" + syncPoint + '}';
    }

    @Override
    public MessageType type()
    {
        return SET_SHARD_DURABLE_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return syncPoint.syncId.epoch();
    }
}
