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

import javax.annotation.Nullable;

import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.local.StoreParticipants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.Toggles.DependencyElision.IF_DURABLE;
import static accord.api.ProtocolModifiers.Toggles.dependencyElision;
import static accord.api.ProtocolModifiers.Toggles.informOfDurability;
import static accord.messages.SimpleReply.Ok;

public class InformDurable extends TxnRequest<Reply> implements PreLoadContext
{
    public static class SerializationSupport
    {
        public static InformDurable create(TxnId txnId, Route<?> scope, Timestamp executeAt, long minEpoch, long waitForEpoch, long maxEpoch, Durability durability)
        {
            return new InformDurable(txnId, scope, executeAt, minEpoch, waitForEpoch, maxEpoch, durability);
        }
    }

    public final @Nullable Timestamp executeAt;
    public final long minEpoch, maxEpoch;
    public final Durability durability;

    public InformDurable(Id to, Topologies topologies, Route<?> route, TxnId txnId, @Nullable Timestamp executeAt, long minEpoch, long maxEpoch, Durability durability)
    {
        super(to, topologies, route, txnId);
        this.executeAt = executeAt;
        this.minEpoch = minEpoch;
        this.maxEpoch = maxEpoch;
        this.durability = durability;
    }

    private InformDurable(TxnId txnId, Route<?> scope, @Nullable Timestamp executeAt, long minEpoch, long waitForEpoch, long maxEpoch, Durability durability)
    {
        super(txnId, scope, waitForEpoch);
        this.executeAt = executeAt;
        this.minEpoch = minEpoch;
        this.maxEpoch = maxEpoch;
        this.durability = durability;
    }

    public static void informDefault(Node node, Topologies any, TxnId txnId, Route<?> route, Timestamp executeAt, Durability durability)
    {
        switch (informOfDurability())
        {
            default: throw new AssertionError("Unhandled InformOfDurability: " + informOfDurability());
            case ALL: informAll(node, any, txnId, route, executeAt, durability); break;
            case HOME: informHome(node, any, txnId, route, executeAt, durability);
        }
    }

    public static void informHome(Node node, Topologies any, TxnId txnId, Route<?> route, Timestamp executeAt, Durability durability)
    {
        long homeEpoch = txnId.epoch();
        Topology homeEpochTopology = any.getEpoch(homeEpoch);
        int homeShardIndex = homeEpochTopology.indexForKey(route.homeKey());
        if (homeShardIndex < 0)
        {
            homeEpochTopology = node.topology().globalForEpoch(homeEpoch);
            homeShardIndex = homeEpochTopology.indexForKey(route.homeKey());
        }

        Shard homeShard = homeEpochTopology.get(homeShardIndex);
        Topologies homeTopology = new Topologies.Single(any, new Topology(homeEpoch, homeShard));
        node.send(homeShard.nodes, to -> new InformDurable(to, homeTopology, route.homeKeyOnlyRoute(), txnId, executeAt, txnId.epoch(), txnId.epoch(), durability));
    }

    public static void informAll(Node node, Topologies inform, TxnId txnId, Route<?> route, Timestamp executeAt, Durability durability)
    {
        node.send(inform.nodes(), to -> new InformDurable(to, inform, route, txnId, executeAt, inform.oldestEpoch(), inform.currentEpoch(), durability));
    }

    @Override
    public Cancellable submit()
    {
        // TODO (expected): do not load from disk to perform this update, just write a delta to any journal
        return node.mapReduceConsumeLocal(txnId, scope, minEpoch, maxEpoch, this);
    }

    @Override
    public Reply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, maxEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        if (safeCommand.current().is(Status.Truncated))
            return Ok;

        Commands.setDurability(safeStore, safeCommand, participants, durability, executeAt);
        return Ok;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return dependencyElision() == IF_DURABLE ? KeyHistory.ASYNC : KeyHistory.NONE;
    }

    @Override
    public Reply reduce(Reply o1, Reply o2)
    {
        return Ok;
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_DURABLE_REQ;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }
}
