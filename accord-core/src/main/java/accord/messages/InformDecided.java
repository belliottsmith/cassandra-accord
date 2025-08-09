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

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.INFORM_DECIDED_REQ;

public class InformDecided extends AbstractRequest<Reply>
{
    public static class SerializationSupport
    {
        public static InformDecided create(TxnId txnId, RoutingKey homeKey)
        {
            return new InformDecided(txnId, homeKey);
        }
    }

    public final RoutingKey homeKey;
    public InformDecided(TxnId txnId, RoutingKey homeKey)
    {
        super(txnId);
        this.homeKey = homeKey;
    }

    public static void informHome(Node node, Topologies any, TxnId txnId, Route<?> route)
    {
        Shard homeShard = InformDurable.homeShard(node, any, txnId, route.homeKey());
        node.send(homeShard.nodes, to -> new InformDecided(txnId, route.homeKey()));
    }

    @Override
    public Cancellable submit()
    {
        // TODO (expected): do not load from disk to perform this update, just write a delta to any journal
        return node.mapReduceConsumeLocal(this, homeKey, txnId.epoch(), this);
    }

    @Override
    public Reply apply(SafeCommandStore safeStore)
    {
        safeStore.progressLog().decided(safeStore, txnId);
        return null;
    }

    @Override
    public TxnId primaryTxnId()
    {
        // we don't need the transaction loaded to update the progress log
        return null;
    }

    @Override
    protected void acceptInternal(Reply reply, Throwable failure)
    {
    }

    @Override
    public String toString()
    {
        return "InformDecided{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return INFORM_DECIDED_REQ;
    }
}
