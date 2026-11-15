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

import accord.local.Node;
import accord.primitives.TxnId;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.BACKLOG_CLAIM_REQ;

/**
 * Sent by a backlog executor (see {@link accord.local.cfk.ExecuteTxnBacklog}) to a transaction's original
 * coordinator, to let it know the backlog executor intends to execute the transaction, so that the original
 * coordinator may pause its own attempt and await a {@link RemoteSuccess} (resuming if none arrives promptly).
 * Note: this message was introduced later, and so may only be sent once every replica understands it
 * (see {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionClaims()}).
 */
public class BacklogClaim implements Request
{
    public final TxnId txnId;

    public BacklogClaim(TxnId txnId)
    {
        this.txnId = txnId;
    }

    @Override
    public Cancellable process(Node on, Node.Id from, ReplyContext replyContext)
    {
        on.executeBacklogSink().onClaimed(txnId, from);
        return null;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public MessageType type()
    {
        return BACKLOG_CLAIM_REQ;
    }

    @Override
    public String toString()
    {
        return "BacklogClaim{" + txnId + '}';
    }
}
