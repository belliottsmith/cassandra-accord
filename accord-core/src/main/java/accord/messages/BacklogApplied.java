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
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.BACKLOG_APPLIED_REQ;

/**
 * Sent by a designated replica (see {@link accord.local.cfk.ExecuteTxnBacklog}) that has executed a transaction and
 * persisted it only to the replicas in its own DC, to tell the transaction's original coordinator which replicas have
 * applied it. The original coordinator counts these replicas towards the transaction's durability, and need not send
 * them its own Apply (see {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionPersistLocally()}).
 * Replicas the designated replica failed to apply the transaction to are reported too; the original coordinator records
 * them as failures in its durability tracker, just as if it had tried them itself. If the designated replica cannot
 * execute the transaction at all it sends no report, and the original coordinator sends its own Apply to its DC once
 * its remote apply delay elapses.
 * The original coordinator replies {@link SimpleReply#Ok} once it has begun to persist the transaction (so has taken
 * responsibility for its durability), or {@link SimpleReply#Nack} if it failed to execute the transaction. If the
 * designated replica receives a Nack or no reply (e.g. because the original coordinator has failed), it persists the
 * transaction to every other replica itself.
 * Note: this message was introduced later, and so may only be sent once every replica understands it.
 */
public class BacklogApplied implements Request
{
    public final TxnId txnId;
    // the replicas that have applied the transaction, and those the designated replica failed to apply it to (e.g.
    // because they are down), which the original coordinator records as failures (rather than trying them itself)
    public final SortedArrayList<Node.Id> appliedAt, failedAt;

    public BacklogApplied(TxnId txnId, SortedArrayList<Node.Id> appliedAt, SortedArrayList<Node.Id> failedAt)
    {
        this.txnId = txnId;
        this.appliedAt = appliedAt;
        this.failedAt = failedAt;
    }

    @Override
    public Cancellable process(Node on, Node.Id from, ReplyContext replyContext)
    {
        on.executeBacklogSink().onApplied(txnId, from, appliedAt, failedAt, replyContext);
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
        return BACKLOG_APPLIED_REQ;
    }

    @Override
    public String toString()
    {
        return "BacklogApplied{" + txnId + ", appliedAt=" + appliedAt + ", failedAt=" + failedAt + '}';
    }
}
