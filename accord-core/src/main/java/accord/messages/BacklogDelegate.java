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
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.BACKLOG_DELEGATE_REQ;

/**
 * Sent by a transaction's original coordinator to a replica in another DC, asking it to execute the transaction on the
 * coordinator's behalf, as the DC is executing the transaction's predecessors (see
 * {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionFollow()}). The replica replies {@link SimpleReply#Ok}
 * if it accepts, and is then obligated to execute the transaction, persisting it and reporting the result to the
 * coordinator ({@link RemoteSuccess}); otherwise {@link SimpleReply#Nack}, in which case the coordinator resumes its
 * own execution (as it does if the replica does not reply, or does not report success promptly).
 * Note: this message was introduced later, and so may only be sent once every replica understands it.
 */
public class BacklogDelegate implements Request
{
    public final TxnId txnId;
    public final FullRoute<?> route;
    public final Timestamp executeAt;

    public BacklogDelegate(TxnId txnId, FullRoute<?> route, Timestamp executeAt)
    {
        this.txnId = txnId;
        this.route = route;
        this.executeAt = executeAt;
    }

    @Override
    public Cancellable process(Node on, Node.Id from, ReplyContext replyContext)
    {
        on.executeBacklogSink().onDelegated(txnId, route, executeAt, from, replyContext);
        return null;
    }

    @Override
    public long waitForEpoch()
    {
        return executeAt.epoch();
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public MessageType type()
    {
        return BACKLOG_DELEGATE_REQ;
    }

    @Override
    public String toString()
    {
        return "BacklogDelegate{" + txnId + '}';
    }
}
