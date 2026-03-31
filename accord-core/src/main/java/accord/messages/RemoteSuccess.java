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

import accord.api.Result;
import accord.coordinate.Coordinations;
import accord.coordinate.ExecuteTxn;
import accord.local.Node;
import accord.primitives.TxnId;

import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static accord.messages.MessageType.StandardMessage.REMOTE_SUCCESS_REQ;

public class RemoteSuccess implements Request
{
    public final TxnId txnId;
    public final Result result;

    public RemoteSuccess(TxnId txnId, Result result)
    {
        this.txnId = txnId;
        this.result = result;
    }

    @Override
    public void process(Node on, Node.Id from, ReplyContext replyContext)
    {
        report(on.coordinations(), txnId, result);
    }

    public static void report(Coordinations coordinations, TxnId txnId, Result result)
    {
        coordinations.forEach(txnId, coordination -> {
            if (coordination.kind() == Execute && coordination instanceof ExecuteTxn)
                ((ExecuteTxn) coordination).onRemoteSuccess(result);
        });
    }

    @Override
    public MessageType type()
    {
        return REMOTE_SUCCESS_REQ;
    }
}
