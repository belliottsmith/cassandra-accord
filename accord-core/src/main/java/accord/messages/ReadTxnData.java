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

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Node;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.messages.MessageType.StandardMessage.READ_REQ;
import static accord.primitives.SaveStatus.ReadyToExecute;

public class ReadTxnData extends ReadData
{
    public static class SerializerSupport
    {
        public static ReadTxnData create(TxnId txnId, Participants<?> scope, @Nullable PartialTxn partialTxn, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
        {
            return new ReadTxnData(txnId, scope, partialTxn, executeAt, executeAtEpoch, flags);
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(ReadyToExecute, ReadyToExecute);

    public ReadTxnData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, @Nullable Txn txn, @Nullable Timestamp executeAt, long executeAtEpoch)
    {
        super(to, topologies, txnId, readScope, txn, executeAt, executeAtEpoch);
    }

    public ReadTxnData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, @Nullable Txn txn, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
    {
        super(to, topologies, txnId, readScope, txn, executeAt, executeAtEpoch, flags);
    }

    public ReadTxnData(TxnId txnId, Participants<?> readScope, @Nullable PartialTxn partialTxn, @Nullable Timestamp executeAt, long executeAtEpoch)
    {
        super(txnId, readScope, partialTxn, executeAt, executeAtEpoch);
    }

    public ReadTxnData(TxnId txnId, Participants<?> readScope, @Nullable PartialTxn partialTxn, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
    {
        super(txnId, readScope, partialTxn, executeAt, executeAtEpoch, flags);
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readTxnData;
    }

    @Override
    public MessageType type()
    {
        return READ_REQ;
    }

    @Override
    public String reason()
    {
        return "Read";
    }
}
