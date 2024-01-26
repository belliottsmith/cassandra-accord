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

import javax.annotation.Nonnull;

import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;

public class ReadEphemeralTxnData extends ReadTxnData
{
    public static class SerializerSupport
    {
        public static ReadEphemeralTxnData create(TxnId txnId, Participants<?> scope, long waitForEpoch, long executeAtEpoch, @Nonnull PartialTxn partialTxn, @Nonnull PartialDeps partialDeps)
        {
            return new ReadEphemeralTxnData(txnId, scope, waitForEpoch, executeAtEpoch, partialTxn, partialDeps);
        }
    }

    public final @Nonnull PartialTxn partialTxn;
    public final @Nonnull PartialDeps partialDeps;

    public ReadEphemeralTxnData(Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, long executeAtEpoch, @Nonnull PartialTxn partialTxn, @Nonnull PartialDeps partialDeps)
    {
        super(to, topologies, txnId, readScope, executeAtEpoch);
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
    }

    public ReadEphemeralTxnData(TxnId txnId, Participants<?> readScope, long waitForEpoch, long executeAtEpoch, @Nonnull PartialTxn partialTxn, @Nonnull PartialDeps partialDeps)
    {
        super(txnId, readScope, waitForEpoch, executeAtEpoch);
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
    }

    @Override
    protected synchronized CommitOrReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        return super.apply(safeStore, safeCommand);
    }
}
