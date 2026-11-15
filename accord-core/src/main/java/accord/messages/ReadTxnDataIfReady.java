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
import accord.local.Command;
import accord.local.Node;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.coordinate.ExecuteFlag.NO_WAIT;

/**
 * An opportunistic read, for use by coordinators that do not necessarily know the full transaction definition
 * (e.g. backlog execution). Each store that is ready to execute begins its read immediately; as soon as we encounter
 * a store that is not ready to execute we abandon the read, replying {@link ReadData.CommitOrReadNack#NotReady}.
 * If every store is ready, then before any data we first reply with a (non-final) {@link ReadData.ReadyWithDefinition}
 * containing the definition and deps of every store we read from, so that the coordinator may reconstitute the
 * transaction (and know every replica it reads from is ready before any data arrives).
 *
 * TODO (desired): retain the reads we have started so that we may cancel them when we abandon
 */
public class ReadTxnDataIfReady extends ReadTxnData
{
    public static class SerializerSupport
    {
        public static ReadTxnDataIfReady create(TxnId txnId, Participants<?> scope, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
        {
            return new ReadTxnDataIfReady(txnId, scope, executeAt, executeAtEpoch, flags);
        }
    }

    private @Nullable PartialTxn collectTxn;
    private @Nullable PartialDeps collectDeps;

    public ReadTxnDataIfReady(Node.Id to, Topologies topologies, TxnId txnId, FullRoute<?> route, Timestamp executeAt)
    {
        super(to, topologies, txnId, route, null, executeAt, executeAt.epoch(), ExecuteFlags.none().with(NO_WAIT));
    }

    protected ReadTxnDataIfReady(TxnId txnId, Participants<?> scope, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
    {
        super(txnId, scope, null, executeAt, executeAtEpoch, flags);
    }

    @Override
    protected boolean abandonIfNotReady()
    {
        return true;
    }

    @Override
    protected void onExecuting(Command command)
    {
        PartialTxn txn = command.partialTxn();
        if (txn != null)
            collectTxn = PartialTxn.merge(collectTxn, txn);
        PartialDeps deps = command.partialDeps();
        if (deps != null)
            collectDeps = collectDeps == null ? deps : collectDeps.with(deps);
    }

    @Override
    protected void onSetupComplete()
    {
        PartialTxn txn;
        PartialDeps deps;
        synchronized (this)
        {
            txn = collectTxn;
            deps = collectDeps;
            collectTxn = null;
            collectDeps = null;
        }
        reply(new ReadyWithDefinition(txn, deps), null);
    }

    @Override
    protected boolean mayFastExecute()
    {
        return false;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readTxnDataIfReady;
    }

    @Override
    public String reason()
    {
        return "Read If Ready";
    }
}
