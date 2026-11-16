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

import org.agrona.collections.IntHashSet;

import accord.coordinate.ExecuteFlag;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.coordinate.ExecuteFlag.NO_WAIT;

/**
 * A read for use by coordinators that do not necessarily know the full transaction definition (e.g. backlog execution).
 * Each store that is ready to execute begins its read immediately. Before any data we first reply with a (non-final)
 * {@link ReadData.ReadyWithDefinition} containing the definition and deps of every store, so that the coordinator may
 * reconstitute the transaction.
 * <p>
 * With {@link ExecuteFlag#NO_WAIT}, the read is opportunistic: as soon as we encounter a store that is not ready to
 * execute we abandon the read, replying {@link ReadData.CommitOrReadNack#NotReady}; so the coordinator knows every
 * replica it reads from is ready before any data arrives. Otherwise, we wait for stores to become ready to execute
 * (as {@link ReadTxnData}). A store that has yet to record the transaction as Stable (typically because its Stable
 * message is still in flight) replies {@link ReadData.CommitOrReadNack#InsufficientAndWaiting}, and once it becomes
 * Stable we send a further {@link ReadData.ReadyWithDefinition} with its definition and deps.
 * <p>
 * So {@link ReadData.CommitOrReadNack#NotReady} only ever means a store was not ready and we were asked not to wait.
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

    /**
     * @param wait if true, wait for stores that are Stable to become ready to execute, rather than abandon
     */
    public ReadTxnDataIfReady(Node.Id to, Topologies topologies, TxnId txnId, FullRoute<?> route, Timestamp executeAt, boolean wait)
    {
        super(to, topologies, txnId, route, null, executeAt, executeAt.epoch(), wait ? ExecuteFlags.none() : ExecuteFlags.none().with(NO_WAIT));
    }

    protected ReadTxnDataIfReady(TxnId txnId, Participants<?> scope, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
    {
        super(txnId, scope, null, executeAt, executeAtEpoch, flags);
    }

    // stores we are waiting on that had not yet recorded the transaction as Stable when we set up, so whose definition
    // and deps we have not yet reported (see onWaitingUpdate); guarded by this object's lock
    private @Nullable IntHashSet awaitingStable;

    @Override
    protected boolean abandonIfNotReady(Command command)
    {
        return flags.contains(NO_WAIT);
    }

    @Override
    protected void onWaitingForStable(int storeId)
    {
        if (awaitingStable == null)
            awaitingStable = new IntHashSet();
        awaitingStable.add(storeId);
    }

    @Override
    protected @Nullable ReadReply onWaitingUpdate(int storeId, Command command)
    {
        if (awaitingStable == null || command.saveStatus().compareTo(SaveStatus.Stable) < 0 || !awaitingStable.remove(storeId))
            return null;

        // this store has now recorded the transaction as Stable (e.g. its Stable message was still in flight when we set
        // up), so report its definition and deps (before any data, which requires it to be ready to execute). Note that
        // the coordinator may nonetheless receive this after our data, if messages are reordered (see ExecuteBacklogTxn)
        return new ReadyWithDefinition(command.partialTxn(), command.partialDeps());
    }

    @Override
    protected void onSetup(Command command)
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
