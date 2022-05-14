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

package accord.impl;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.*;
import accord.primitives.*;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static accord.local.Status.NotWitnessed;

public class InMemoryCommand extends Command
{
    public final CommandStore commandStore;
    private final TxnId txnId;

    private AbstractRoute route;
    private RoutingKey homeKey, progressKey;
    private PartialTxn partialTxn;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    private PartialDeps partialDeps = PartialDeps.NONE;
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;

    private boolean isGloballyPersistent; // only set on home shard

    private NavigableMap<TxnId, Command> waitingOnCommit;
    private NavigableMap<TxnId, Command> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public InMemoryCommand(CommandStore commandStore, TxnId txnId)
    {
        this.commandStore = commandStore;
        this.txnId = txnId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InMemoryCommand command = (InMemoryCommand) o;
        return commandStore == command.commandStore
                && txnId.equals(command.txnId)
                && Objects.equals(homeKey, command.homeKey)
                && Objects.equals(progressKey, command.progressKey)
                && Objects.equals(partialTxn, command.partialTxn)
                && promised.equals(command.promised)
                && accepted.equals(command.accepted)
                && Objects.equals(executeAt, command.executeAt)
                && partialDeps.equals(command.partialDeps)
                && Objects.equals(writes, command.writes)
                && Objects.equals(result, command.result)
                && status == command.status
                && isGloballyPersistent == command.isGloballyPersistent
                && Objects.equals(waitingOnCommit, command.waitingOnCommit)
                && Objects.equals(waitingOnApply, command.waitingOnApply)
                && Objects.equals(listeners, command.listeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, txnId, partialTxn, promised, accepted, executeAt, partialDeps, writes, result, status, waitingOnCommit, waitingOnApply, listeners);
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public CommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    protected void setHomeKey(RoutingKey key)
    {
        this.homeKey = key;
    }

    @Override
    public RoutingKey progressKey()
    {
        return progressKey;
    }

    @Override
    protected void setProgressKey(RoutingKey key)
    {
        this.progressKey = key;
    }

    @Override
    public AbstractRoute route()
    {
        return route;
    }

    @Override
    protected void setRoute(AbstractRoute route)
    {
        this.route = route;
    }

    @Override
    public PartialTxn partialTxn()
    {
        return partialTxn;
    }

    @Override
    protected void setPartialTxn(PartialTxn txn)
    {
        this.partialTxn = txn;
    }

    @Override
    public Ballot promised()
    {
        return promised;
    }

    @Override
    public void setPromised(Ballot ballot)
    {
        this.promised = ballot;
    }

    @Override
    public Ballot accepted()
    {
        return accepted;
    }

    @Override
    public void setAccepted(Ballot ballot)
    {
        this.accepted = ballot;
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    @Override
    public void setExecuteAt(Timestamp timestamp)
    {
        this.executeAt = timestamp;
    }

    @Override
    public PartialDeps partialDeps()
    {
        return partialDeps;
    }

    @Override
    public void setPartialDeps(PartialDeps deps)
    {
        this.partialDeps = deps;
    }

    @Override
    public Writes writes()
    {
        return writes;
    }

    @Override
    public void setWrites(Writes writes)
    {
        this.writes = writes;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public void setResult(Result result)
    {
        this.result = result;
    }

    @Override
    public Status status()
    {
        return status;
    }

    @Override
    public void setStatus(Status status)
    {
        this.status = status;
    }

    @Override
    public boolean isGloballyPersistent()
    {
        return isGloballyPersistent;
    }

    @Override
    public void setGloballyPersistent(boolean v)
    {
        isGloballyPersistent = v;
    }

    @Override
    public Command addListener(Listener listener)
    {
        listeners.add(listener);
        return this;
    }

    @Override
    public void removeListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void notifyListeners()
    {
        listeners.forEach(this);
    }

    @Override
    public void addWaitingOnCommit(Command command)
    {
        if (waitingOnCommit == null)
            waitingOnCommit = new TreeMap<>();

        waitingOnCommit.put(command.txnId(), command);
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return waitingOnCommit != null && !waitingOnCommit.isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(Command command)
    {
        if (waitingOnCommit == null)
            return;
        waitingOnCommit.remove(command.txnId());
    }

    @Override
    public Command firstWaitingOnCommit()
    {
        return isWaitingOnCommit() ? waitingOnCommit.firstEntry().getValue() : null;
    }

    @Override
    public void addWaitingOnApplyIfAbsent(Command command)
    {
        if (waitingOnApply == null)
            waitingOnApply = new TreeMap<>();

        waitingOnApply.putIfAbsent(command.txnId(), command);
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return waitingOnApply != null && !waitingOnApply.isEmpty();
    }

    @Override
    public void removeWaitingOn(Command command)
    {
        if (waitingOnCommit != null)
            waitingOnCommit.remove(command.txnId());

        if (waitingOnApply != null)
            waitingOnApply.remove(command.txnId());
    }

    @Override
    public Command firstWaitingOnApply()
    {
        return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
    }
}
