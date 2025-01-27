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

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.api.LocalListeners;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.RemoteListeners;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.messages.TxnRequest.computeScope;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Contact a replica to perform a synchronous or asynchronous wait on some condition for a transaction + some keys.
 * If the callbackId is less than zero, the wait condition is met synchronously - that is, a reply is not forthcoming
 * until the wait condition is met. A non-negative callbackId will register this callbackId if the wait condition is not
 * met, and return the set of keys where the wait condition *was* met.
 *
 * TODO (desired): return an OK message indicating we're waiting synchronously
 */
public class Await implements Request, MapReduceConsume<SafeCommandStore, Void>, PreLoadContext, LocalListeners.ComplexListener, Timeouts.Timeout
{
    public static class SerializerSupport
    {
        public static Await create(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, boolean notifyProgressLog, long minAwaitEpoch, long maxAwaitEpoch, int callbackId)
        {
            return new Await(txnId, scope, blockedUntil, minAwaitEpoch, maxAwaitEpoch, callbackId, notifyProgressLog);
        }
    }

    public final TxnId txnId;
    public final Participants<?> scope;
    public final BlockedUntil blockedUntil;
    public final long minAwaitEpoch, maxAwaitEpoch;
    public final int callbackId; // < 0 means synchronous await
    public final boolean notifyProgressLog;

    transient Node node;
    transient Id replyTo;
    transient ReplyContext replyContext;

    private transient volatile RemoteListeners.Registration asyncRegistration;
    private static final AtomicReferenceFieldUpdater<Await, RemoteListeners.Registration> registrationUpdater = AtomicReferenceFieldUpdater.newUpdater(Await.class, RemoteListeners.Registration.class, "asyncRegistration");

    private transient volatile int synchronouslyWaitingOn;
    private static final AtomicIntegerFieldUpdater<Await> synchronouslyWaitingOnUpdater = AtomicIntegerFieldUpdater.newUpdater(Await.class, "synchronouslyWaitingOn");

    private transient volatile Collection<LocalListeners.Registered> syncRegistrations;
    private static final AtomicReferenceFieldUpdater<Await, Collection<LocalListeners.Registered>> syncRegistrationsUpdater = AtomicReferenceFieldUpdater.newUpdater(Await.class, (Class)Collection.class, "syncRegistrations");
    private transient RegisteredTimeout timeout;

    // we use exactly -1 to make serialization easy (can increment by 1 and store a non-negative integer)
    private static final int SYNCHRONOUS_CALLBACKID = -1;

    public Await(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil, int callbackId, boolean notifyProgressLog)
    {
        this.txnId = txnId;
        this.callbackId = callbackId;
        this.notifyProgressLog = notifyProgressLog;
        this.scope = computeScope(to, topologies, participants);
        this.blockedUntil = blockedUntil;
        this.maxAwaitEpoch = topologies.currentEpoch();
        this.minAwaitEpoch = topologies.oldestEpoch();
        Invariants.require(minAwaitEpoch >= txnId.epoch());
    }

    public Await(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil, boolean notifyProgressLog)
    {
        this(to, topologies, txnId, participants, blockedUntil, SYNCHRONOUS_CALLBACKID, notifyProgressLog);
    }

    private Await(TxnId txnId, Participants<?> scope, BlockedUntil blockedUntil, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, boolean notifyProgressLog)
    {
        this.txnId = txnId;
        this.scope = scope;
        this.blockedUntil = blockedUntil;
        this.minAwaitEpoch = minAwaitEpoch;
        this.maxAwaitEpoch = maxAwaitEpoch;
        this.callbackId = callbackId;
        this.notifyProgressLog = notifyProgressLog;
        Invariants.require(minAwaitEpoch >= txnId.epoch());
    }

    @Override
    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        this.node = node;
        this.replyTo = replyToNode;
        this.replyContext = replyContext;
        node.mapReduceConsumeLocal(this, scope, minAwaitEpoch, maxAwaitEpoch, this);
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minAwaitEpoch, txnId, maxAwaitEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        Invariants.require(minAwaitEpoch >= txnId.epoch());
        if (command.saveStatus().compareTo(blockedUntil.unblockedFrom) >= 0)
        {
            onNotWaiting(safeStore, safeCommand);
            return null;
        }

        Commands.supplementParticipants(safeStore, safeCommand, participants);

        if (callbackId >= 0)
        {
            RemoteListeners.Registration registered = asyncRegistration;
            if (registered == null)
            {
                registered = node.remoteListeners().register(txnId, blockedUntil.unblockedFrom, blockedUntil.remoteDurability, replyTo, callbackId);
                if (!registrationUpdater.compareAndSet(this, null, registered))
                    registered = asyncRegistration;
            }
            registered.add(safeStore, safeCommand);
        }
        else
        {
            if (syncRegistrations == null)
                syncRegistrationsUpdater.compareAndSet(this, null, new ConcurrentLinkedQueue<>());
            syncRegistrations.add(safeStore.register(txnId, this));
            synchronouslyWaitingOnUpdater.incrementAndGet(this);
        }

        if (notifyProgressLog)
            safeStore.progressLog().waiting(blockedUntil, safeStore, safeCommand, null, null, participants);
        return null;
    }

    @Override
    public Void reduce(Void o1, Void o2)
    {
        return null;
    }

    @Override
    public void accept(Void result, Throwable failure)
    {
        if (failure != null)
        {
            cancel();
            node.reply(replyTo, replyContext, null, failure);
        }
        else if (callbackId >= 0)
        {
            RemoteListeners.Registration asyncRegistration = this.asyncRegistration;
            AwaitOk reply = asyncRegistration == null || 0 == asyncRegistration.done() ? AwaitOk.Ready : AwaitOk.NotReady;
            node.reply(replyTo, replyContext, reply, null);
        }
        else
        {
            int waitingOn = synchronouslyWaitingOnUpdater.decrementAndGet(this);
            if (waitingOn >= 0)
            {
                long expiresAtMicros = node.agent().expiresAt(replyContext, MICROSECONDS);
                if (expiresAtMicros > 0)
                {
                    timeout = node.timeouts().registerAt(this, expiresAtMicros, MICROSECONDS);
                    if (-1 == synchronouslyWaitingOn)
                        timeout.cancel(); // we could leave a dangling timeout in this rare race condition
                }
            }
            else
            {
                onSynchronousAwaitComplete();
            }
        }
    }

    public void timeout()
    {
        timeout = null;
        cancel();
    }

    public int stripe()
    {
        return txnId.hashCode();
    }

    void cancel()
    {
        RegisteredTimeout cancelTimeout = timeout;
        Collection<LocalListeners.Registered> cancelRegistrations = syncRegistrations;
        if (cancelTimeout != null)
            cancelTimeout.cancel();
        if (cancelRegistrations != null)
            cancelRegistrations.forEach(LocalListeners.Registered::cancel);
        timeout = null;
        syncRegistrations = null;
        asyncRegistration = null;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public MessageType type()
    {
        return MessageType.AWAIT_REQ;
    }

    public enum AwaitOk implements Reply
    {
        NotReady, Ready;

        @Override
        public MessageType type()
        {
            return MessageType.AWAIT_RSP;
        }
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch();
    }

    @Override
    public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (!checkOneSynchronousAwait(safeCommand))
            return true;

        onNotWaiting(safeStore, safeCommand);
        if (-1 == synchronouslyWaitingOnUpdater.decrementAndGet(this))
        {
            onSynchronousAwaitComplete();
            if (timeout != null)
                timeout.cancel();
            syncRegistrations = null;
        }

        return false;
    }

    protected boolean checkOneSynchronousAwait(SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        SaveStatus saveStatus = command.saveStatus();
        return saveStatus.compareTo(blockedUntil.unblockedFrom) >= 0
               || (blockedUntil.additionallyUnblockedBy != null && blockedUntil.additionallyUnblockedBy.test(saveStatus));
    }

    protected void onNotWaiting(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
    }

    protected void onSynchronousAwaitComplete()
    {
        node.reply(replyTo, replyContext, AwaitOk.Ready, null);
    }

    public static class AsyncAwaitComplete implements Request, PreLoadContext, Consumer<SafeCommandStore>
    {
        public final TxnId txnId;
        public final Route<?> route; // at least those Routable we registered with
        public final SaveStatus newStatus;
        public final int callbackId;
        transient Node.Id from;

        public AsyncAwaitComplete(TxnId txnId, Route<?> route, SaveStatus newStatus, int callbackId)
        {
            this.txnId = txnId;
            this.route = Invariants.requireArgument(route, route != null);
            this.newStatus = newStatus;
            this.callbackId = callbackId;
        }

        @Override
        public MessageType type()
        {
            return MessageType.ASYNC_AWAIT_COMPLETE_REQ;
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {
            this.from = from;
            node.forEachLocal(this, route, txnId.epoch(), Long.MAX_VALUE, this);
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return txnId;
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            if (safeCommand == null || safeCommand.current().saveStatus() == SaveStatus.Uninitialised)
                return;

            Commands.updateRoute(safeStore, safeCommand, route);
            safeStore.progressLog().remoteCallback(safeStore, safeCommand, newStatus, callbackId, from);
        }
    }
}
