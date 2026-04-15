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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import accord.api.AsyncExecutor;
import accord.api.MessageSink;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class LocalDelivery<R extends Reply> extends SafeCallback<R> implements ReplyContext
{
    // TODO (desired): refactor expiration passing, so we don't need to stash here
    private long expiresAtMicros;
    private volatile RegisteredTimeout slowTimeout;
    private static final AtomicReferenceFieldUpdater<LocalDelivery, RegisteredTimeout> slowTimeoutUpdater = AtomicReferenceFieldUpdater.newUpdater(LocalDelivery.class, RegisteredTimeout.class, "slowTimeout");

    public LocalDelivery(AsyncExecutor executor, Callback<R> callback)
    {
        super(executor, callback);
    }

    public Cancellable deliver(Node node, Request request)
    {
        Node.Id self = node.id();
        TxnId txnId = request.primaryTxnId();
        MessageType messageType = request.type();
        this.expiresAtMicros = node.agent().selfExpiresAt(txnId, messageType, MICROSECONDS);
        long slowAtMicros = node.agent().selfSlowAt(txnId, messageType, MICROSECONDS);
        if (slowAtMicros > 0 && slowAtMicros < expiresAtMicros)
        {
            slowTimeout = node.timeouts().registerAt(new Timeouts.Timeout()
            {
                @Override public void timeout() { safeCall(self, LocalDelivery.this, LocalDelivery::invokeSlow); }
                @Override public int stripe() { return (int)expiresAtMicros; }
            }, slowAtMicros, MICROSECONDS);
        }
        return request.process(node, self, this);
    }

    private static void invokeSlow(Callback<?> callback, Node.Id from, LocalDelivery<?> delivery)
    {
        if (delivery.clearSlow() != null)
            callback.onSlowResponse(from);
    }

    @Override
    public void success(Node.Id from, R reply)
    {
        if (reply.isFinal())
            cancelSlow();
        super.success(from, reply);
    }

    @Override
    public void failure(Node.Id from, Throwable t)
    {
        cancelSlow();
        super.failure(from, t);
    }

    private void cancelSlow()
    {
        RegisteredTimeout timeout = clearSlow();
        if (timeout != null)
            timeout.cancel();
    }

    private RegisteredTimeout clearSlow()
    {
        return slowTimeoutUpdater.getAndSet(this, null);
    }

    @Override
    public long expiresAt(TimeUnit units)
    {
        return units.convert(expiresAtMicros, MICROSECONDS);
    }

    @Override
    public void reply(Node.Id to, MessageSink sink, Reply success, Throwable failure)
    {
        if (success != null) success(to, (R)success);
        else if (failure != null) failure(to, failure);
        else Invariants.expect(false, "Both success and failure are null");
    }
}
