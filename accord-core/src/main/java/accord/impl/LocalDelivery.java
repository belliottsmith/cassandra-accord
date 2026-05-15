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

import accord.api.MessageSink;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class LocalDelivery<R extends Reply> implements ReplyContext
{
    // TODO (desired): refactor expiration passing, so we don't need to stash here
    final Node node;
    final Callback<R> callback;
    private long expiresAtMicros;
    private volatile RegisteredTimeout timeout;

    private static final RegisteredTimeout NO_SLOW_TIMEOUT = () -> {};
    private static final AtomicReferenceFieldUpdater<LocalDelivery, RegisteredTimeout> timeoutUpdater = AtomicReferenceFieldUpdater.newUpdater(LocalDelivery.class, RegisteredTimeout.class, "timeout");

    public LocalDelivery(Node node, Callback<R> callback)
    {
        this.node = node;
        this.callback = callback;
    }

    public Cancellable deliver(Request request)
    {
        TxnId txnId = request.primaryTxnId();
        MessageType messageType = request.type();
        this.expiresAtMicros = node.agent().selfExpiresAt(txnId, messageType, MICROSECONDS);
        long slowAtMicros = node.agent().selfSlowAt(txnId, messageType, MICROSECONDS);
        timeout = slowTimeout(slowAtMicros);
        return request.process(node, node.id(), this);
    }

    private RegisteredTimeout slowTimeout(long slowAtMicros)
    {
        if (slowAtMicros <= 0 || slowAtMicros >= expiresAtMicros)
            return NO_SLOW_TIMEOUT;

        return node.timeouts().registerAt(new Timeouts.Timeout()
        {
            @Override public void timeout() { onSlow(); }
            @Override public int stripe() { return (int)expiresAtMicros; }
        }, slowAtMicros, MICROSECONDS);
    }

    private RegisteredTimeout expiresTimeout()
    {
        return node.timeouts().registerAt(new Timeouts.Timeout()
        {
            @Override public void timeout()
            {
                if (timeoutUpdater.getAndSet(LocalDelivery.this, null) != null)
                    callback.onFailure(node.id(), null);
            }
            @Override public int stripe() { return (int)expiresAtMicros; }
        }, expiresAtMicros, MICROSECONDS);
    }

    private void onSlow()
    {
        RegisteredTimeout slowTimeout = this.timeout;
        if (slowTimeout == null)
            return;

        RegisteredTimeout expiresTimeout = expiresTimeout();
        if (timeoutUpdater.compareAndSet(this, slowTimeout, expiresTimeout)) callback.onSlow(node.id());
        else expiresTimeout.cancel();
    }

    private void onSuccess(R reply)
    {
        // if the reply is non-final the execution timeout created by request.process() will have been cancelled.
        // if we have no slow timeout, we're now not protected against no reply arriving, so we set our own expiry timeout
        if (reply.isFinal() ? cancelTimeout() : ensureTimeout())
            callback.onSuccess(node.id(), reply);
    }

    private void onFailure(Throwable t)
    {
        if (cancelTimeout())
            callback.onFailure(node.id(), t);
    }

    private boolean cancelTimeout()
    {
        RegisteredTimeout timeout = clearTimeout();
        if (timeout == null)
            return false;
        timeout.cancel();
        return true;
    }

    private boolean ensureTimeout()
    {
        RegisteredTimeout cur = timeout;
        if (cur == NO_SLOW_TIMEOUT)
        {
            RegisteredTimeout upd = expiresTimeout();
            if (timeoutUpdater.compareAndSet(this, cur, upd))
                return true;

            upd.cancel();
            return false;
        }
        return cur != null;
    }

    private RegisteredTimeout clearTimeout()
    {
        return timeoutUpdater.getAndSet(this, null);
    }

    @Override
    public long expiresAt(TimeUnit units)
    {
        return units.convert(expiresAtMicros, MICROSECONDS);
    }

    @Override
    public void reply(Node.Id to, MessageSink sink, Reply success, Throwable failure)
    {
        if (success != null) onSuccess((R)success);
        else if (failure != null) onFailure(failure);
        else Invariants.expect(false, "Both success and failure are null");
    }
}
