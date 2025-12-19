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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.annotation.Nullable;

import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Node;
import accord.local.MapReduceConsumeCommandStores;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public abstract class NoWaitRequest<P extends Participants<?>, R extends Reply> extends MapReduceConsumeCommandStores<P, R> implements Request, Timeouts.Timeout
{
    private static class Cancellation implements Cancellable
    {
        final RegisteredTimeout timeout;
        final Cancellable cancel;

        Cancellation(RegisteredTimeout timeout, Cancellable cancel)
        {
            this.timeout = timeout;
            this.cancel = cancel;
        }

        @Override
        public void cancel()
        {
            if (timeout != null) timeout.cancel();
            if (cancel != null) cancel.cancel();
        }
    }

    private static final class Done extends Cancellation { Done() { super(null, null); } }

    private static final Done CANCEL = new Done();
    private static final Done DONE = new Done();
    private static final Cancellation EMPTY = new Cancellation(null, null);

    public final TxnId txnId;
    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;
    private boolean hasSentFinalReply;

    private transient volatile Cancellation cancellation;
    private static final AtomicReferenceFieldUpdater<NoWaitRequest, Cancellation> cancellationUpdater = AtomicReferenceFieldUpdater.newUpdater(NoWaitRequest.class, Cancellation.class, "cancellation");

    protected NoWaitRequest(TxnId txnId, P scope)
    {
        super(scope);
        this.txnId = txnId;
    }

    @Override
    public final void process(Node on, Node.Id replyTo, ReplyContext replyContext)
    {
        this.node = on;
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        Cancellable cancel = submit();
        if (cancel != null)
        {
            long expiresAt = node.agent().expiresAt(replyContext, MICROSECONDS);
            if (expiresAt > 0)
            {
                RegisteredTimeout timeout = node.timeouts().registerAt(this, expiresAt, MICROSECONDS);
                Cancellation cancellation = new Cancellation(timeout, cancel);
                if (!cancellationUpdater.compareAndSet(this, null, cancellation))
                    (this.cancellation == CANCEL ? cancellation : cancellation.timeout).cancel();
            }
        }
    }

    protected boolean isDone()
    {
        return cancellation instanceof Done;
    }

    protected boolean isCancelled()
    {
        return cancellation == CANCEL;
    }

    protected boolean ifDoneExpectCancelled()
    {
        if (!isDone())
            return false;
        Invariants.require(cancellation == CANCEL);
        return true;
    }

    protected abstract Cancellable submit();

    @Override
    public final void accept(R reply, Throwable failure)
    {
        cleanup(failure == null ? clearInternal() : cancelInternal());
        acceptInternal(reply, failure);
    }

    protected void acceptInternal(R reply, Throwable failure)
    {
        Invariants.require(reply != null || failure != null, "No reply produced for %s", this);
        if (failure != null || reply.isFinal())
        {
            Invariants.require(!hasSentFinalReply);
            hasSentFinalReply = true;
        }
        if (failure != null) cancel();
        node.reply(replyTo, replyContext, reply, failure);
    }

    @Override
    public final void timeout()
    {
        cleanup(timeoutInternal());
    }

    protected @Nullable Cancellable timeoutInternal()
    {
        return cancelInternal().cancel;
    }

    protected boolean cancel()
    {
        Cancellation clear = cancelInternal();
        if (clear.getClass() == Done.class)
            return false;

        cleanup(clear);
        return true;
    }

    /**
     * invoked on any termination, to ensure state is cleared
     * @return
     */
    protected void clear()
    {
        cleanup(clearInternal());
    }

    protected final Cancellation clearInternal()
    {
        return clearInternal(DONE);
    }

    protected final Cancellation cancelInternal()
    {
        return clearInternal(CANCEL);
    }

    /**
     * invoked on any termination, to ensure state is cleared
     * @return
     */
    protected final Cancellation clearInternal(Done done)
    {
        while (true)
        {
            // can loop at most once
            Cancellation cur = cancellation;
            if (cur == DONE || cur == CANCEL || cancellationUpdater.compareAndSet(this, cur, done))
                return cur != null ? cur : EMPTY;
        }
    }

    public ReplyContext replyContext()
    {
        return replyContext;
    }

    @Override
    public R reduce(R o1, R o2)
    {
        throw new IllegalStateException();
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public int stripe()
    {
        return txnId.hashCode();
    }

    private static void cleanup(@Nullable Cancellable cancel)
    {
        if (cancel != null)
            cancel.cancel();
    }

    @Override
    public String reason()
    {
        return getClass().getSimpleName();
    }
}
