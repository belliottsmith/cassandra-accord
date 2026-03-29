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

package accord.impl.basic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nonnull;

import accord.api.AsyncExecutor;
import accord.api.MessageSink.ReplySink;
import accord.impl.basic.Cluster.Link;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Message;
import accord.messages.Reply;
import accord.messages.Reply.FailureReply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class NodeSink implements ReplySink
{
    public enum Action { DELIVER, DROP, DELIVER_WITH_FAILURE, FAILURE }

    public interface TimeoutSupplier
    {
        long localDelay();
        long slowDelay();
        long expiresDelay();
        long slowAt();
        long expiresAt();
        long failsAt();
        long now();
        TimeUnit units();
    }

    final Id self;
    final Function<Id, Node> lookup;
    final Cluster parent;
    final TimeoutSupplier timeouts;

    int nextMessageId = 0;
    Map<Long, Callback> callbacks = new LinkedHashMap<>();

    public NodeSink(Id self, Function<Id, Node> lookup, Cluster parent, TimeoutSupplier timeouts)
    {
        this.self = self;
        this.lookup = lookup;
        this.parent = parent;
        this.timeouts = timeouts;
    }

    @Override
    public void send(Id to, Request send)
    {
        Invariants.require(!to.equals(self));
        maybeEnqueue(to, nextMessageId++, timeouts.expiresAt(), send, null);
    }

    @Override
    public Cancellable send(Id to, Request send, int attempt, @Nonnull AsyncExecutor executor, Callback callback)
    {
        TimeUnit units = timeouts.units();
        long now = timeouts.now();
        long expiresAt = timeouts.expiresAt();
        long slowAt = timeouts.slowAt();
        long messageId = nextMessageId++;
        callbacks.put(messageId, callback);
        PendingRunnable slow = PendingRunnable.create(() -> {
            if (callback == callbacks.get(messageId))
                callback.onSlow(to);
        });
        PendingRunnable timeout = PendingRunnable.create(() -> {
            if (callback == callbacks.remove(messageId))
                callback.onFailure(to, null);
        });
        if (maybeEnqueue(to, messageId, expiresAt, send, callback) || parent.random.decide(0.05f))
        {
            parent.pending.add(slow, slowAt - now, units);
            parent.pending.add(timeout, expiresAt - now, units);
        }
        return () -> callbacks.remove(messageId);
    }

    public void reply(Id replyToNode, ReplyContext replyContext, Reply reply, Throwable failure)
    {
        if (reply == null)
        {
            if (failure == null)
                throw new IllegalArgumentException("Both reply and failure are null");
            reply = new FailureReply(failure);
        }

        long expiresAt = replyContext.expiresAt(MILLISECONDS);
        if (expiresAt > 0) expiresAt = translateLocalToQueueMillis(lookup.apply(self), expiresAt);
        else expiresAt = timeouts.expiresAt();
        maybeEnqueue(replyToNode, Packet.getMessageId(replyContext), expiresAt, reply, null);
    }

    private boolean maybeEnqueue(Node.Id to, long id, long expiresAtQueueMillis, Message message, Callback callback)
    {
        Link link = parent.links.apply(self, to);
        Invariants.require(!to.equals(self));
        if (lookup.apply(to) == null /* client */)
        {
            parent.messageListener.onMessage(Action.DELIVER, self, to, id, message);
            deliver(to, id, expiresAtQueueMillis, message, link);
            return true;
        }

        Action action = link.action.get();
        parent.messageListener.onMessage(action, self, to, id, message);
        switch (action)
        {
            case DELIVER:
                deliver(to, id, expiresAtQueueMillis, message, link);
                return true;
            case DELIVER_WITH_FAILURE:
                deliver(to, id, expiresAtQueueMillis, message, link);
            case FAILURE:
                if (callback != null)
                {
                    long failsAt = timeouts.failsAt();
                    parent.pending.add(PendingRunnable.create(() -> {
                        if (callback == callbacks.remove(id))
                        {
                            try
                            {
                                callback.onFailure(to, new SimulatedFault("Simulation Failure; src=" + self + ", to=" + to + ", id=" + id + ", message=" + message));
                            }
                            catch (Throwable t)
                            {
                                callback.onCallbackFailure(to, t);
                                lookup.apply(self).agent().onException(t);
                            }
                        }
                    }), failsAt - timeouts.now(), timeouts.units());
                }
                return false;
            case DROP:
                // TODO (desired): parent.notifyDropped is a trace logger that is very similar in spirit to MessageListener; can we unify?
                parent.notifyDropped(self, to, id, message);
                return true;
            default:
                throw new AssertionError("Unexpected action: " + action);
        }
    }

    private void deliver(Node.Id to, long id, long expiresAtQueueMillis, Message message, Link link)
    {
        long expiresAtLocalMillis = translateQueueToLocalMillisWithJitter(lookup.apply(to), expiresAtQueueMillis);
        Packet packet;
        if (message instanceof Reply) packet = new Packet(self, to, expiresAtLocalMillis, id, (Reply) message);
        else packet = new Packet(self, to, expiresAtLocalMillis, id, (Request) message);
        parent.add(packet, link.latencyMicros.getAsLong(), MICROSECONDS);
    }

    private long translateQueueToLocalMillisWithJitter(Node node, long expiresAtQueueMillis)
    {
        if (node == null)
            return 0;
        long expiresAt = expiresAtQueueMillis - parent.pending.nowInMillis();
        expiresAt *= 1.8f - parent.random.nextFloat();
        expiresAt += node.time().elapsed(MILLISECONDS);
        return expiresAt;
    }

    private long translateLocalToQueueMillis(Node node, long expiresAtLocalMillis)
    {
        long expiresAt = expiresAtLocalMillis - node.time().elapsed(MILLISECONDS);
        expiresAt += parent.pending.nowInMillis();
        return expiresAt;
    }
}
