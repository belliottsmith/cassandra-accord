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

package accord.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.TxnId;
import accord.utils.async.Cancellable;

public interface MessageSink
{
    void send(Id to, Request request);
    default Cancellable send(Id to, Request request, AsyncExecutor executor, Callback<?> callback)
    {
        return send(to, request, 1, executor, callback);
    }
    Cancellable send(Id to, Request request, int attempt, AsyncExecutor executor, Callback<?> callback);

    /**
     * Invoked when a request delivered locally (i.e. bypassing this sink; see {@link accord.impl.LocalDelivery})
     * replies with a final reply, so that implementations may measure local delivery as they would a remote round-trip.
     *
     * @param elapsed the time between delivery and reply, as measured by {@link Node#elapsed}
     */
    default void onLocalReply(MessageType requestType, @Nullable TxnId txnId, long elapsed, TimeUnit units) {}

    interface ReplySink extends MessageSink
    {
        void reply(Node.Id to, ReplyContext context, Reply success, Throwable failure);
    }

    class NoOpSink implements ReplySink
    {
        @Override public void send(Id to, Request request) {}
        @Override public Cancellable send(Id to, Request request, int attempt, AsyncExecutor executor, Callback<?> callback) { return null; }
        @Override public void reply(Id to, ReplyContext context, Reply success, Throwable failure) {}
    }
}
