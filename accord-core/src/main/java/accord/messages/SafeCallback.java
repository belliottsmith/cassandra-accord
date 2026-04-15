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

import java.util.Objects;

import accord.api.AsyncExecutor;
import accord.local.Node;

public class SafeCallback<T extends Reply>
{
    private final AsyncExecutor executor;
    private final Callback<T> callback;

    public SafeCallback(AsyncExecutor executor, Callback<T> callback)
    {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.callback = Objects.requireNonNull(callback, "callback");
    }

    public void success(Node.Id from, T reply)
    {
        safeCall(from, reply, Callback::onSuccess);
    }

    public void slowResponse(Node.Id from)
    {
        safeCall(from, null, (callback, id, ignore) -> callback.onSlowResponse(id));
    }

    public void failure(Node.Id from, Throwable t)
    {
        safeCall(from, t, Callback::onFailure);
    }

    public void timeout(Node.Id from)
    {
        failure(from, null);
    }

    public void onCallbackFailure(Node.Id from, Throwable t)
    {
        safeCall(from, t, Callback::onCallbackFailure);
    }

    protected interface SafeCall<T, P>
    {
        void accept(Callback<T> callback, Node.Id id, P param);
    }

    protected final <P> void safeCall(Node.Id from, P param, SafeCall<T, P> call)
    {
        // TODO (low priority, correctness): if the executor is shutdown this propgates the exception to the network stack
        executor.executeMaybeImmediately(() -> {
            try
            {
                call.accept(callback, from, param);
            }
            catch (Throwable t)
            {
                try
                {
                    if (callback.onCallbackFailure(from, t))
                        return;
                }
                catch (Throwable t2)
                {
                    t.addSuppressed(t2);
                }
                throw t;
            }
        });
    }

    @Override
    public String toString()
    {
        return callback.toString();
    }
}
