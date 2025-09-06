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

package accord.utils.async;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.api.AsyncExecutor;

import static accord.utils.Invariants.illegalState;
import static accord.utils.async.AsyncCallbacks.ifSuccess;

/**
 * Handle for async computations that supports multiple listeners and registering
 * listeners after the computation has started
 *
 * TODO (expected): by default AsyncResult methods should be started immediately; should introduce newChain() for building a chain.
 */
public interface AsyncResult<V>
{
    boolean isDone();
    boolean isSuccess();
    AsyncResult<V> invoke(BiConsumer<? super V, Throwable> callback);

    // TODO (expected): see how many calls to this method we can avoid
    default AsyncChain<V> chain()
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                AsyncResult.this.invoke(callback);
                return null;
            }
        };
    }

    // runs immediately if already done, otherwise submits to the provided executor
    default AsyncChain<V> chainImmediatelyElse(@Nullable AsyncExecutor executor)
    {
        AsyncChain<V> result = chain();
        if (!isDone())
            result = result.withExecutor(executor);
        return result;
    }

    default <T> AsyncResult<T> map(Function<? super V, ? extends T> mapper)
    {
        return chain().<T>map(mapper).beginAsResult();
    }

    default <T> AsyncResult<T> map(Function<? super V, ? extends T> mapper, AsyncExecutor executor)
    {
        return chain().<T>map(mapper, executor).beginAsResult();
    }

    default <T> AsyncResult<T> flatMap(Function<? super V, ? extends AsyncResult<T>> mapper)
    {
        return chain().flatMap(mapper.andThen(AsyncResult::chain)).beginAsResult();
    }

    default <T> AsyncResult<T> flatMap(Function<? super V, ? extends AsyncResult<T>> mapper, AsyncExecutor executor)
    {
        return chain().flatMap(mapper.andThen(AsyncResult::chain), executor).beginAsResult();
    }

    /**
     * When the chain has failed, this allows the chain to attempt to recover if possible.  The provided function may return a {@code null} to represent
     * that recovery was not possible and that the original exception should propagate.
     * <p/>
     * This is similar to {@link java.util.concurrent.CompletableFuture#exceptionally(Function)} but with async handling; would have the same semantics as the following
     * <p/>
     * {@code
     * CompletableFuture<V> failedFuture ...
     * failedFuture.exceptionally(cause -> {
     *     if (canHandle(cause)
     *       return handle(cause); // returns CompletableFuture<V>
     *     return CompletableFuture.completeExceptionally(cause) // return original exception
     * }).flatMap(f -> f); // "flatten" from CompletableFuture<CompletableFuture<V>> to CompletableFuture<V>
     * }
     */
    default AsyncResult<V> recover(Function<? super Throwable, ? extends AsyncResult<V>> mapper)
    {
        return chain().recover(mapper.andThen(AsyncResult::chain)).beginAsResult();
    }

    /**
     * Adds a callback that fires on success only
     */
    default AsyncResult<V> invokeIfSuccess(Runnable run)
    {
        return invoke(ifSuccess(run));
    }

    default AsyncResult<V> invokeIfSuccess(Consumer<? super V> consumer)
    {
        return invoke(ifSuccess(consumer));
    }

    default AsyncResult<V> invokeIfSuccess(Runnable run, Executor executor)
    {
        return invoke(ifSuccess(run), executor);
    }

    default AsyncResult<V> invokeIfSuccess(Consumer<? super V> consumer, Executor executor)
    {
        return invoke(ifSuccess(consumer), executor);
    }

    /**
     * Adds a callback that fires on either success or failure
     */
    default AsyncResult<V> invoke(Runnable run)
    {
        return invoke(AsyncCallbacks.always(run));
    }

    default AsyncResult<V> invoke(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        return invoke(AsyncCallbacks.inExecutor(callback, executor));
    }

    interface Settable<V> extends AsyncResult<V>
    {
        boolean trySuccess(V value);

        default void setSuccess(V value)
        {
            if (!trySuccess(value))
                throw illegalState("Result has already been set on " + this);
        }

        boolean tryFailure(Throwable throwable);

        default void setFailure(Throwable throwable)
        {
            if (!tryFailure(throwable))
            {
                IllegalStateException e = illegalState("Result has already been set on " + this);
                e.addSuppressed(throwable);
                throw e;
            }
        }

        default BiConsumer<V, Throwable> settingCallback()
        {
            return (success, fail) -> {

                if (fail == null)
                    trySuccess(success);
                else
                    tryFailure(fail);
            };
        }
    }
}
