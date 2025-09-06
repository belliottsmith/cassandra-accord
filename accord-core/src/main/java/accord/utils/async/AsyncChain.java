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

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import accord.api.AsyncExecutor;
import accord.utils.TriFunction;

import static accord.utils.async.AsyncCallbacks.ifSuccess;

public interface AsyncChain<V>
{
    // a marker interface for the start of a chain, that is bubbled up through a chain until the chain is ready and started with begin()
    interface Head<V> extends AsyncChain<V>, BiConsumer<V, Throwable>
    {
        Cancellable begin();
    }

    <OV, OC extends AsyncChain<OV> & BiConsumer<? super V, Throwable>> AsyncChain<OV> then(Function<Head<?>, OC> then);

    default <P, OV, OC extends AsyncChain<OV> & BiConsumer<? super V, Throwable>> AsyncChain<OV> then(BiFunction<Head<?>, P, OC> then, P p)
    {
        return then(head -> then.apply(head, p));
    }

    default <P1, P2, OV, OC extends AsyncChain<OV> & BiConsumer<? super V, Throwable>> AsyncChain<OV> then(TriFunction<Head<?>, P1, P2, OC> then, P1 p1, P2 p2)
    {
        return then(head -> then.apply(head, p1, p2));
    }

    /**
     * When the chain has failed, this allows the chain to attempt to recover if possible.  The provided function may return a {@code null} to represent
     * that recovery was not possible and that the original exception should propgate.
     * <p/>
     * This is similiar to {@link java.util.concurrent.CompletableFuture#exceptionally(Function)} but with async handling; would have the same semantics as the following
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
    default AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
    {
        return then(AsyncChains.MapRecover::new, mapper);
    }

    default <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
    {
        return then(AsyncChains.Map::new, mapper);
    }

    default <T> AsyncChain<T> mapToNull()
    {
        return map(ignore -> null);
    }

    default <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
    {
        return then(AsyncChains.FlatMap::new, mapper);
    }

    default AsyncChain<V> invoke(BiConsumer<? super V, Throwable> callback)
    {
        return then(AsyncChains.Callback::new, callback);
    }

    default <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper, AsyncExecutor executor)
    {
        return then(AsyncChains.AsyncMap::new, mapper, executor);
    }

    default <T> AsyncChain<T> flatMapOverride(Supplier<? extends AsyncChain<T>> override)
    {
        return then(AsyncChains.FlatMapOverride::new, override);
    }

    default <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper, AsyncExecutor executor)
    {
        return then(AsyncChains.AsyncFlatMap::new, mapper, executor);
    }

    default <T> AsyncChain<T> flatMapResult(Function<? super V, ? extends AsyncResult<T>> mapper)
    {
        return then(AsyncChains.FlatMapResult::new, mapper);
    }

    default AsyncChain<V> withExecutor(@Nullable AsyncExecutor executor)
    {
        if (executor == null)
            return this;

        // since a chain runs as a sequence of callbacks, by adding a callback that moves to this executor any new actions
        // will be run on that desired executor.
        return map(a -> a, executor);
    }

    /**
     * Adds a callback that fires on success only
     */
    default AsyncChain<V> invokeIfSuccess(Runnable run)
    {
        return invoke(ifSuccess(run));
    }

    default AsyncChain<V> invokeIfSuccess(Consumer<? super V> consumer)
    {
        return invoke(ifSuccess(consumer));
    }

    default AsyncChain<V> invokeIfSuccess(Runnable run, AsyncExecutor executor)
    {
        return invoke(ifSuccess(run), executor);
    }

    default AsyncChain<V> invokeIfSuccess(Consumer<? super V> consume, AsyncExecutor executor)
    {
        return invoke(ifSuccess(consume), executor);
    }

    /**
     * Adds a callback that fires on either success or failure
     */
    default AsyncChain<V> invoke(Runnable run)
    {
        return invoke(AsyncCallbacks.always(run));
    }

    default AsyncChain<V> invoke(BiConsumer<? super V, Throwable> callback, AsyncExecutor executor)
    {
        return then(AsyncChains.AsyncCallback::new, callback, executor);
    }

    /**
     * Causes the chain to begin, starting all work required.  This method must be called exactly once, not calling will
     * not cause any work to start, and calling multiple times will be rejected.
     */
    @Nullable Cancellable begin(BiConsumer<? super V, Throwable> callback);

    default AsyncResult<V> beginAsResult()
    {
        return AsyncResults.forChain(this);
    }
}