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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import accord.utils.async.AsyncCallbacks;
import accord.utils.async.AsyncCallbacks.RunOrFail;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

public interface AsyncExecutor extends Executor
{
    // unlike execute, throws no exceptions, nor will not wrap the runnable
    default Cancellable execute(RunOrFail run)
    {
        return execute(this, run);
    }

    default boolean tryExecuteImmediately(Runnable run) { return false; }

    default AsyncChain<Void> chain(Runnable run)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super Void, Throwable> callback)
            {
                return execute(new AsyncCallbacks.RunAndCallback(run, callback));
            }
        };
    }

    default <V> AsyncChain<V> chain(Callable<V> call)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return execute(new AsyncCallbacks.CallAndCallback<>(call, callback));
            }
        };
    }

    default <V> AsyncChain<V> flatChain(Callable<? extends AsyncChain<V>> call)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return execute(new AsyncCallbacks.FlatCallAndCallback<>(call, callback));
            }
        };
    }

    // Depending on this implementation this method may queue-jump, i.e. task submission order is not guaranteed.
    // Make sure this is semantically safe at all call-sites.
    default void executeMaybeImmediately(Runnable run)
    {
        if (!tryExecuteImmediately(run))
            execute(run);
    }

    static <V> AsyncChain<V> chain(Executor executor, Callable<V> call)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return execute(executor, new AsyncCallbacks.CallAndCallback<>(call, callback));
            }
        };
    }

    static Cancellable execute(Executor executor, RunOrFail runOrFail)
    {
        try
        {
            if (executor instanceof ExecutorService)
            {
                Future<?> future = ((ExecutorService) executor).submit(runOrFail);
                return () -> future.cancel(false);
            }
            else
            {
                executor.execute(runOrFail);
            }
        }
        catch (Throwable t)
        {
            runOrFail.fail(t);
        }
        return null;
    }
}
