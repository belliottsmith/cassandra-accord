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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static accord.utils.Invariants.illegalState;

// TODO (expected): these methods are test-only; in Cassandra we should not be using these as not simulator safe
public class AsyncChainUtils
{
    public static <V> V getBlocking(AsyncChain<V> chain) throws InterruptedException, ExecutionException
    {
        try
        {
            return getBlocking(chain, 0, TimeUnit.DAYS);
        }
        catch (TimeoutException e)
        {
            throw illegalState("Should not throw timeout exception e");
        }
    }

    public static <V> V getBlockingAndRethrow(AsyncChain<V> chain)
    {
        class Result
        {
            final V result;
            final Throwable failure;

            public Result(V result, Throwable failure)
            {
                this.result = result;
                this.failure = failure;
            }
        }

        AtomicReference<Result> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        chain.begin((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        Result result = callbackResult.get();
        if (result.failure == null) return result.result;
        else throw new RuntimeException(result.failure);
    }

    public static <V> V getBlocking(AsyncChain<V> chain, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException
    {
        class Result
        {
            final V result;
            final Throwable failure;

            public Result(V result, Throwable failure)
            {
                this.result = result;
                this.failure = failure;
            }
        }

        AtomicReference<Result> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        chain.begin((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        if (timeout > 0)
        {
            if (!latch.await(timeout, unit))
                throw new TimeoutException();
        }
        else
            latch.await();

        Result result = callbackResult.get();
        if (result.failure == null) return result.result;
        else throw new ExecutionException(result.failure);
    }

    public static <V> V getUninterruptibly(AsyncResult<V> result) throws ExecutionException
    {
        return getUninterruptibly(result.chain());
    }

    public static <V> V getUninterruptibly(AsyncChain<V> chain) throws ExecutionException
    {
        try
        {
            return getUninterruptibly(chain, 1, TimeUnit.DAYS);
        }
        catch (TimeoutException e)
        {
            throw illegalState("Should not throw timeout exception e");
        }
    }

    public static <V> V getUninterruptibly(AsyncResult<V> result, long time, TimeUnit unit) throws ExecutionException, TimeoutException
    {
        return getUninterruptibly(result.chain(), time, unit);
    }

    public static <V> V getUninterruptibly(AsyncChain<V> chain, long time, TimeUnit unit) throws ExecutionException, TimeoutException
    {
        boolean interrupted = false;
        try
        {
            while (true)
            {
                try
                {
                    return getBlocking(chain, time, unit);
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
        }
        finally
        {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    public static <V> V getUnchecked(AsyncResult<V> result)
    {
        return getUnchecked(result.chain());
    }

    public static <V> V getUnchecked(AsyncChain<V> chain)
    {
        try
        {
            return getUninterruptibly(chain);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void awaitUninterruptibly(AsyncResult<?> result)
    {
        awaitUninterruptibly(result.chain());
    }

    public static void awaitUninterruptibly(AsyncChain<?> chain)
    {
        try
        {
            getUninterruptibly(chain);
        }
        catch (ExecutionException e)
        {
            // ignore
        }
    }

    public static void awaitUninterruptiblyAndRethrow(AsyncChain<?> chain)
    {
        try
        {
            getUninterruptibly(chain);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }
}
