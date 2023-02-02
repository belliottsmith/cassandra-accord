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

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import accord.utils.Invariants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import static accord.utils.async.AsyncChainCombiner.Reduce;

public abstract class AsyncChains<V> implements AsyncChain<V>
{
    static class Immediate<V> implements AsyncChain<V>
    {
        static class FailureHolder
        {
            final Throwable cause;
            FailureHolder(Throwable cause)
            {
                this.cause = cause;
            }
        }

        final private Object value;
        private Immediate(V success) { this.value = success; }
        private Immediate(Throwable failure) { this.value = new FailureHolder(failure); }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            if (value != null && value.getClass() == FailureHolder.class)
                return (AsyncChain<T>) this;
            return new Immediate<>(mapper.apply((V) value));
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            if (value != null && value.getClass() == FailureHolder.class)
                return (AsyncChain<T>) this;
            return mapper.apply((V) value);
        }

        @Override
        public AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback)
        {
            if (value == null || value.getClass() != FailureHolder.class)
                callback.accept((V) value, null);
            else
                callback.accept(null, ((FailureHolder)value).cause);
            return this;
        }

        @Override
        public void begin(BiConsumer<? super V, Throwable> callback)
        {
            addCallback(callback);
        }
    }

    public abstract static class Head<V> extends AsyncChains<V> implements BiConsumer<V, Throwable>
    {
        protected Head()
        {
            super(null);
            next = this;
        }

        void begin()
        {
            Invariants.checkArgument(next != null);
            BiConsumer<? super V, Throwable> next = this.next;
            this.next = null;
            begin(next);
        }

        @Override
        public void accept(V v, Throwable throwable)
        {
            // we implement here just to simplify logic a little
            throw new UnsupportedOperationException();
        }
    }

    static abstract class Link<I, O> extends AsyncChains<O> implements BiConsumer<I, Throwable>
    {
        protected Link(Head<?> head)
        {
            super(head);
        }

        @Override
        public void begin(BiConsumer<? super O, Throwable> callback)
        {
            Preconditions.checkArgument(!(callback instanceof AsyncChains.Head));
            Preconditions.checkState(next instanceof AsyncChains.Head);
            Head<?> head = (Head<?>) next;
            next = callback;
            head.begin();
        }
    }

    public static abstract class Map<I, O> extends Link<I, O> implements Function<I, O>
    {
        Map(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next.accept(null, throwable);
            else next.accept(apply(i), null);
        }
    }

    static class EncapsulatedMap<I, O> extends Map<I, O>
    {
        final Function<? super I, ? extends O> map;

        EncapsulatedMap(Head<?> head, Function<? super I, ? extends O> map)
        {
            super(head);
            this.map = map;
        }

        @Override
        public O apply(I i)
        {
            return map.apply(i);
        }
    }

    public static abstract class FlatMap<I, O> extends Link<I, O> implements Function<I, AsyncChain<O>>
    {
        FlatMap(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next.accept(null, throwable);
            else apply(i).begin(next);
        }
    }

    static class EncapsulatedFlatMap<I, O> extends FlatMap<I, O>
    {
        final Function<? super I, ? extends AsyncChain<O>> map;

        EncapsulatedFlatMap(Head<?> head, Function<? super I, ? extends AsyncChain<O>> map)
        {
            super(head);
            this.map = map;
        }

        @Override
        public AsyncChain<O> apply(I i)
        {
            return map.apply(i);
        }
    }

    // if extending Callback, be sure to invoke super.accept()
    static class Callback<I> extends Link<I, I>
    {
        Callback(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            next.accept(i, throwable);
        }
    }

    static class EncapsulatedCallback<I> extends Callback<I>
    {
        final BiConsumer<? super I, Throwable> callback;

        EncapsulatedCallback(Head<?> head, BiConsumer<? super I, Throwable> callback)
        {
            super(head);
            this.callback = callback;
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            super.accept(i, throwable);
            callback.accept(i, throwable);
        }
    }

    // either the thing we start, or the thing we do in follow-up
    BiConsumer<? super V, Throwable> next;
    AsyncChains(Head<?> head)
    {
        this.next = (BiConsumer) head;
    }

    @Override
    public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
    {
        return add(EncapsulatedMap::new, mapper);
    }

    @Override
    public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
    {
        return add(EncapsulatedFlatMap::new, mapper);
    }

    @Override
    public AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        return add(EncapsulatedCallback::new, callback);
    }

    // can be used by transformations that want efficiency, and can directly extend Link, FlatMap or Callback
    // (or perhaps some additional helper implementations that permit us to simply implement apply for Map and FlatMap)
    <O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> add(Function<Head<?>, T> factory)
    {
        Preconditions.checkState(next instanceof Head<?>);
        Head<?> head = (Head<?>) next;
        T result = factory.apply(head);
        next = result;
        return result;
    }

    <P, O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> add(BiFunction<Head<?>, P, T> factory, P param)
    {
        Preconditions.checkState(next instanceof Head<?>);
        Head<?> head = (Head<?>) next;
        T result = factory.apply(head, param);
        next = result;
        return result;
    }

    private static <V> Runnable encapsulate(Callable<V> callable, BiConsumer<? super V, Throwable> receiver)
    {
        return () -> {
            try
            {
                V result = callable.call();
                receiver.accept(result, null);
            }
            catch (Throwable t)
            {
                receiver.accept(null, t);
            }
        };
    }

    private static Runnable encapsulate(Runnable runnable, BiConsumer<? super Void, Throwable> receiver)
    {
        return () -> {
            try
            {
                runnable.run();
                receiver.accept(null, null);
            }
            catch (Throwable t)
            {
                receiver.accept(null, t);
            }
        };
    }

    public static <V> AsyncChain<V> success(V success)
    {
        return new Immediate<>(success);
    }

    public static <V> AsyncChain<V> failure(Throwable failure)
    {
        return new Immediate<>(failure);
    }

    public static <V> AsyncChain<V> ofCallable(Executor executor, Callable<V> callable)
    {
        return new Head<V>()
        {
            @Override
            public void begin(BiConsumer<? super V, Throwable> next)
            {
                executor.execute(encapsulate(callable, next));
            }
        };
    }

    public static AsyncChain<Void> ofRunnable(Executor executor, Runnable runnable)
    {
        return new Head<Void>()
        {
            @Override
            public void begin(BiConsumer<? super Void, Throwable> callback)
            {
                executor.execute(AsyncChains.encapsulate(runnable, callback));
            }
        };
    }

    public static <V> AsyncChain<List<V>> all(List<AsyncChain<V>> chains)
    {
        Preconditions.checkArgument(!chains.isEmpty());
        return new AsyncChainCombiner.All<>(chains);
    }

    public static <V> AsyncChain<V> reduce(List<AsyncChain<V>> chains, BiFunction<V, V, V> reducer)
    {
        Preconditions.checkArgument(!chains.isEmpty());
        if (chains.size() == 1)
            return chains.get(0);
        if (Reduce.canAppendTo(chains.get(0), reducer))
        {
            AsyncChainCombiner.Reduce<V> appendTo = (AsyncChainCombiner.Reduce<V>) chains.get(0);
            appendTo.addAll(chains.subList(1, chains.size()));
            return appendTo;
        }
        return new Reduce<>(chains, reducer);
    }

    public static <V> AsyncChain<V> reduce(AsyncChain<V> a, AsyncChain<V> b, BiFunction<V, V, V> reducer)
    {
        if (Reduce.canAppendTo(a, reducer))
        {
            AsyncChainCombiner.Reduce<V> appendTo = (AsyncChainCombiner.Reduce<V>) a;
            appendTo.add(b);
            return a;
        }
        return new Reduce<>(Lists.newArrayList(a, b), reducer);
    }

    public static <V> V getBlocking(AsyncChain<V> chain) throws InterruptedException, ExecutionException
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

        latch.await();
        Result result = callbackResult.get();
        if (result.failure == null) return result.result;
        else throw new ExecutionException(result.failure);
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

        if (!latch.await(timeout, unit))
            throw new TimeoutException();
        Result result = callbackResult.get();
        if (result.failure == null) return result.result;
        else throw new ExecutionException(result.failure);
    }

    public static <V> V getUninterruptibly(AsyncChain<V> chain)
    {
        try
        {
            return getBlocking(chain);
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <V> void awaitUninterruptibly(AsyncChain<V> chain)
    {
        getUninterruptibly(chain);
    }
}