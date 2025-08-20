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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import accord.utils.Reduce;

import static accord.utils.Invariants.illegalState;

public abstract class AsyncChains<V> implements AsyncChain<V>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncChains.class);

    static class ImmediateSuccess<V> extends AsyncChains.Head<V>
    {
        final private V success;
        private ImmediateSuccess(V success) { this.success = success; }

        @Override
        protected Cancellable start(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(success, null);
            return null;
        }
    }

    static class ImmediateFailure<V> extends AsyncChains.Head<V>
    {
        final private Throwable failure;
        private ImmediateFailure(Throwable failure) { this.failure = failure; }

        @Override
        protected Cancellable start(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(null, failure);
            return null;
        }
    }

    public abstract static class Head<V> extends AsyncChains<V> implements BiConsumer<V, Throwable>
    {
        protected Head()
        {
            super(null);
            next = this;
        }

        protected abstract @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback);

        @Override
        public final @Nullable Cancellable begin(BiConsumer<? super V, Throwable> callback)
        {
            Invariants.requireArgument(next != null);
            next = null;
            return start(callback);
        }

        Cancellable begin()
        {
            Invariants.requireArgument(next != null);
            BiConsumer<? super V, Throwable> next = this.next;
            this.next = null;
            return start(next);
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
        public Cancellable begin(BiConsumer<? super O, Throwable> callback)
        {
            Invariants.requireArgument(!(callback instanceof AsyncChains.Head));
            checkNextIsHead();
            Head<?> head = (Head<?>) next;
            next = callback;
            return head.begin();
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
            else
            {
                O update;
                try
                {
                    update = apply(i);
                }
                catch (Throwable t)
                {
                    next.accept(null, t);
                    return;
                }
                next.accept(update, null);
            }
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
            try
            {
                return map.apply(i);
            }
            catch (Throwable t)
            {
                return AsyncChains.failure(t);
            }
        }
    }

    public static abstract class Recover<I> extends Link<I, I> implements Function<Throwable, AsyncChain<I>>
    {
        Recover(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable == null)
            {
                next.accept(i, null);
                return;
            }
            AsyncChain<I> recover = apply(throwable);
            if (recover == null) next.accept(null, throwable);
            else                 recover.begin(next);
        }
    }

    static class EncapsulatedRecover<I> extends Recover<I>
    {
        private final Function<? super Throwable, ? extends AsyncChain<I>> map;

        public EncapsulatedRecover(Head<?> head, Function<? super Throwable, ? extends AsyncChain<I>> function)
        {
            super(head);
            this.map = function;
        }

        @Override
        public AsyncChain<I> apply(Throwable throwable)
        {
            try
            {
                return map.apply(throwable);
            }
            catch (Throwable t)
            {
                return AsyncChains.failure(t);
            }
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
            callback.accept(i, throwable);
            super.accept(i, throwable);
        }
    }

    private static class DetectLeak extends AsyncChains.Head<Void>
    {
        private final AtomicBoolean called = new AtomicBoolean(false);
        private final Throwable caller = new IllegalStateException("AsyncChain.begin not called");
        private final Consumer<Throwable> onLeak;
        private final Runnable onCall;

        private DetectLeak(Consumer<Throwable> onLeak, Runnable onCall)
        {
            this.onLeak = Objects.requireNonNull(onLeak);
            this.onCall = Objects.requireNonNull(onCall);
        }

        @Override
        protected Cancellable start(BiConsumer<? super Void, Throwable> callback)
        {
            called.set(true);
            onCall.run();
            callback.accept(null, null);
            return null;
        }

        @Override
        protected void finalize()
        {
            if (!called.get())
                onLeak.accept(caller);
        }
    }

    @VisibleForTesting
    static abstract class AbstractReducingAsyncChain<I, O> extends AsyncChainCombiner<I, O>
    {
        private AbstractReducingAsyncChain(AsyncChain<I> accum, AsyncChain<I> add)
        {
            super(list(accum, add));
        }

        private AbstractReducingAsyncChain(List<? extends AsyncChain<? extends I>> list)
        {
            super(list);
        }

        private static <V> List<AsyncChain<V>> list(AsyncChain<V> accum, AsyncChain<V> add)
        {
            List<AsyncChain<V>> list = new ArrayList<>(2);
            list.add(accum);
            list.add(add);
            return list;
        }

        void add(AsyncChain<I> a)
        {
            inputs().add(a);
        }

        @VisibleForTesting
        int size()
        {
            return inputs().size();
        }
    }

    @VisibleForTesting
    static class ReducingFunctionAsyncChain<V> extends AbstractReducingAsyncChain<V, V>
    {
        private final BiFunction<? super V, ? super V, ? extends V> reducer;

        private ReducingFunctionAsyncChain(AsyncChain<V> accum, AsyncChain<V> add, BiFunction<? super V, ? super V, ? extends V> reducer)
        {
            super(accum, add);
            this.reducer = reducer;
        }

        private ReducingFunctionAsyncChain(List<? extends AsyncChain<? extends V>> list, BiFunction<? super V, ? super V, ? extends V> reducer)
        {
            super(list);
            this.reducer = reducer;
        }

        private static <V> boolean match(AsyncChain<V> accum, BiFunction<? super V, ? super V, ? extends V> reducer)
        {
            return accum instanceof ReducingFunctionAsyncChain && ((ReducingFunctionAsyncChain<?>) accum).reducer == reducer;
        }

        @Override
        V process(V[] inputs)
        {
            V result = inputs[0];
            for (int i = 1; i < inputs.length; i++)
                result = reducer.apply(result, inputs[i]);
            return result;
        }
    }

    @VisibleForTesting
    static class ReducingAsyncChain<V> extends AbstractReducingAsyncChain<V, V>
    {
        private final Reduce<V, V> reducer;

        private ReducingAsyncChain(List<? extends AsyncChain<? extends V>> list, Reduce<V, V> reducer)
        {
            super(list);
            this.reducer = reducer;
        }

        private ReducingAsyncChain(AsyncChain<V> accum, AsyncChain<V> add, Reduce<V, V> reducer)
        {
            super(accum, add);
            this.reducer = reducer;
        }

        private static <V> boolean match(AsyncChain<V> accum, Reduce<? super V, ? extends V> reducer)
        {
            return accum instanceof ReducingAsyncChain && ((ReducingAsyncChain<?>) accum).reducer == reducer;
        }

        @Override
        V process(V[] inputs)
        {
            V result = inputs[0];
            for (int i = 1; i < inputs.length; i++)
                result = reducer.reduce(result, inputs[i]);
            return result;
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
    public AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
    {
        return add(EncapsulatedRecover::new, mapper);
    }

    @Override
    public AsyncChain<V> invoke(BiConsumer<? super V, Throwable> callback)
    {
        return add(EncapsulatedCallback::new, callback);
    }

    // can be used by transformations that want efficiency, and can directly extend Link, FlatMap or Callback
    // (or perhaps some additional helper implementations that permit us to simply implement apply for Map and FlatMap)
    <O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> add(Function<Head<?>, T> factory)
    {
        checkNextIsHead();
        Head<?> head = (Head<?>) next;
        T result = factory.apply(head);
        next = result;
        return result;
    }

    <P, O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> add(BiFunction<Head<?>, P, T> factory, P param)
    {
        checkNextIsHead();
        Head<?> head = (Head<?>) next;
        T result = factory.apply(head, param);
        next = result;
        return result;
    }

    protected void checkNextIsHead()
    {
        Invariants.require(next != null, "Begin was called multiple times");
        Invariants.require(next instanceof Head<?>, "Next is not an instance of AsyncChains.Head (it is %s); was map/flatMap called on the same object multiple times?", next.getClass());
    }

    public static AsyncChain<?> detectLeak(Consumer<Throwable> onLeak, Runnable onCall)
    {
        return new DetectLeak(onLeak, onCall);
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
                logger.trace("AsyncChain Callable threw an Exception", t);
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
                logger.debug("AsyncChain Runnable threw an Exception", t);
                receiver.accept(null, t);
            }
        };
    }

    public static <V> AsyncChain<V> success(V success)
    {
        return new ImmediateSuccess<>(success);
    }

    public static <V> AsyncChain<V> failure(Throwable failure)
    {
        return new ImmediateFailure<>(failure);
    }

    public static <V, T> AsyncChain<T> map(AsyncChain<V> chain, Function<? super V, ? extends T> mapper, Executor executor)
    {
        // type parameter needed for compilation for some reason on some JDKs
        return chain.flatMap(v -> new Head<T>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                return AsyncChains.submit(executor, callback, () -> {
                    T value;
                    try
                    {
                        value = mapper.apply(v);
                    }
                    catch (Throwable t)
                    {
                        callback.accept(null, t);
                        return;
                    }
                    callback.accept(value, null);
                });
            }
        });
    }

    public static <V, T> AsyncChain<T> flatMap(AsyncChain<V> chain, Function<? super V, ? extends AsyncChain<T>> mapper, Executor executor)
    {
        return chain.flatMap(v -> new Head<T>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                return AsyncChains.submit(executor, callback, () -> {
                    try
                    {
                        mapper.apply(v).begin(callback);
                    }
                    catch (Throwable t)
                    {
                        callback.accept(null, t);
                    }
                });
            }
        });
    }

    private static Cancellable submit(Executor executor, BiConsumer<?, Throwable> callback, Runnable run)
    {
        try
        {
            if (executor instanceof ExecutorService)
            {
                Future<?> future = ((ExecutorService) executor).submit(run);
                return () -> future.cancel(true);
            }
            else
            {
                executor.execute(run);
                return null;
            }
        }
        catch (Throwable t)
        {
            // TODO (low priority, correctness): If the executor is shutdown then the callback may run in an unexpected thread, which may not be thread safe
            callback.accept(null, t);
            return null;
        }
    }

    public static <V> AsyncChain<V> ofCallable(Executor executor, Callable<V> callable)
    {
        return ofCallable(executor, callable, AsyncChains::encapsulate);
    }

    public static <V> AsyncChain<V> ofCallable(Executor executor,
                                               Callable<V> callable,
                                               BiFunction<Callable<V>, BiConsumer<? super V, Throwable>, Runnable> encapsulator)
    {
        return new Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return AsyncChains.submit(executor, callback, encapsulator.apply(callable, callback));
            }
        };
    }

    public static AsyncChain<Void> ofRunnable(Executor executor, Runnable runnable)
    {
        return new Head<Void>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super Void, Throwable> callback)
            {
                return AsyncChains.submit(executor, callback, encapsulate(runnable, callback));
            }
        };
    }

    public static <V> AsyncChain<List<V>> allOf(List<? extends AsyncChain<? extends V>> chains)
    {
        return new AsyncChainCombiner<>(chains) {

            @Override
            List<V> process(V[] inputs)
            {
                return Arrays.asList(inputs);
            }
        };
    }

    public static <V> AsyncChain<V> reduce(List<? extends AsyncChain<? extends V>> chains, Reduce<V, V> reducer)
    {
        if (chains.size() == 1)
            return (AsyncChain<V>) chains.get(0);
        return new ReducingAsyncChain<>(chains, reducer);
    }

    public static <I, O> AsyncChain<O> reduce(List<? extends AsyncChain<? extends I>> chains, Reduce<I, O> reducer, O identity)
    {
        switch (chains.size())
        {
            case 0: return AsyncChains.success(identity);
            case 1: return chains.get(0).map(a -> reducer.reduce(identity, a));
        }
        return new AbstractReducingAsyncChain<>(chains)
        {
            @Override
            O process(I[] inputs)
            {
                O result = identity;
                for (I input : inputs)
                    result = reducer.reduce(result, input);
                return result;
            }
        };
    }

    public static <V> AsyncChain<V> reduce(AsyncChain<V> accum, AsyncChain<V> add, Reduce<V, V> reducer)
    {
        if (ReducingAsyncChain.match(accum, reducer))
        {
            ((ReducingAsyncChain<V>) accum).add(add);
            return accum;
        }
        return new ReducingAsyncChain<>(accum, add, reducer);
    }

    // TODO (expected): move this methods to test-only; in Cassandra we should not be using these as not simulator safe
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

    public static <V> V getUninterruptibly(AsyncChain<V> chain) throws ExecutionException
    {
        try
        {
            return getUninterruptibly(chain, 0, TimeUnit.DAYS);
        }
        catch (TimeoutException e)
        {
            throw illegalState("Should not throw timeout exception e");
        }
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
