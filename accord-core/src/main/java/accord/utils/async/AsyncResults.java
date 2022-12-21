package accord.utils.async;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AsyncResults
{
    private AsyncResults() {}

    private static class Result<V>
    {
        final V value;
        final Throwable failure;

        public Result(V value, Throwable failure)
        {
            this.value = value;
            this.failure = failure;
        }
    }

    private static <V> void notify(Executor executor, BiConsumer<? super V, Throwable> callback, V value, Throwable failure)
    {
        try
        {
            if (executor != null)
            {
                executor.execute(() -> callback.accept(value, failure));
                return;
            }
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
        }
        //TODO we don't have a equivalent of org.apache.cassandra.concurrent.ExecutionFailure#handle, so if the user
        // logic fails, "should" we propagate?
        callback.accept(value, failure);
    }

    static class AbstractResult<V> implements AsyncResult<V>
    {
        private static final AtomicReferenceFieldUpdater<AbstractResult, Object> STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractResult.class, Object.class, "state");

        private volatile Object state;

        private static class Listener<V>
        {
            final BiConsumer<? super V, Throwable> callback;
            final Executor executor;
            Listener<V> next;

            public Listener(BiConsumer<? super V, Throwable> callback, Executor executor)
            {
                this.callback = callback;
                this.executor = executor;
            }
        }

        private void notify(Listener<V> listener, Result<V> result)
        {
            while (listener != null)
            {
                AsyncResults.notify(listener.executor, listener.callback, result.value, result.failure);
                listener = listener.next;
            }
        }

        boolean trySetResult(Result<V> result)
        {
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                    return false;
                Listener<V> listener = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, result))
                {
                    notify(listener, result);
                    return true;
                }
            }
        }

        boolean trySetResult(V result, Throwable failure)
        {
            return trySetResult(new Result<>(result, failure));
        }

        void setResult(Result<V> result)
        {
            if (!trySetResult(result))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        private AsyncChain<V> newChain()
        {
            return new AsyncChains.Head<V>()
            {
                @Override
                public void begin(BiConsumer<? super V, Throwable> callback)
                {
                    AbstractResult.this.addCallback(callback);
                }
            };
        }


        void setResult(V result, Throwable failure)
        {
            if (!trySetResult(result, failure))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            return newChain().map(mapper);
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            return newChain().flatMap(mapper);
        }

        @Override
        public AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback)
        {
            Listener<V> listener = null;
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                {
                    Result<V> result = (Result<V>) current;
                    callback.accept(result.value, result.failure);
                    return null;
                }
                if (listener == null)
                    listener = new Listener<>(callback, null);

                listener.next = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, listener))
                    return this;
            }
        }

        @Override
        public boolean isDone()
        {
            return state instanceof Result;
        }

        @Override
        public boolean isSuccess()
        {
            Object current = state;
            return current instanceof Result && ((Result) current).failure == null;
        }
    }

    static class Chain<V> extends AbstractResult<V>
    {
        public Chain(AsyncChain<V> chain)
        {
            chain.begin(this::setResult);
        }
    }

    public static class Settable<V> extends AbstractResult<V> implements AsyncResult.Settable<V>
    {
        @Override
        public boolean trySuccess(V value)
        {
            return trySetResult(value, null);
        }

        @Override
        public boolean tryFailure(Throwable throwable)
        {
            return trySetResult(null, throwable);
        }
    }

    static class Immediate<V> implements AsyncResult<V>
    {
        private final V value;
        private final Throwable failure;

        Immediate(V value)
        {
            this.value = value;
            this.failure = null;
        }

        Immediate(Throwable failure)
        {
            this.value = null;
            this.failure = failure;
        }

        private AsyncChain<V> newChain()
        {
            return new AsyncChains.Head<V>()
            {
                @Override
                public void begin(BiConsumer<? super V, Throwable> callback)
                {
                    AsyncResults.Immediate.this.addCallback(callback);
                }
            };
        }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            return newChain().map(mapper);
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            return newChain().flatMap(mapper);
        }

        @Override
        public AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(value, failure);
            return this;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public boolean isSuccess()
        {
            return failure == null;
        }
    }

    /**
     * Creates a notifier for the given chain. This calls begin on the supplied chain
     */
    static <V> AsyncResult<V> forChain(AsyncChain<V> chain)
    {
        return new Chain<>(chain);
    }

    public static <V> AsyncResult<V> success(V value)
    {
        return new Immediate<>(value);
    }

    public static <V> AsyncResult<V> failure(Throwable failure)
    {
        return new Immediate<>(failure);
    }

    public static <V> AsyncResult<V> ofCallable(Executor executor, Callable<V> callable)
    {
        Settable<V> notifier = new Settable<V>();
        executor.execute(() -> {
            try
            {
                notifier.trySuccess(callable.call());
            }
            catch (Exception e)
            {
                notifier.tryFailure(e);
            }
        });
        return notifier;
    }

    public static AsyncResult<Void> ofRunnable(Executor executor, Runnable runnable)
    {
        Settable<Void> notifier = new Settable<Void>();
        executor.execute(() -> {
            try
            {
                runnable.run();
                notifier.trySuccess(null);
            }
            catch (Exception e)
            {
                notifier.tryFailure(e);
            }
        });
        return notifier;
    }

    public static <V> AsyncResult.Settable<V> settable()
    {
        return new Settable<>();
    }

    public static <V> AsyncChain<List<V>> all(List<AsyncChain<V>> results)
    {
        Preconditions.checkArgument(!results.isEmpty());
        return new AsyncChainCombiner.All<>(results);
    }

    public static <V> AsyncChain<V> reduce(List<AsyncChain<V>> results, BiFunction<V, V, V> reducer)
    {
        Preconditions.checkArgument(!results.isEmpty());
        if (results.size() == 1)
            return results.get(0);
        return new AsyncChainCombiner.Reduce<>(results, reducer);
    }

    public static <V> V getBlocking(AsyncResult<V> notifier) throws InterruptedException
    {
        AtomicReference<Result<V>> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        notifier.addCallback((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        latch.await();
        Result<V> result = callbackResult.get();
        if (result.failure == null) return result.value;
        else throw new RuntimeException(result.failure);
    }

    public static <V> V getUninterruptibly(AsyncResult<V> notifier)
    {
        try
        {
            return getBlocking(notifier);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <V> void awaitUninterruptibly(AsyncResult<V> notifier)
    {
        getUninterruptibly(notifier);
    }
}
