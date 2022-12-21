package accord.utils.async;

import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Handle for async computations that supports multiple listeners and registering
 * listeners after the computation has started
 */
public interface AsyncResult<V> extends AsyncChain<V>
{
    AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback);

    default AsyncResult<V> addCallback(Runnable runnable)
    {
        return addCallback((unused, failure) -> {
            if (failure == null) runnable.run();
            else throw new RuntimeException(failure);
        });
    }

    default <T> AsyncResult<T> map2(Function<? super V, ? extends T> mapper, Executor executor)
    {
        AsyncResult.Settable<T> settable = AsyncResults.settable();
        addCallback((success, failure) -> {
            if (failure != null)
            {
                settable.setFailure(failure);
                return;
            }
            try
            {
                T result = mapper.apply(success);
                settable.setSuccess(result);
            }
            catch (Throwable t)
            {
                settable.setFailure(t);
            }
        }, executor);
        return settable;
    }

    default <T> AsyncResult<T> map2(Function<? super V, ? extends T> mapper)
    {
        return map2(mapper, MoreExecutors.directExecutor());
    }

    default <T> AsyncResult<T> flatMap2(Function<? super V, ? extends AsyncResult<T>> mapper)
    {
        return flatMap2(mapper, MoreExecutors.directExecutor());
    }

    default <T> AsyncResult<T> flatMap2(Function<? super V, ? extends AsyncResult<T>> mapper, Executor executor)
    {
        AsyncResult.Settable<T> settable = AsyncResults.settable();
        addCallback((success, failure) -> {
           if (failure != null)
           {
               settable.setFailure(failure);
               return;
           }
           try
           {
               AsyncResult<T> next = mapper.apply(success);
               next.addCallback((s2, f2) -> {
                   if (f2 != null)
                   {
                       settable.tryFailure(f2);
                       return;
                   }
                   settable.trySuccess(s2);
               });
           }
           catch (Throwable t)
           {
               settable.setFailure(t);
           }
        }, executor);
        return settable;
    }

    default AsyncResult<V> addCallback(Runnable runnable, Executor executor)
    {
        addCallback(AsyncCallbacks.inExecutor(runnable, executor));
        return this;
    }

    boolean isDone();
    boolean isSuccess();

    default AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        return addCallback(AsyncCallbacks.inExecutor(callback, executor));
    }

    default AsyncResult<V> addListener(Runnable runnable)
    {
        return addCallback(runnable);
    }

    default AsyncResult<V> addListener(Runnable runnable, Executor executor)
    {
        return addCallback(runnable, executor);
    }

    @Override
    default void begin(BiConsumer<? super V, Throwable> callback)
    {
        addCallback(callback);
    }

    default AsyncResult<V> beginAsResult()
    {
        return this;
    }

    interface Settable<V> extends AsyncResult<V>
    {
        boolean trySuccess(V value);

        default void setSuccess(V value)
        {
            if (!trySuccess(value))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        boolean tryFailure(Throwable throwable);

        default void setFailure(Throwable throwable)
        {
            if (!tryFailure(throwable))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        default BiConsumer<V, Throwable> settingCallback()
        {
            return (result, throwable) -> {

                if (throwable == null)
                    trySuccess(result);
                else
                    tryFailure(throwable);
            };
        }
    }
}
