package accord.impl.basic;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class DelayedExecutorService extends AbstractExecutorService
{
    private static final int THREAD_SCHEDULING_OVERHEAD_MILLIS = 5;

    private final PendingQueue pending;
    private final Random random;

    public DelayedExecutorService(PendingQueue pending, Random random)
    {
        this.pending = pending;
        this.random = random;
    }

    @Override
    protected <T> Task<T> newTaskFor(Runnable runnable, T value) {
        return newTaskFor(Executors.callable(runnable, value));
    }

    @Override
    protected <T> Task<T> newTaskFor(Callable<T> callable) {
        return new Task<>(callable);
    }

    private Task<?> newTaskFor(@NotNull Runnable command) {
        return command instanceof Task ? (Task<?>) command : newTaskFor(command, null);
    }

    @Override
    public void execute(@NotNull Runnable command) {
        Task<?> task = newTaskFor(command);
        int jitterMillis = THREAD_SCHEDULING_OVERHEAD_MILLIS + random.nextInt(1000);
        pending.add(task, jitterMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
    }

    @NotNull
    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
        return false;
    }


    private static class Task<T> extends CompletableFuture<T> implements RunnableFuture<T>, Pending
    {
        private final Callable<T> fn;

        public Task(Callable<T> fn)
        {
            this.fn = fn;
        }

        @Override
        public void run()
        {
            try
            {
                complete(fn.call());
            }
            catch (Throwable t)
            {
                completeExceptionally(t);
            }
        }
    }
}
