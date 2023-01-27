package accord.impl.basic;

import org.apache.cassandra.concurrent.FutureTask;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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

    private Task<?> newTaskFor(Runnable command) {
        return command instanceof Task ? (Task<?>) command : newTaskFor(command, null);
    }

    @Override
    public void execute(Runnable command) {
        Task<?> task = newTaskFor(command);
        int jitterMillis = THREAD_SCHEDULING_OVERHEAD_MILLIS + random.nextInt(1000);
        pending.add(task, jitterMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
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
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return false;
    }


    private static class Task<T> extends FutureTask<T> implements Pending
    {

        public Task(Callable<T> fn)
        {
            super(fn);
        }
    }
}
