package accord.impl.basic;

import accord.burn.random.Decision;
import accord.burn.random.IntRange;
import accord.burn.random.RandomInt;
import accord.burn.random.SegmentedIntRange;
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
    private final PendingQueue pending;
    private final RandomInt jitterInNano;

    public DelayedExecutorService(PendingQueue pending, Random random)
    {
        this.pending = pending;
        this.jitterInNano = new SegmentedIntRange(
                new IntRange(random, microToNanos(0), microToNanos(50)),
                new IntRange(random, microToNanos(50), msToNanos(5)),
                // this is different from Apache Cassandra Simulator as this is computed differently for each executor
                // rather than being a global config
                new Decision.FixedChance(random, new IntRange(random, 1, 10).getInt() / 100f)
        );
    }

    private static int msToNanos(int value)
    {
        return Math.toIntExact(TimeUnit.MILLISECONDS.toNanos(value));
    }

    private static int microToNanos(int value)
    {
        return Math.toIntExact(TimeUnit.MICROSECONDS.toNanos(value));
    }

    @Override
    protected <T> Task<T> newTaskFor(Runnable runnable, T value)
    {
        return newTaskFor(Executors.callable(runnable, value));
    }

    @Override
    protected <T> Task<T> newTaskFor(Callable<T> callable)
    {
        return new Task<>(callable);
    }

    private Task<?> newTaskFor(Runnable command)
    {
        return command instanceof Task ? (Task<?>) command : newTaskFor(command, null);
    }

    @Override
    public void execute(Runnable command)
    {
        pending.add(newTaskFor(command), jitterInNano.getInt(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
    {
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
