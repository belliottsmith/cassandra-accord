package accord.utils.async;

import com.google.common.util.concurrent.MoreExecutors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
// To run faster, uncomment the below
@Fork(1)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
public class AsyncChainBenchmark {

    @Benchmark
    public AsyncResult<Integer> chainMap1()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainMap2()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainMap3()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapResult1()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult.Settable<Integer> map1 = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap2(i -> map1)
                .beginAsResult();
        head.trySuccess(0);
        map1.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapResult2()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult.Settable<Integer> map1 = AsyncResults.settable();
        AsyncResult.Settable<Integer> map2 = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap2(i -> map1)
                .flatMap2(i -> map2)
                .beginAsResult();
        head.trySuccess(0);
        map1.trySuccess(0);
        map2.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapResult3()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult.Settable<Integer> map1 = AsyncResults.settable();
        AsyncResult.Settable<Integer> map2 = AsyncResults.settable();
        AsyncResult.Settable<Integer> map3 = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap(a -> map1)
                .flatMap(a -> map2)
                .flatMap(a -> map3)
                .beginAsResult();
        head.trySuccess(0);
        map1.trySuccess(0);
        map2.trySuccess(0);
        map3.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap1()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap(i -> AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 0))
                .beginAsResult();
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap2()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap(i -> AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 0))
                .flatMap(i -> AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 0))
                .beginAsResult();
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap3()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap(i -> AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 0))
                .flatMap(i -> AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 0))
                .flatMap(i -> AsyncChains.ofCallable(MoreExecutors.directExecutor(), () -> 0))
                .beginAsResult();
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> resultMap1()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .map2(AsyncChainBenchmark::map);
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> resultMap2()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .map2(AsyncChainBenchmark::map)
                .map2(AsyncChainBenchmark::map);
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> resultMap3()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .map2(AsyncChainBenchmark::map)
                .map2(AsyncChainBenchmark::map)
                .map2(AsyncChainBenchmark::map);
        head.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap1()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult.Settable<Integer> map1 = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap2(i -> map1);
        head.trySuccess(0);
        map1.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap2()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult.Settable<Integer> map1 = AsyncResults.settable();
        AsyncResult.Settable<Integer> map2 = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap2(i -> map1)
                .flatMap2(i -> map2);
        head.trySuccess(0);
        map1.trySuccess(0);
        map2.trySuccess(0);
        return result;
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap3()
    {
        AsyncResult.Settable<Integer> head = AsyncResults.settable();
        AsyncResult.Settable<Integer> map1 = AsyncResults.settable();
        AsyncResult.Settable<Integer> map2 = AsyncResults.settable();
        AsyncResult.Settable<Integer> map3 = AsyncResults.settable();
        AsyncResult<Integer> result = head
                .flatMap2(i -> map1)
                .flatMap2(i -> map2)
                .flatMap2(i -> map3);
        head.trySuccess(0);
        map1.trySuccess(0);
        map2.trySuccess(0);
        map3.trySuccess(0);
        return result;
    }

    private static <A> A map(A a)
    {
        return a;
    }

    private static <A> AsyncResult<A> flatMap(A a) {
        // avoid Immediate as the overhead is less there
        AsyncResults.Settable<A> promise = new AsyncResults.Settable<>();
        promise.setSuccess(a);
        return promise;
    }
}
