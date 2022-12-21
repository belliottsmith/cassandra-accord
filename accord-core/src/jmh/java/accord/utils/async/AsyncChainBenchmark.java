package accord.utils.async;

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
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
public class AsyncChainBenchmark {

    @Benchmark
    public AsyncResult<Integer> chainMap1()
    {
        return AsyncResults.success(0)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainMap2()
    {
        return AsyncResults.success(0)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainMap3()
    {
        return AsyncResults.success(0)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap1()
    {
        return AsyncResults.success(0)
                .flatMap(a -> flatMap(a))
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap2()
    {
        return AsyncResults.success(0)
                .flatMap(a -> flatMap(a))
                .flatMap(a -> flatMap(a))
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap3()
    {
        return AsyncResults.success(0)
                .flatMap(a -> flatMap(a))
                .flatMap(a -> flatMap(a))
                .flatMap(a -> flatMap(a))
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> resultMap1()
    {
        return AsyncResults.success(0)
                .map2(AsyncChainBenchmark::map);
    }

    @Benchmark
    public AsyncResult<Integer> resultMap2()
    {
        return AsyncResults.success(0)
                .map2(AsyncChainBenchmark::map)
                .map2(AsyncChainBenchmark::map);
    }

    @Benchmark
    public AsyncResult<Integer> resultMap3()
    {
        return AsyncResults.success(0)
                .map2(AsyncChainBenchmark::map)
                .map2(AsyncChainBenchmark::map)
                .map2(AsyncChainBenchmark::map);
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap1()
    {
        return AsyncResults.success(0)
                .flatMap2(AsyncChainBenchmark::flatMap);
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap2()
    {
        return AsyncResults.success(0)
                .flatMap2(AsyncChainBenchmark::flatMap)
                .flatMap2(AsyncChainBenchmark::flatMap);
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap3()
    {
        return AsyncResults.success(0)
                .flatMap2(AsyncChainBenchmark::flatMap)
                .flatMap2(AsyncChainBenchmark::flatMap)
                .flatMap2(AsyncChainBenchmark::flatMap);
    }


    @Benchmark
    public AsyncResult<Integer> chainMapAndBack1()
    {
        return AsyncResults.success(0)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainMapAndBack2()
    {
        return AsyncResults.success(0)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainMapAndBack3()
    {
        return AsyncResults.success(0)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .map(AsyncChainBenchmark::map)
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapAndBack1()
    {
        return AsyncResults.success(0)
                .flatMap(a -> flatMap(a))
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapAndBack2()
    {
        return AsyncResults.success(0)
                .flatMap(a -> flatMap(a))
                .flatMap(a -> flatMap(a))
                .beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapAndBack3()
    {
        return AsyncResults.success(0)
                .flatMap(a -> flatMap(a))
                .flatMap(a -> flatMap(a))
                .flatMap(a -> flatMap(a))
                .beginAsResult();
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
