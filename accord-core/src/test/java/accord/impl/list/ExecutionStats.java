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

package accord.impl.list;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;

import accord.coordinate.ExecutePath;
import accord.local.Node;
import accord.primitives.Deps;
import accord.primitives.TxnId;

import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;

/**
 * Cluster-wide counts of coordinated execution attempts and completed executions, so that we can measure how much
 * work is duplicated by competing coordinators (e.g. the original coordinator, recovery, and backlog executors).
 * An execution is "completed" once its result has been computed and it proceeds to persist.
 * <p>
 * We also measure the chains of transactions that key transactions wait on to execute. For each transaction we record
 * when we first began to execute it (anywhere) and when its execution first completed. Its blocker is then the
 * dependency whose execution completed last, after we began and before we completed executing the transaction, i.e.
 * the last dependency it plausibly waited for. Its wait depth is the number of links of this chain of blockers that
 * completed after it began executing, i.e. the number of transactions it waited on in sequence; its cross-DC
 * wait depth counts only those links whose original coordinators are in different DCs. These are the chains that
 * backlog execution (see {@link accord.local.cfk.ExecuteTxnBacklog}) aims to accelerate: without it, each
 * cross-DC link costs at least a one-way trip between DCs, as the Apply of one link must reach the replicas
 * serving the coordinator of the next.
 */
public class ExecutionStats
{
    private static final TxnId[] NO_TXNIDS = new TxnId[0];

    private final Map<ExecutePath, AtomicLong> attemptsByPath = new EnumMap<>(ExecutePath.class);
    private final AtomicLong attemptsWithoutPath = new AtomicLong();
    private final ConcurrentHashMap<TxnId, AtomicInteger> executed = new ConcurrentHashMap<>();

    private final @Nullable LongSupplier nowMillis;
    private volatile BiPredicate<Node.Id, Node.Id> sameDc = (a, b) -> true;

    private static class Started
    {
        final TxnId[] deps;
        final long startedAtMillis;
        long executedAtMillis = -1;

        Started(TxnId[] deps, long startedAtMillis)
        {
            this.deps = deps;
            this.startedAtMillis = startedAtMillis;
        }
    }

    // the dependencies of each transaction, and when we first began and completed executing it
    private final ConcurrentHashMap<TxnId, Started> started = new ConcurrentHashMap<>();

    public ExecutionStats()
    {
        this(null);
    }

    public ExecutionStats(@Nullable LongSupplier nowMillis)
    {
        this.nowMillis = nowMillis;
        for (ExecutePath path : ExecutePath.values())
            attemptsByPath.put(path, new AtomicLong());
    }

    public void setSameDc(BiPredicate<Node.Id, Node.Id> sameDc)
    {
        this.sameDc = sameDc;
    }

    void onExecuting(TxnId txnId, Deps deps, ExecutePath path)
    {
        if (path == null) attemptsWithoutPath.incrementAndGet();
        else attemptsByPath.get(path).incrementAndGet();

        if (nowMillis == null || deps == null || started.containsKey(txnId))
            return;

        started.put(txnId, new Started(deps.txnIds().toArray(NO_TXNIDS), nowMillis.getAsLong()));
    }

    void onExecuted(TxnId txnId)
    {
        AtomicInteger count = executed.computeIfAbsent(txnId, ignore -> new AtomicInteger());
        if (count.incrementAndGet() == 1 && nowMillis != null)
        {
            Started s = started.get(txnId);
            if (s != null)
                s.executedAtMillis = nowMillis.getAsLong();
        }
    }

    private static boolean isMeasured(TxnId txnId)
    {
        return txnId.is(Key) && (txnId.is(Read) || txnId.is(Write));
    }

    private @Nullable TxnId blocker(Started s)
    {
        TxnId blocker = null;
        long blockerExecutedAt = s.startedAtMillis;
        for (TxnId dep : s.deps)
        {
            Started d = started.get(dep);
            if (d == null || d.executedAtMillis < 0)
                continue;
            if (d.executedAtMillis > blockerExecutedAt && d.executedAtMillis <= s.executedAtMillis)
            {
                blocker = dep;
                blockerExecutedAt = d.executedAtMillis;
            }
        }
        return blocker;
    }

    // [0]: wait depth; [1]: cross-DC wait depth
    private int[] waitDepth(TxnId txnId, Started s)
    {
        int[] result = new int[2];
        if (s.executedAtMillis < 0)
            return result;

        TxnId next = txnId;
        Started cur = s;
        // guard against cycles amongst transactions completing at the same (logical) millisecond
        Set<TxnId> visited = new HashSet<>();
        visited.add(txnId);
        while (true)
        {
            TxnId blocker = blocker(cur);
            if (blocker == null || !visited.add(blocker))
                break;
            Started b = started.get(blocker);
            if (b.executedAtMillis <= s.startedAtMillis)
                break;
            ++result[0];
            if (!sameDc.test(next.node, blocker.node))
                ++result[1];
            next = blocker;
            cur = b;
        }
        return result;
    }

    private static String distribution(long[] values)
    {
        if (values.length == 0)
            return "{}";
        Arrays.sort(values);
        long sum = 0;
        for (long v : values) sum += v;
        return String.format("{mean=%.2f p50=%d p90=%d p99=%d max=%d}", (double)sum / values.length,
                             values[values.length / 2], values[(int)(values.length * 0.9)],
                             values[(int)(values.length * 0.99)], values[values.length - 1]);
    }

    /**
     * @return a summary of the chains that key transactions waited on (see class javadoc)
     */
    public String chains()
    {
        int count = 0;
        long[] depths = new long[started.size()], crossDcDepths = new long[started.size()], waits = new long[started.size()];
        int waitCount = 0;
        for (Map.Entry<TxnId, Started> e : started.entrySet())
        {
            if (!isMeasured(e.getKey()))
                continue;
            int[] d = waitDepth(e.getKey(), e.getValue());
            depths[count] = d[0];
            crossDcDepths[count] = d[1];
            ++count;
            Started s = e.getValue();
            if (s.startedAtMillis >= 0 && s.executedAtMillis >= 0)
                waits[waitCount++] = s.executedAtMillis - s.startedAtMillis;
        }
        return "wait depth: " + distribution(Arrays.copyOf(depths, count))
               + ", cross-DC wait depth: " + distribution(Arrays.copyOf(crossDcDepths, count))
               + ", execution latency (ms): " + distribution(Arrays.copyOf(waits, waitCount));
    }

    @Override
    public String toString()
    {
        long total = 0, duplicated = 0, extra = 0;
        for (AtomicInteger count : executed.values())
        {
            int c = count.get();
            total += c;
            if (c > 1)
            {
                ++duplicated;
                extra += c - 1;
            }
        }
        StringBuilder attempts = new StringBuilder();
        for (Map.Entry<ExecutePath, AtomicLong> e : attemptsByPath.entrySet())
        {
            if (e.getValue().get() > 0)
                attempts.append(e.getKey()).append('=').append(e.getValue().get()).append(' ');
        }
        if (attemptsWithoutPath.get() > 0)
            attempts.append("other=").append(attemptsWithoutPath.get());
        return "attempts: {" + attempts.toString().trim() + "}, executed txns: " + executed.size()
               + ", executions: " + total + ", txns executed more than once: " + duplicated + ", duplicate executions: " + extra;
    }
}
