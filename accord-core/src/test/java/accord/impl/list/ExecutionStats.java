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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
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
import accord.primitives.Ballot;
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
 * <p>
 * Finally, we break down client latency into phases (see {@link #phases()}), and may dump each transaction's
 * timeline (see {@link #dump}).
 */
public class ExecutionStats
{
    private static final TxnId[] NO_TXNIDS = new TxnId[0];

    private final Map<ExecutePath, AtomicLong> attemptsByPath = new EnumMap<>(ExecutePath.class);
    private final AtomicLong attemptsWithoutPath = new AtomicLong();
    private final ConcurrentHashMap<TxnId, AtomicInteger> executed = new ConcurrentHashMap<>();

    private final @Nullable LongSupplier nowMillis;
    private volatile BiPredicate<Node.Id, Node.Id> sameDc = (a, b) -> true;

    /**
     * The timeline of a transaction; all times are logical millis, or -1 if unknown
     */
    private static class Timeline
    {
        long clientStartAt = -1, submittedAt = -1, coordinatorStartedAt = -1, clientReplyAt = -1;
        @Nullable ExecutePath coordinatorPath;
        String keys;

        // the first execution attempt anywhere
        TxnId[] deps;
        long startedAt = -1;
        @Nullable ExecutePath startedPath;

        // the first completed execution
        long executedAt = -1;
        Node.Id executedBy;
        boolean executedByRecovery;
        // every node that completed an execution (with repetition)
        final List<Node.Id> executors = new ArrayList<>(2);
    }

    private final ConcurrentHashMap<TxnId, Timeline> timelines = new ConcurrentHashMap<>();

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

    private Timeline timeline(TxnId txnId)
    {
        return timelines.computeIfAbsent(txnId, ignore -> new Timeline());
    }

    private boolean measuring()
    {
        return nowMillis != null;
    }

    void onSubmitted(TxnId txnId)
    {
        if (measuring())
            timeline(txnId).submittedAt = nowMillis.getAsLong();
    }

    /**
     * The client received a reply for {@code txnId}, which it submitted at {@code clientStartAt}
     */
    public void onClientReply(TxnId txnId, long clientStartAt, String keys)
    {
        if (!measuring())
            return;
        Timeline t = timeline(txnId);
        t.clientStartAt = clientStartAt;
        t.clientReplyAt = nowMillis.getAsLong();
        t.keys = keys;
    }

    void onExecuting(Node.Id self, TxnId txnId, Deps deps, ExecutePath path)
    {
        if (path == null) attemptsWithoutPath.incrementAndGet();
        else attemptsByPath.get(path).incrementAndGet();

        if (!measuring())
            return;

        long now = nowMillis.getAsLong();
        Timeline t = timeline(txnId);
        if (t.startedAt < 0 && deps != null)
        {
            t.deps = deps.txnIds().toArray(NO_TXNIDS);
            t.startedAt = now;
            t.startedPath = path;
        }
        if (t.coordinatorStartedAt < 0 && txnId.node.equals(self) && path != ExecutePath.RECOVER)
        {
            t.coordinatorStartedAt = now;
            t.coordinatorPath = path;
        }
    }

    void onExecuted(Node.Id self, TxnId txnId, Ballot ballot)
    {
        AtomicInteger count = executed.computeIfAbsent(txnId, ignore -> new AtomicInteger());
        if (measuring() && self != null)
            timeline(txnId).executors.add(self);
        if (count.incrementAndGet() == 1 && measuring())
        {
            Timeline t = timeline(txnId);
            t.executedAt = nowMillis.getAsLong();
            t.executedBy = self;
            t.executedByRecovery = ballot != null && !ballot.equals(Ballot.ZERO);
        }
    }

    private static boolean isMeasured(TxnId txnId)
    {
        return txnId.is(Key) && (txnId.is(Read) || txnId.is(Write));
    }

    private @Nullable TxnId blocker(Timeline t)
    {
        if (t.deps == null || t.executedAt < 0)
            return null;

        TxnId blocker = null;
        long blockerExecutedAt = t.startedAt;
        for (TxnId dep : t.deps)
        {
            Timeline d = timelines.get(dep);
            if (d == null || d.executedAt < 0)
                continue;
            if (d.executedAt > blockerExecutedAt && d.executedAt <= t.executedAt)
            {
                blocker = dep;
                blockerExecutedAt = d.executedAt;
            }
        }
        return blocker;
    }

    // [0]: wait depth; [1]: cross-DC wait depth
    private int[] waitDepth(TxnId txnId, Timeline t)
    {
        int[] result = new int[2];
        TxnId next = txnId;
        Timeline cur = t;
        // guard against cycles amongst transactions completing at the same (logical) millisecond
        Set<TxnId> visited = new HashSet<>();
        visited.add(txnId);
        while (true)
        {
            TxnId blocker = blocker(cur);
            if (blocker == null || !visited.add(blocker))
                break;
            Timeline b = timelines.get(blocker);
            if (b.executedAt <= t.startedAt)
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

    private static class Values
    {
        long[] values = new long[16];
        int size;
        void add(long v) { if (size == values.length) values = Arrays.copyOf(values, size * 2); values[size++] = v; }
        long[] get() { return Arrays.copyOf(values, size); }
        double mean() { long sum = 0; for (int i = 0 ; i < size ; ++i) sum += values[i]; return size == 0 ? 0 : (double)sum / size; }
    }

    /**
     * @return a summary of the chains that key transactions waited on (see class javadoc)
     */
    public String chains()
    {
        Values depths = new Values(), crossDcDepths = new Values(), waits = new Values();
        for (Map.Entry<TxnId, Timeline> e : timelines.entrySet())
        {
            Timeline t = e.getValue();
            if (!isMeasured(e.getKey()) || t.startedAt < 0 || t.executedAt < 0)
                continue;
            int[] d = waitDepth(e.getKey(), t);
            depths.add(d[0]);
            crossDcDepths.add(d[1]);
            waits.add(t.executedAt - t.startedAt);
        }
        return "wait depth: " + distribution(depths.get())
               + ", cross-DC wait depth: " + distribution(crossDcDepths.get())
               + ", execution latency (ms): " + distribution(waits.get());
    }

    /**
     * @return the mean duration of each phase of the client latency of key transactions:
     * <ul>
     *   <li>submit: client to coordinator</li>
     *   <li>coordinate: until the original coordinator begins to execute (by its path)</li>
     *   <li>execute: until the first execution completes anywhere (by who completed it)</li>
     *   <li>reply: until the client receives its reply</li>
     * </ul>
     */
    public String phases()
    {
        Values submit = new Values(), reply = new Values(), total = new Values();
        Map<ExecutePath, Values> coordinate = new EnumMap<>(ExecutePath.class);
        Values[] execute = new Values[] { new Values(), new Values(), new Values() };
        String[] executedBy = new String[] { "coordinator", "other", "recovery" };
        for (Map.Entry<TxnId, Timeline> e : timelines.entrySet())
        {
            TxnId txnId = e.getKey();
            Timeline t = e.getValue();
            if (!isMeasured(txnId) || t.clientReplyAt < 0 || t.submittedAt < 0 || t.coordinatorStartedAt < 0 || t.executedAt < 0)
                continue;
            total.add(t.clientReplyAt - t.clientStartAt);
            submit.add(t.submittedAt - t.clientStartAt);
            coordinate.computeIfAbsent(t.coordinatorPath, ignore -> new Values()).add(t.coordinatorStartedAt - t.submittedAt);
            int by = t.executedByRecovery ? 2 : t.executedBy.equals(txnId.node) ? 0 : 1;
            execute[by].add(t.executedAt - t.coordinatorStartedAt);
            reply.add(t.clientReplyAt - t.executedAt);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("n=%d total=%.0f submit=%.0f coordinate={", total.size, total.mean(), submit.mean()));
        for (Map.Entry<ExecutePath, Values> e : coordinate.entrySet())
            sb.append(String.format("%s:%.0f(n=%d) ", e.getKey(), e.getValue().mean(), e.getValue().size));
        sb.setLength(sb.length() - 1);
        sb.append("} execute={");
        for (int i = 0 ; i < execute.length ; ++i)
            sb.append(String.format("%s:%.0f(n=%d)%s", executedBy[i], execute[i].mean(), execute[i].size, i + 1 < execute.length ? " " : ""));
        sb.append(String.format("} reply=%.0f", reply.mean()));
        return sb.toString();
    }

    /**
     * @return a breakdown of duplicate executions of key transactions: those by the same node, by another node in the
     * same DC as an earlier execution, and by a node in a DC that had not yet executed the transaction
     */
    public String duplicates()
    {
        long sameNode = 0, sameDcDup = 0, otherDc = 0, coordinatorDup = 0;
        for (Map.Entry<TxnId, Timeline> e : timelines.entrySet())
        {
            if (!isMeasured(e.getKey()))
                continue;
            List<Node.Id> executors = e.getValue().executors;
            for (int i = 1 ; i < executors.size() ; ++i)
            {
                Node.Id id = executors.get(i);
                boolean node = false, dc = false;
                for (int j = 0 ; j < i ; ++j)
                {
                    node |= executors.get(j).equals(id);
                    dc |= sameDc.test(executors.get(j), id);
                }
                if (node) ++sameNode;
                else if (dc) ++sameDcDup;
                else ++otherDc;
            }
            if (executors.size() > 1 && executors.contains(e.getKey().node))
                ++coordinatorDup;
        }
        return "same node=" + sameNode + ", same DC=" + sameDcDup + ", new DC=" + otherDc + ", txns duplicated involving their coordinator=" + coordinatorDup;
    }

    /**
     * Write each key transaction's timeline to {@code file} as CSV, ordered by TxnId
     */
    public void dump(Path file) throws IOException
    {
        List<TxnId> txnIds = new ArrayList<>();
        for (TxnId txnId : timelines.keySet())
        {
            if (isMeasured(txnId))
                txnIds.add(txnId);
        }
        txnIds.sort(TxnId::compareTo);
        Files.createDirectories(file.getParent());
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file)))
        {
            out.println("txnId,coordinator,keys,clientStart,submitted,coordinatorStarted,coordinatorPath,firstStarted,firstStartedPath,executed,executedBy,byRecovery,clientReply,blocker,blockerCoordinator,blockerExecuted,waitDepth,crossDcWaitDepth,deps");
            for (TxnId txnId : txnIds)
            {
                Timeline t = timelines.get(txnId);
                TxnId blocker = blocker(t);
                Timeline b = blocker == null ? null : timelines.get(blocker);
                int[] depth = t.startedAt < 0 || t.executedAt < 0 ? new int[2] : waitDepth(txnId, t);
                out.println(String.join(",",
                    txnId.toString().replace(',', ' '), String.valueOf(txnId.node.id), '"' + (t.keys == null ? "" : t.keys) + '"',
                    String.valueOf(t.clientStartAt), String.valueOf(t.submittedAt), String.valueOf(t.coordinatorStartedAt), String.valueOf(t.coordinatorPath),
                    String.valueOf(t.startedAt), String.valueOf(t.startedPath),
                    String.valueOf(t.executedAt), t.executedBy == null ? "" : String.valueOf(t.executedBy.id), String.valueOf(t.executedByRecovery),
                    String.valueOf(t.clientReplyAt),
                    blocker == null ? "" : blocker.toString().replace(',', ' '), blocker == null ? "" : String.valueOf(blocker.node.id), b == null ? "" : String.valueOf(b.executedAt),
                    String.valueOf(depth[0]), String.valueOf(depth[1]), t.deps == null ? "" : Arrays.stream(t.deps).map(d -> d.toString().replace(',', ' ')).collect(java.util.stream.Collectors.joining(";"))));
            }
        }
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
