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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import accord.api.Scheduler;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

// TODO (expected): move this to a test package, along with InMemoryX
public class Snapshotter<S>
{
    public static class SnapshotAborted extends RuntimeException { SnapshotAborted() { super("Snapshot aborted due to earlier snapshot being restored"); } }

    private static final class PendingSnapshot
    {
        // we don't restart bootstraps after clearing journal state and replaying, so must finish snapshots
        final boolean runBeforeRestart;
        final long delay;
        final Consumer<Runnable> onReady;

        private PendingSnapshot(boolean runBeforeRestart, long delay, Consumer<Runnable> onReady)
        {
            this.runBeforeRestart = runBeforeRestart;
            this.delay = delay;
            this.onReady = onReady;
        }
    }

    final RandomSource random;
    final Scheduler scheduler;
    private S snapshot;
    private Scheduler.Scheduled scheduled;
    private final Deque<PendingSnapshot> pendingSnapshots = new ArrayDeque<>();
    private long pendingDelay = 0;

    protected Snapshotter(Scheduler scheduler, RandomSource random)
    {
        this.random = random;
        this.scheduler = scheduler;
    }

    protected AsyncResult<Void> snapshot(boolean runBeforeRestart, S snapshot)
    {
        return snapshot(runBeforeRestart, AsyncResults.success(snapshot));
    }

    protected AsyncResult<Void> snapshot(boolean runBeforeRestart, AsyncResult<S> snapshot)
    {
        AsyncResult.Settable<Void> result = new AsyncResults.SettableResult<>();
        long delay = Math.max(1, random.nextBiasedLong(100, 1000, 5000) - pendingDelay);
        pendingDelay += delay;

        if (scheduled != null && runBeforeRestart && !pendingSnapshots.stream().anyMatch(p -> p.runBeforeRestart))
        {
            scheduled.cancel();
            scheduled = null;
        }

        pendingSnapshots.add(new PendingSnapshot(runBeforeRestart, delay, (continuation) -> {
            if (continuation != null)
            {
                snapshot.invoke((success, fail) -> {
                    if (fail != null) result.setFailure(fail);
                    else
                    {
                        this.snapshot = success;
                        result.setSuccess(null);
                    }
                    continuation.run();
                });
            }
            else
            {
                result.setFailure(new SnapshotAborted());
            }
        }));

        if (scheduled == null)
            scheduleRunSnapshot();
        return result;
    }

    private void scheduleRunSnapshot()
    {
        Invariants.require(!pendingSnapshots.isEmpty());
        // schedule as recurring so that we don't run them
        Runnable run = () -> {
            scheduled = null;
            if (pendingSnapshots.isEmpty())
                return;

            PendingSnapshot pendingSnapshot = pendingSnapshots.pollFirst();
            pendingDelay -= pendingSnapshot.delay;
            pendingSnapshot.onReady.accept(() -> {
                if (!pendingSnapshots.isEmpty())
                    scheduleRunSnapshot();
            });
        };

        if (pendingSnapshots.stream().anyMatch(p -> p.runBeforeRestart)) scheduled = scheduler.once(run, pendingSnapshots.peekFirst().delay, TimeUnit.MILLISECONDS);
        else scheduled = scheduler.selfRecurring(run, pendingSnapshots.peekFirst().delay, TimeUnit.MILLISECONDS);
    }

    protected void restore(Consumer<S> restore)
    {
        if (snapshot != null)
            restore.accept(snapshot);

        while (!pendingSnapshots.isEmpty())
            pendingSnapshots.pollFirst().onReady.accept(null);
    }
}
