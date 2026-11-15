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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import accord.coordinate.ExecutePath;
import accord.primitives.TxnId;

/**
 * Cluster-wide counts of coordinated execution attempts and completed executions, so that we can measure how much
 * work is duplicated by competing coordinators (e.g. the original coordinator, recovery, and backlog executors).
 * An execution is "completed" once its result has been computed and it proceeds to persist.
 */
public class ExecutionStats
{
    private final Map<ExecutePath, AtomicLong> attemptsByPath = new EnumMap<>(ExecutePath.class);
    private final AtomicLong attemptsWithoutPath = new AtomicLong();
    private final ConcurrentHashMap<TxnId, AtomicInteger> executed = new ConcurrentHashMap<>();

    public ExecutionStats()
    {
        for (ExecutePath path : ExecutePath.values())
            attemptsByPath.put(path, new AtomicLong());
    }

    void onExecuting(ExecutePath path)
    {
        if (path == null) attemptsWithoutPath.incrementAndGet();
        else attemptsByPath.get(path).incrementAndGet();
    }

    void onExecuted(TxnId txnId)
    {
        executed.computeIfAbsent(txnId, ignore -> new AtomicInteger()).incrementAndGet();
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
