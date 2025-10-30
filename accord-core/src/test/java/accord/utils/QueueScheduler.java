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

package accord.utils;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import accord.api.Scheduler;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.RecurringPendingRunnable;

public class QueueScheduler implements Scheduler
{
    final int source;
    final PendingQueue pending;

    public QueueScheduler(int source, PendingQueue pending)
    {
        this.source = source;
        this.pending = pending;
    }

    @Override
    public void now(Runnable run)
    {
        run.run();
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        return recurring(run, constant(delay), units);
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(source, null, run, constant(delay), units, false);
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public Scheduled selfRecurring(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(source, null, run, constant(delay), units, true);
        pending.add(result, delay, units);
        return result;
    }

    public Scheduled recurring(Runnable run, LongSupplier delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(source, pending, run, delay, units, true);
        pending.add(result, delay.getAsLong(), units);
        return result;
    }

    public static class ConstantLongSupplier implements LongSupplier
    {
        final long v;

        public ConstantLongSupplier(long v)
        {
            this.v = v;
        }

        @Override
        public long getAsLong()
        {
            return v;
        }

        @Override
        public String toString()
        {
            return "" + v;
        }
    }

    private LongSupplier constant(long delay)
    {
        return new ConstantLongSupplier(delay);
    }
}
