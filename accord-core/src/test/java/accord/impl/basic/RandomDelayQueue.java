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

package accord.impl.basic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import accord.burn.random.FrequentLargeRange;
import accord.impl.basic.DelayedCommandStores.DelayedCommandStore.DelayedTask;
import accord.utils.Invariants;
import accord.utils.RandomSource;

import static accord.impl.basic.RecurringPendingRunnable.isRecurring;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;

public class RandomDelayQueue implements PendingQueue
{
    public static class Factory implements Supplier<RandomDelayQueue>
    {
        final RandomSource seeds;

        public Factory(RandomSource seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public RandomDelayQueue get()
        {
            return new RandomDelayQueue(seeds.fork());
        }
    }

    static class Item implements Comparable<Item>
    {
        final long time;
        final int seq;
        final Pending item;

        Item(long time, int seq, Pending item)
        {
            this.time = time;
            this.seq = seq;
            this.item = item;
        }

        @Override
        public int compareTo(Item that)
        {
            int c = Long.compare(this.time, that.time);
            if (c == 0) c = Integer.compare(this.seq, that.seq);
            return c;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o instanceof Pending) return item.equals(o);
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item1 = (Item) o;
            return time == item1.time && seq == item1.seq && item.equals(item1.item);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "@" + time + "/" + seq + ":" + item;
        }
    }

    final PriorityQueue<Item> queue = new PriorityQueue<>();
    private final LongSupplier jitterMillis;
    long now;
    int seq;
    final IdentityHashMap<Pending, Boolean> preregistered = new IdentityHashMap<>();
    int recurring;

    public RandomDelayQueue(RandomSource random)
    {
        this.jitterMillis = FrequentLargeRange.builder(random)
                                              .small(0, 50, TimeUnit.MICROSECONDS)
                                              .large(50, TimeUnit.MICROSECONDS, 5, TimeUnit.MILLISECONDS)
                                              .build()
                                              .mapAsLong(TimeUnit.NANOSECONDS::toMillis)
                                              .asLongSupplier(random);
    }

    @Override
    public void add(Pending item)
    {
        add(item, 0, TimeUnit.NANOSECONDS);
    }

    @Override
    public void addNoDelay(Pending item)
    {
        if (null == preregistered.remove(item) && isRecurring(item))
            ++recurring;
        queue.add(new Item(now, seq++, item));
    }

    @Override
    public void add(Pending item, long delay, TimeUnit units)
    {
        if (delay < 0)
            throw illegalArgument("Delay must be positive or 0, but given " + delay);
        if (null == preregistered.remove(item) && isRecurring(item))
            ++recurring;
        queue.add(new Item(now + units.toMillis(delay) + jitterMillis.getAsLong(), seq++, item));
    }

    @Override
    public void preregister(Pending item)
    {
        if (null != preregistered.put(item, true))
            throw new IllegalStateException();
        if (isRecurring(item))
            ++recurring;
    }

    @Override
    public boolean remove(Pending item)
    {
        if (!queue.removeIf(i -> i.item == item) && preregistered.remove(item) == null)
            return false;

        if (isRecurring(item))
            --recurring;
        return true;
    }

    @Override
    public Pending poll()
    {
        Item item = queue.poll();
        if (item == null)
            return null;

        if (isRecurring(item.item))
            --recurring;

        now = item.time;
        return item.item;
    }

    @Override
    public List<Pending> drain(Predicate<Pending> toDrain)
    {
        List<Item> items = new ArrayList<>(queue);
        queue.clear();
        List<Pending> ret = new ArrayList<>();
        for (Item item : items)
        {
            if (toDrain.test(item.item))
            {
                if (isRecurring(item.item))
                    --recurring;
                ret.add(item.item);
            }
            else
            {
                queue.add(item);
            }
        }
        return ret;
    }

    @Override
    public int size()
    {
        return queue.size();
    }

    @Override
    public long nowInMillis()
    {
        return now;
    }

    @Override
    public boolean hasNonRecurring()
    {
        return recurring != queue.size() + preregistered.size();
    }

    @Override
    public Iterator<Pending> iterator()
    {
        return queue.stream().map(i -> i.item).iterator();
    }

    public abstract static class MonitoringQueue extends RandomDelayQueue
    {
        public MonitoringQueue(RandomSource random)
        {
            super(random);
        }

        protected abstract void added(Pending pending, long delay);

        @Override
        public void add(Pending item, long delay, TimeUnit units)
        {
            added(item, units.toNanos(delay));
            super.add(item, delay, units);
        }
    }

    public static class ReconcilingQueueFactory
    {
        final Pending[] pendings = new Pending[2];
        final long[] delays = new long[2];
        int waiting = 0;

        class ReconcilingQueue extends MonitoringQueue
        {
            final int id;

            public ReconcilingQueue(RandomSource random, int id)
            {
                super(random);
                this.id = id;
            }

            @Override
            protected void added(Pending pending, long delay)
            {
                reconcile(pending, delay, id);
            }
        }

        synchronized void reconcile(Pending item, long delay, int id)
        {
            pendings[id] = item;
            delays[id] = delay;
            switch (++waiting)
            {
                default: throw new IllegalStateException();
                case 1:
                {
                    try
                    {
                        wait();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    return;
                }
                case 2:
                {
                    if (delays[0] != delays[1])
                        throw illegalState("%d != %d", delays[0], delays[1]);
                    if (pendings[0].getClass() != pendings[1].getClass())
                        throw illegalState("%s != %s", pendings[0], pendings[1]);
                    if (pendings[0] instanceof DelayedTask)
                    {
                        DelayedTask a = (DelayedTask) pendings[0];
                        DelayedTask b = (DelayedTask) pendings[1];
                        if (a.callable().getClass() != b.callable().getClass())
                            throw illegalState("%s != %s", a.callable(), b.callable());

                    }
                    else if (pendings[0] instanceof Packet)
                    {
                        Packet a = (Packet) pendings[0];
                        Packet b = (Packet) pendings[1];
                        if (a.requestId != b.requestId || a.replyId != b.replyId || a.src.id != b.src.id || a.dst.id != b.dst.id || a.message.type() != b.message.type())
                            throw illegalState("%s != %s", a, b);
                    }
                    waiting = 0;
                    notifyAll();
                }
            }
        }

        public ReconcilingQueue get(boolean first, RandomSource random)
        {
            return new ReconcilingQueue(random, first ? 0 : 1);
        }

        public ReconcilingQueueFactory()
        {
        }
    }

    public static class RecordingQueue extends MonitoringQueue
    {
        final DataOutputStream out;

        synchronized public void added(Pending item, long delay)
        {
            try
            {
                out.writeLong(delay);
                out.writeUTF(print(item));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public RecordingQueue(DataOutputStream out, RandomSource random)
        {
            super(random);
            this.out = out;
        }
    }

    public static class ReplayQueue extends MonitoringQueue
    {
        final DataInputStream in;

        synchronized public void added(Pending item, long delay)
        {
            try
            {
                long testDelay = in.readLong();
                String testString = in.readUTF();
                Invariants.require(testDelay == delay);
                Invariants.require(testString.equals(print(item)));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public ReplayQueue(DataInputStream in, RandomSource random)
        {
            super(random);
            this.in = in;
        }
    }


    private static String print(Pending pending)
    {
        if (pending instanceof Packet)
            return pending.toString();
        if (pending instanceof DelayedTask)
            return ((DelayedTask<?>) pending).owner().toString();
        if (pending instanceof RecurringPendingRunnable && ((RecurringPendingRunnable) pending).delay instanceof Cluster.ConstantLongSupplier)
            return ((RecurringPendingRunnable) pending).delay.toString();
        return "";
    }
}
