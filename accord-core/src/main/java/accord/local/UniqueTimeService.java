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

package accord.local;

import java.util.concurrent.atomic.AtomicLong;

import accord.utils.Invariants;

public interface UniqueTimeService
{
    /**
     * Expected to provide a value > all prior values
     */
    default long uniqueNow() { return uniqueNow(0); }

    /**
     * Expected to provide a value > all prior values, and greater than the parameter
     */
    long uniqueNow(long greaterThan);

    default long uniqueStale(long greaterThan) { return uniqueNow(greaterThan); }

    class AtomicUniqueTime extends AtomicLong implements UniqueTimeService
    {
        final TimeService time;

        public AtomicUniqueTime(TimeService time)
        {
            this.time = time;
        }

        @Override
        public long uniqueNow(long greaterThan)
        {
            long now = time.now();
            return accumulateAndGet(Math.max(now - 1, greaterThan), (a, b) -> Math.max(a, b) + 1);
        }
    }

    class AtomicUniqueTimeWithStaleReservation extends AtomicLong implements UniqueTimeService
    {
        // 1 in every ~1k hlc are reserved for stale hlc by default
        private static final long RESERVATION_MASK = 0x3ff;
        final TimeService time;
        final AtomicLong lastStale = new AtomicLong();

        public AtomicUniqueTimeWithStaleReservation(TimeService time)
        {
            this.time = time;
        }

        @Override
        public long uniqueNow(long greaterThan)
        {
            long now = time.now();
            long min = Math.max(now, greaterThan + 1);
            while (true)
            {
                long cur = get();
                if (cur >= min)
                    break;

                long next = Math.max(min, cur + 1);
                if (!isSafeNow(next))
                    ++next;

                if (compareAndSet(cur, next))
                    return next;
            }

            while (true)
            {
                long result = incrementAndGet();
                if (isSafeNow(result))
                    return result;
            }
        }

        @Override
        public long uniqueStale(long greaterThan)
        {
            long now = time.now();
            long min = greaterThan + 1;
            while (true)
            {
                long cur = lastStale.get();
                if (cur >= now)
                    return uniqueNow(greaterThan);

                long next = ensureStale(Math.max(min, cur + 1));
                if (lastStale.compareAndSet(cur, next))
                    return next;
            }
        }

        protected long ensureStale(long stale)
        {
            if ((stale & RESERVATION_MASK) == 0)
                return stale;

            stale += 1 + RESERVATION_MASK - (stale & RESERVATION_MASK);
            Invariants.require((stale & RESERVATION_MASK) == 0);
            return stale;
        }

        protected boolean isSafeNow(long now)
        {
            return (now & RESERVATION_MASK) != 0;
        }
    }
}
