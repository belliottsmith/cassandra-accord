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

import java.util.Objects;

import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

public class RejectBefore extends ReducingRangeMap<Timestamp>
{
    public static class SerializerSupport
    {
        public static RejectBefore create(boolean inclusiveEnds, RoutingKey[] ends, Timestamp[] values)
        {
            return new RejectBefore(inclusiveEnds, ends, values);
        }
    }

    public static RejectBefore EMPTY = new RejectBefore();

    private RejectBefore()
    {
    }

    RejectBefore(boolean inclusiveEnds, RoutingKey[] starts, Timestamp[] values)
    {
        super(inclusiveEnds, starts, values);
    }

    public boolean rejects(TxnId txnId, Routables<?> keysOrRanges)
    {
        return null == foldl(keysOrRanges, (rejectIfBefore, test) -> rejectIfBefore.epoch() > test.epoch() || rejectIfBefore.hlc() > test.hlc() ? null : test, txnId, Objects::isNull);
    }

    public static RejectBefore add(RejectBefore existing, Ranges ranges, Timestamp timestamp)
    {
        RejectBefore add = create(ranges, timestamp);
        return merge(existing, add);
    }

    public static RejectBefore create(Ranges ranges, Timestamp value)
    {
        if (value == null)
            throw new IllegalArgumentException("value is null");

        if (ranges.isEmpty())
            return RejectBefore.EMPTY;

        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (Range range : ranges)
        {
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
        }
        return builder.build();
    }

    public static RejectBefore merge(RejectBefore historyLeft, RejectBefore historyRight)
    {
        return ReducingIntervalMap.merge(historyLeft, historyRight, Timestamp::mergeMax, RejectBefore.Builder::new);
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, Timestamp, RejectBefore>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected RejectBefore buildInternal()
        {
            return new RejectBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Timestamp[0]));
        }
    }
}
