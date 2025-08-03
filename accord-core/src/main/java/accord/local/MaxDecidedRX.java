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

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.primitives.Range;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import static accord.primitives.TxnId.maxIfNull;
import static accord.primitives.TxnId.noneIfNull;

import accord.primitives.Ranges;
import accord.utils.ReducingRangeMap;

public class MaxDecidedRX extends ReducingRangeMap<TxnId>
{
    public static final MaxDecidedRX EMPTY = new MaxDecidedRX();

    private MaxDecidedRX()
    {
        super();
    }

    private MaxDecidedRX(boolean inclusiveEnds, RoutingKey[] starts, TxnId[] values)
    {
        super(inclusiveEnds, starts, values);
    }

    TxnId min(Routables<?> keysOrRanges)
    {
        return noneIfNull(foldlWithDefault(keysOrRanges, TxnId::nonNullOrMin, null, TxnId.NONE));
    }

    TxnId max(Routables<?> keysOrRanges)
    {
        return maxIfNull(foldlWithDefault(keysOrRanges, TxnId::nonNullOrMax, null, TxnId.MAX));
    }

    TxnId min(Range range)
    {
        return noneIfNull(foldlWithDefault(Ranges.of(range), TxnId::nonNullOrMin, null, TxnId.NONE));
    }

    TxnId max(Range range)
    {
        return maxIfNull(foldlWithDefault(Ranges.of(range), TxnId::nonNullOrMax, null, TxnId.MAX));
    }

    public @Nullable TxnId minDecidedDependencyId(Unseekables<?> keysOrRanges, TxnId txnId)
    {
        Invariants.require(txnId.isSyncPoint());
        // first check max, as if this is later we don't know that we can safely filter
        TxnId maxDecidedId = max(keysOrRanges);
        if (maxDecidedId.compareTo(txnId) < 0)
            return min(keysOrRanges);
        return null;
    }

    @VisibleForImplementation
    public @Nullable TxnId minDecidedDependencyId(Unseekable keyOrRange, TxnId txnId)
    {
        Invariants.require(txnId.isSyncPoint());
        if (keyOrRange.domain() == Domain.Key)
        {
            TxnId minMaxDecidedId = get((RoutingKey) keyOrRange);
            if (minMaxDecidedId.compareTo(txnId) < 0)
                return minMaxDecidedId;
        }
        else
        {
            // first check max, as if this is later we don't know that we can safely filter
            Range range = (Range) keyOrRange;
            TxnId maxDecidedId = max(range);
            if (maxDecidedId.compareTo(txnId) < 0)
                return min(range);
        }
        return null;
    }

    @VisibleForImplementation
    public static @Nullable TxnId minDecidedDependencyId(MaxDecidedRX maxDecidedRX, Unseekables<?> keysOrRanges, TxnId txnId)
    {
        return maxDecidedRX == null ? null : maxDecidedRX.minDecidedDependencyId(keysOrRanges, txnId);
    }

    public TxnId get(RoutingKey key)
    {
        return TxnId.noneIfNull(super.get(key));
    }

    public MaxDecidedRX update(Unseekables<?> keysOrRanges, TxnId maxId)
    {
        if (keysOrRanges.isEmpty())
            return this;
        return merge(this, create(keysOrRanges, maxId, Builder::new), TxnId::max, Builder::new);
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, TxnId, MaxDecidedRX>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected MaxDecidedRX buildInternal()
        {
            return new MaxDecidedRX(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new TxnId[0]));
        }
    }

}
