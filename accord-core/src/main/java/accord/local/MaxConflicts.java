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

import accord.api.RoutingKey;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.BTreeReducingRangeMap;
import accord.utils.btree.ReducingBTree;

// TODO (expected): track read/write conflicts separately
public class MaxConflicts extends BTreeReducingRangeMap<MaxConflicts.Entry>
{
    public static class Entry extends ReducingBTree.Entry<Entry>
    {
        public final Timestamp any, write;
        public Entry(RoutingKey start, RoutingKey end, Timestamp any, Timestamp write)
        {
            super(start, end);
            this.any = any;
            this.write = write;
        }

        public Entry(Timestamp any, Timestamp write)
        {
            super(null, null, UnsafeMarker.NULLS);
            this.any = any;
            this.write = write;
        }

        @Override
        public boolean equalsIgnoreRange(Entry that)
        {
            return this.any.equals(that.any) && this.write.equals(that.write);
        }

        @Override
        public Entry with(RoutingKey start, RoutingKey end)
        {
            return new Entry(start, end, any, write);
        }

        public Timestamp mergeMax(Timestamp atLeast)
        {
            return Timestamp.mergeMax(atLeast, any);
        }

        public Timestamp mergeMaxWrite(Timestamp atLeast)
        {
            return Timestamp.mergeMax(atLeast, write);
        }

        public static Entry reduce(RoutingKey start, RoutingKey end, Entry a, Entry b)
        {
            Timestamp any = Timestamp.mergeMax(a.any, b.any);
            Timestamp write = Timestamp.mergeMax(a.write, b.write);
            if (any == a.any && write == a.write && a.equalsRange(start, end))
                return a;
            if (any.equals(b.any) && write.equals(b.write) && b.equalsRange(start, end))
                return b;
            return new Entry(start, end, any, write);
        }
    }
    public static final MaxConflicts EMPTY = new MaxConflicts();

    private MaxConflicts()
    {
        super();
    }

    private MaxConflicts(Object[] tree)
    {
        super(tree);
    }

    Timestamp get(TxnId txnId, Routables<?> keysOrRanges)
    {
        return txnId.isSomeRead() ? getMaxWrite(keysOrRanges) : getMax(keysOrRanges);
    }

    Timestamp getMaxWrite(Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Entry::mergeMaxWrite, Timestamp.NONE);
    }

    Timestamp getMax(Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Entry::mergeMax, Timestamp.NONE);
    }

    public MaxConflicts update(TxnId txnId, Routables<?> keysOrRanges, Timestamp executeAt)
    {
        return update(keysOrRanges, executeAt, txnId.isSomeRead() ? Timestamp.NONE : executeAt);
    }

    public MaxConflicts update(Routables<?> keysOrRanges, Timestamp all, Timestamp write)
    {
        // note: we use mergeMax to ensure we take the maximum epoch and hlc independently from any conflict
        //  this is particularly essential for propagating unique HLCs, so that bootstrap recipients don't
        //  begin serving reads too early
        return add(this, keysOrRanges, new Entry(all, write), Entry::reduce, MaxConflicts::new);
    }

    public static class Builder extends BTreeReducingRangeMap.Builder<Entry, MaxConflicts>
    {
        public MaxConflicts build()
        {
            return build(MaxConflicts::new);
        }
    }
}
