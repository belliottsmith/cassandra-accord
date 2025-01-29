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

package accord.topology;

import java.util.List;
import java.util.Set;

import accord.api.VisibleForImplementation;
import accord.local.Node.Id;
import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Shorts;

// TODO (expected): introduce recovery quorum size configuration
public class Shard
{
    public static class SerializerSupport
    {
        public static Shard create(Range range, SortedArrayList<Id> nodes, SortedArrayList<Id> notInFastPath, SortedArrayList<Id> joining, boolean pendingRemoval)
        {
            return new Shard(range, nodes, notInFastPath, joining, pendingRemoval);
        }
    }

    private static final SortedArrayList<Id> NO_NODES = SortedArrayList.ofSorted(new Id[0]);

    public final Range range;
    public final SortedArrayList<Id> nodes;
    public final SortedArrayList<Id> notInFastPath;
    public final SortedArrayList<Id> joining;
    public final short rf;
    public final short maxFailures;
    public final short fastPathElectorateSize;
    public final short simpleFastQuorumSize;
    public final short privilegedWithoutDepsFastQuorumSize;
    public final short privilegedWithDepsFastQuorumSize;
    public final short slowQuorumSize;
    public final short recoveryQuorumSize;
    public final boolean pendingRemoval;

    Shard(Range range, SortedArrayList<Id> nodes, SortedArrayList<Id> notInFastPath, SortedArrayList<Id> joining, boolean pendingRemoval)
    {
        this.range = range;
        this.nodes = nodes;
        this.notInFastPath = Invariants.requireArgument(notInFastPath, nodes.containsAll(notInFastPath));
        this.joining = Invariants.requireArgument(joining, nodes.containsAll(joining),
                                                  "joining nodes must also be present in nodes; joining=%s, nodes=%s", joining, nodes);
        this.rf = Shorts.saturatedCast(nodes.size());
        this.maxFailures = Shorts.saturatedCast(maxToleratedFailures(rf));
        this.fastPathElectorateSize = Shorts.saturatedCast(nodes.size() - notInFastPath.size());
        this.slowQuorumSize = Shorts.saturatedCast(slowQuorumSize(nodes.size()));
        this.recoveryQuorumSize = slowQuorumSize;
        this.simpleFastQuorumSize = Shorts.saturatedCast(simpleFastQuorumSize(rf, fastPathElectorateSize, recoveryQuorumSize));
        this.privilegedWithoutDepsFastQuorumSize = Shorts.saturatedCast(privilegedWithoutDepsFastQuorumSize(rf, fastPathElectorateSize, recoveryQuorumSize));
        this.privilegedWithDepsFastQuorumSize = Shorts.saturatedCast(privilegedWithDepsFastQuorumSize(rf, fastPathElectorateSize, recoveryQuorumSize));
        this.pendingRemoval = pendingRemoval;
    }

    public static Shard create(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate, Set<Id> joining)
    {
        return create(range, nodes, fastPathElectorate, joining, false);
    }

    public static Shard create(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate)
    {
        return create(range, nodes, fastPathElectorate, false);
    }

    public static Shard create(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate, boolean pendingRemoval)
    {
        return create(range, nodes, fastPathElectorate, NO_NODES, pendingRemoval);
    }


    public static Shard create(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate, Set<Id> joining, boolean pendingRemoval)
    {
        Invariants.requireArgument(nodes.containsAll(fastPathElectorate));
        return new Shard(range, nodes, nodes.without(fastPathElectorate::contains),
                         joining instanceof SortedArrayList<?> ? (SortedArrayList<Id>) joining : SortedArrayList.copyUnsorted(joining, Id[]::new),
                         pendingRemoval);
    }

    public boolean electorateIsSubset()
    {
        return !notInFastPath.isEmpty();
    }

    public boolean rejectsFastPath(TxnId txnId, int rejections)
    {
        return rejections > fastPathElectorateSize - fastQuorumSize(txnId);
    }

    public boolean rejectsFastPath(int fastQuorumSize, int rejections)
    {
        return rejections > fastPathElectorateSize - fastQuorumSize;
    }

    public boolean acceptsFastPath(TxnId txnId, int accepts)
    {
        return accepts >= fastQuorumSize(txnId);
    }

    public int fastQuorumSize(TxnId txnId)
    {
        return fastQuorumSize(txnId.fastPath());
    }

    public int fastQuorumSize(FastPath fastPath)
    {
        switch (fastPath)
        {
            default: throw new UnhandledEnum(fastPath);
            case Unoptimised: return simpleFastQuorumSize;
            case PrivilegedCoordinatorWithoutDeps: return privilegedWithoutDepsFastQuorumSize;
            case PrivilegedCoordinatorWithDeps: return privilegedWithDepsFastQuorumSize;
        }
    }

    public boolean isInFastPath(Id id)
    {
        return !notInFastPath.contains(id);
    }

    public int rf()
    {
        return rf;
    }

    public boolean contains(RoutableKey key)
    {
        return range.contains(key);
    }

    public String toString(boolean extendedInfo)
    {
        String s = "Shard[" + range.start() + ',' + range.end() + ']';

        if (extendedInfo)
        {
            StringBuilder sb = new StringBuilder(s);
            sb.append(":(");
            for (int i=0, mi=nodes.size(); i<mi; i++)
            {
                if (i > 0)
                    sb.append(", ");

                Id node = nodes.get(i);
                sb.append(node);
                if (isInFastPath(node))
                    sb.append('f');
            }
            sb.append(')');
            if (!joining.isEmpty())
                sb.append(":joining=").append(joining);
            s = sb.toString();
        }
        return s;
    }

    public boolean contains(Id id)
    {
        return nodes.find(id) >= 0;
    }

    public boolean containsAll(List<Id> ids)
    {
        for (int i = 0, max = ids.size() ; i < max ; ++i)
        {
            if (!contains(ids.get(i)))
                return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return toString(true);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return    rf == shard.rf
                  && maxFailures == shard.maxFailures
                  && fastPathElectorateSize == shard.fastPathElectorateSize
                  && simpleFastQuorumSize == shard.simpleFastQuorumSize
                  && privilegedWithoutDepsFastQuorumSize == shard.privilegedWithoutDepsFastQuorumSize
                  && privilegedWithDepsFastQuorumSize == shard.privilegedWithDepsFastQuorumSize
                  && slowQuorumSize == shard.slowQuorumSize
                  && range.equals(shard.range)
                  && nodes.equals(shard.nodes)
                  && notInFastPath.equals(shard.notInFastPath)
                  && joining.equals(shard.joining);
    }

    @Override
    public int hashCode()
    {
        return range.hashCode();
    }

    @VisibleForTesting
    public static int maxToleratedFailures(int replicas)
    {
        return (replicas - 1) / 2;
    }

    @VisibleForTesting
    public static int simpleFastQuorumSize(int rf, int electorate, int recoveryQuorum)
    {
        return fastQuorumSize(rf, electorate, recoveryQuorum, 1);
    }

    @VisibleForTesting
    public static int privilegedWithoutDepsFastQuorumSize(int rf, int electorate, int recoveryQuorum)
    {
        return fastQuorumSize(rf, electorate, recoveryQuorum, 0);
    }

    @VisibleForTesting
    public static int privilegedWithDepsFastQuorumSize(int rf, int electorate, int recoveryQuorum)
    {
        return fastQuorumSize(rf, electorate, recoveryQuorum, -1);
    }

    static int fastQuorumSize(int rf, int electorate, int recoveryQuorum, int d)
    {
        Invariants.requireArgument(electorate >= recoveryQuorum);
        return Math.max((d + electorate + rf - recoveryQuorum + 1)/2, (rf/2) + 1);
    }

    @VisibleForImplementation
    public static int slowQuorumSize(int replicas)
    {
        return replicas - maxToleratedFailures(replicas);
    }

    public FastPath bestFastPath()
    {
        return privilegedWithDepsFastQuorumSize == privilegedWithoutDepsFastQuorumSize
               ? FastPath.PrivilegedCoordinatorWithoutDeps
               : FastPath.PrivilegedCoordinatorWithDeps;
    }
}
