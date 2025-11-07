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

import accord.api.TopologySorter;
import accord.local.Node;
import accord.primitives.Participants;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.IndexedBiFunction;

import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.primitives.TxnId.FastPath.Unoptimised;

abstract class TopologyCollector<C, K, T, E extends Exception>
{
    abstract C allocate(int size);
    abstract C update(C collector, ActiveEpoch e, K select, boolean permitMissing);
    T none() { throw new UnsupportedOperationException(); }
    abstract T one(ActiveEpoch e, K select, boolean permitMissing);
    abstract T multi(C collector);
    abstract T retired(long requestedEpoch, long minEpoch) throws E;
    abstract T notReady(long requestedEpoch, long maxEpoch) throws E;

    static class Simple extends TopologyCollector<Topologies.Builder, Routables<?>, Topologies, TopologyException>
    {
        final TopologySorter.Supplier sorter;
        final Topologies.SelectNodeOwnership selectNodeOwnership;

        Simple(TopologySorter.Supplier sorter, Topologies.SelectNodeOwnership selectNodeOwnership)
        {
            this.sorter = sorter;
            this.selectNodeOwnership = selectNodeOwnership;
        }

        @Override
        public Topologies.Builder update(Topologies.Builder collector, ActiveEpoch e, Routables<?> select, boolean permitMissing)
        {
            collector.add(e.global.select(select, permitMissing, selectNodeOwnership));
            return collector;
        }

        @Override
        public Topologies one(ActiveEpoch e, Routables<?> unseekables, boolean permitMissing)
        {
            return new Topologies.Single(sorter, e.global.select(unseekables, permitMissing, selectNodeOwnership));
        }

        @Override
        public Topologies multi(Topologies.Builder builder)
        {
            return builder.build(sorter);
        }

        @Override
        Topologies retired(long requestedEpoch, long minEpoch) throws TopologyException
        {
            throw new TopologyRetiredException(requestedEpoch, minEpoch);
        }

        @Override
        Topologies notReady(long requestedEpoch, long maxEpoch) throws TopologyException
        {
            throw new TopologyNotReadyException(requestedEpoch, maxEpoch);
        }

        @Override
        public Topologies.Builder allocate(int count)
        {
            return new Topologies.Builder(count);
        }
    }

    static class BestFastPath extends TopologyCollector<TxnId.FastPath, Routables<?>, TxnId.FastPath, RuntimeException> implements IndexedBiFunction<Shard, Boolean, Boolean>
    {
        final Node.Id self;

        BestFastPath(Node.Id self)
        {
            this.self = self;
        }

        @Override
        public TxnId.FastPath update(TxnId.FastPath collector, ActiveEpoch e, Routables<?> select, boolean permitMissing)
        {
            return merge(collector, one(e, select, permitMissing));
        }

        @Override
        public TxnId.FastPath one(ActiveEpoch e, Routables<?> routables, boolean permitMissing)
        {
            if (!e.local.ranges.containsAll(routables) || !e.local.foldl(routables, this, true))
                return Unoptimised;

            return e.local.foldl(routables, (s, v, i) -> merge(v, s.bestFastPath()), null);
        }

        @Override
        public TxnId.FastPath multi(TxnId.FastPath result)
        {
            return result;
        }

        @Override
        TxnId.FastPath retired(long requestedEpoch, long minEpoch)
        {
            return Unoptimised;
        }

        @Override
        TxnId.FastPath notReady(long requestedEpoch, long maxEpoch)
        {
            return Unoptimised;
        }

        @Override
        public TxnId.FastPath allocate(int count)
        {
            return null;
        }

        private static TxnId.FastPath merge(TxnId.FastPath a, TxnId.FastPath b)
        {
            if (a == null) return b;
            if (a == Unoptimised || b == Unoptimised) return Unoptimised;
            if (a == PrivilegedCoordinatorWithDeps || b == PrivilegedCoordinatorWithDeps) return PrivilegedCoordinatorWithDeps;
            return PrivilegedCoordinatorWithoutDeps;
        }

        @Override
        public Boolean apply(Shard shard, Boolean prev, int index)
        {
            return prev && shard.isInFastPath(self);
        }
    }

    static class SupportsPrivilegedFastPath extends TopologyCollector<Boolean, Routables<?>, Boolean, RuntimeException> implements IndexedBiFunction<Shard, Boolean, Boolean>
    {
        final Node.Id self;

        SupportsPrivilegedFastPath(Node.Id self)
        {
            this.self = self;
        }

        @Override
        public Boolean update(Boolean collector, ActiveEpoch e, Routables<?> select, boolean permitMissing)
        {
            return collector && one(e, select, permitMissing);
        }

        @Override
        public Boolean one(ActiveEpoch e, Routables<?> routables, boolean permitMissing)
        {
            return e.local.ranges.containsAll(routables) && e.local.foldl(routables, this, true);
        }

        @Override
        public Boolean multi(Boolean result)
        {
            return result;
        }

        @Override
        Boolean retired(long requestedEpoch, long minEpoch) throws RuntimeException
        {
            return false;
        }

        @Override
        Boolean notReady(long requestedEpoch, long maxEpoch) throws RuntimeException
        {
            return false;
        }

        @Override
        public Boolean allocate(int count)
        {
            return true;
        }

        @Override
        public Boolean apply(Shard shard, Boolean prev, int index)
        {
            return prev && shard.isInFastPath(self);
        }
    }

    static class UnsyncedSelector<K extends Participants<?>> extends TopologyCollector<K, K, K, TopologyException>
    {
        static final UnsyncedSelector INSTANCE = new UnsyncedSelector();

        @Override
        public K allocate(int size)
        {
            return null;
        }

        @Override
        public K none()
        {
            return null;
        }

        @Override
        public K multi(K collector)
        {
            return collector;
        }

        @Override
        K retired(long requestedEpoch, long minEpoch) throws TopologyRetiredException
        {
            throw new TopologyRetiredException(requestedEpoch, minEpoch);
        }

        @Override
        K notReady(long requestedEpoch, long maxEpoch) throws TopologyException
        {
            throw new TopologyNotReadyException(requestedEpoch, maxEpoch);
        }

        @Override
        public K one(ActiveEpoch e, K select, boolean permitMissing)
        {
            return (K) select.without(e.quorumReady());
        }

        @Override
        public K update(K collector, ActiveEpoch e, K select, boolean permitMissing)
        {
            select = (K)select.without(e.quorumReady());
            return collector == null ? select : (K)collector.with((Participants) select);
        }
    }

    static class HasChangedReplication extends TopologyCollector<HasChangedReplication.ReplicationChangeTracker, Unseekables<?>, Boolean, RuntimeException>
    {
        static class ReplicationChangeTracker
        {
            int rf = -1;
            boolean hasChanged;
        }

        static final HasChangedReplication INSTANCE = new HasChangedReplication();

        @Override
        public ReplicationChangeTracker allocate(int size)
        {
            return new ReplicationChangeTracker();
        }

        @Override
        public Boolean none()
        {
            return false;
        }

        @Override
        public Boolean multi(ReplicationChangeTracker collector)
        {
            return collector.hasChanged;
        }

        @Override
        Boolean retired(long requestedEpoch, long minEpoch)
        {
            return true;
        }

        @Override
        Boolean notReady(long requestedEpoch, long maxEpoch)
        {
            return true;
        }

        @Override
        public Boolean one(ActiveEpoch e, Unseekables<?> select, boolean permitMissing)
        {
            return false;
        }

        @Override
        public ReplicationChangeTracker update(ReplicationChangeTracker collector, ActiveEpoch e, Unseekables<?> select, boolean permitMissing)
        {
            e.global.foldl(select, (shard, c, i1) -> {
                if (c.rf < 0) c.rf = shard.rf;
                else c.hasChanged |= c.rf != shard.rf;
                return c;
            }, collector);
            return collector;
        }
    }
}
