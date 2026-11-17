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

import java.util.function.BiPredicate;

import accord.api.TopologySorter;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;

/**
 * A TopologySorter for simulated clusters with multiple DCs: prefers self, then nodes in the same DC,
 * then (as {@link SizeOfIntersectionSorter}) nodes that intersect more of the shards
 */
public class DcAwareSorter implements TopologySorter
{
    public static class Supplier implements TopologySorter.Supplier
    {
        final Node.Id self;
        final BiPredicate<Node.Id, Node.Id> sameDc;
        final java.util.function.ToIntFunction<Node.Id> executionPreference;

        public Supplier(Node.Id self, BiPredicate<Node.Id, Node.Id> sameDc, java.util.function.ToIntFunction<Node.Id> executionPreference)
        {
            this.self = self;
            this.sameDc = sameDc;
            this.executionPreference = executionPreference;
        }

        @Override
        public int executionPreference(Node.Id node)
        {
            return executionPreference.applyAsInt(node);
        }

        @Override
        public TopologySorter get(Topology topology)
        {
            return new DcAwareSorter(this, SizeOfIntersectionSorter.SUPPLIER.get(topology));
        }

        @Override
        public TopologySorter get(Topologies topologies)
        {
            return new DcAwareSorter(this, SizeOfIntersectionSorter.SUPPLIER.get(topologies));
        }

        @Override
        public boolean isNearby(Node.Id node)
        {
            return sameDc.test(self, node);
        }

        @Override
        public boolean areNearby(Node.Id a, Node.Id b)
        {
            return sameDc.test(a, b);
        }

        int rank(Node.Id node)
        {
            return node.equals(self) ? 2 : isNearby(node) ? 1 : 0;
        }
    }

    final Supplier supplier;
    final TopologySorter delegate;

    DcAwareSorter(Supplier supplier, TopologySorter delegate)
    {
        this.supplier = supplier;
        this.delegate = delegate;
    }

    @Override
    public int compare(Node.Id node1, Node.Id node2, ShardSelection shards)
    {
        // least preferable sorts first
        int c = Integer.compare(supplier.rank(node1), supplier.rank(node2));
        if (c != 0) return c;
        return delegate.compare(node1, node2, shards);
    }

    @Override
    public NodeStatus status(Node.Id node)
    {
        return delegate.status(node);
    }
}
