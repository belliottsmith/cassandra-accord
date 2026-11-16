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

package accord.coordinate;

import org.agrona.collections.IntHashSet;

import accord.local.Node;
import accord.utils.SortedArrays;
import accord.utils.SortedList;
import accord.utils.SortedListSet;
import accord.utils.TinyEnumSet;

public enum ExecuteFlag
{
    READY_TO_EXECUTE, HAS_UNIQUE_HLC, NO_WAIT;

    private static final ExecuteFlag[] LOOKUP = values();
    public static ExecuteFlag forOrdinal(int ordinal)
    {
        return LOOKUP[ordinal];
    }

    public static final class ExecuteFlags extends TinyEnumSet<ExecuteFlag>
    {
        private static final ExecuteFlags[] LOOKUP = new ExecuteFlags[1 << ExecuteFlag.values().length];
        static
        {
            for (int i = 0 ; i < LOOKUP.length ; ++i)
                LOOKUP[i] = new ExecuteFlags(i);
        }
        public static ExecuteFlags none() { return LOOKUP[0]; }
        public static ExecuteFlags all() { return LOOKUP[LOOKUP.length - 1]; }
        public static ExecuteFlags get(int bits) { return LOOKUP[bits]; }
        public static ExecuteFlags get(ExecuteFlag a) { return LOOKUP[encode(a)]; }
        public static ExecuteFlags get(ExecuteFlag a, ExecuteFlag b) { return LOOKUP[encode(a) | encode(b)]; }
        public ExecuteFlags with(ExecuteFlag a) { return LOOKUP[bitset | encode(a)]; }
        public ExecuteFlags without(ExecuteFlag a) { return LOOKUP[bitset & ~encode(a)]; }
        public ExecuteFlags or(ExecuteFlags that) { return selfOrLookup(this.bitset | that.bitset); }
        public ExecuteFlags and(ExecuteFlags that) { return selfOrLookup(this.bitset & that.bitset); }
        public int bits() { return bitset; }
        private ExecuteFlags(int bits) { super(bits); }

        private ExecuteFlags selfOrLookup(int bitset)
        {
            if (bitset == this.bitset)
                return this;
            return LOOKUP[bitset];
        }

        @Override
        public String toString()
        {
            return toString(ExecuteFlag::forOrdinal);
        }

        public static void collect(CoordinationFlags into, Node.Id id, ExecuteFlags add, Object expectIfReadyToExecute, Object actualReadyToExecute)
        {
            // TODO (expected): this is overly restrictive, disabling the optimisation in multi shard cases; should only expect the parts the shard owns to be equal
            if (add != none() && !expectIfReadyToExecute.equals(actualReadyToExecute))
                add = none(); // HAS_UNIQUE_HLC only accurate if READY_TO_EXECUTE was also accurate
            into.add(id, add);
        }
    }

    public interface CoordinationFlags
    {
        boolean isReadyToExecute(Node.Id node);
        boolean isAnyReadyToExecute();
        ExecuteFlags all();

        void setNoWait();
        void add(Node.Id node, ExecuteFlags flags);

        default ExecuteFlags get(Node.Id node)
        {
            ExecuteFlags result = all();
            if (isReadyToExecute(node)) result = result.with(READY_TO_EXECUTE);
            return result;
        }

        /**
         * @return true iff the coordinator has sent {@code node} a message that records the transaction as Stable,
         * so that (assuming it was delivered) it does not need to be sent the transaction's definition or deps again.
         * This is coordinator-local knowledge, and is not sent to replicas.
         */
        default boolean isStableSent(Node.Id node)
        {
            return false;
        }

        /**
         * @return true iff we should persist the transaction only to replicas nearby (e.g. in our DC), reporting to the
         * original coordinator which have applied it (see {@link accord.api.ProtocolModifiers#coordinatorBacklogExecutionPersistLocally()}).
         * This is coordinator-local knowledge, and is not sent to replicas.
         */
        default boolean persistNearbyOnly()
        {
            return false;
        }

        /**
         * @return a view of {@code flags} that additionally reports {@link #persistNearbyOnly()}
         */
        static CoordinationFlags withPersistNearbyOnly(CoordinationFlags flags)
        {
            if (flags instanceof WithStableSent)
                return new WithStableSent(flags, ((WithStableSent) flags).stableSentTo, true);
            return new WithStableSent(flags, new IntHashSet(), true);
        }

        /**
         * @return a view of {@code flags} that additionally reports {@code stableSentTo} via {@link #isStableSent}.
         * {@code stableSentTo} is copied, so may be safely modified afterwards.
         */
        static CoordinationFlags withStableSent(CoordinationFlags flags, IntHashSet stableSentTo)
        {
            if (stableSentTo.isEmpty())
                return flags;
            IntHashSet copy = new IntHashSet(stableSentTo.size());
            copy.addAll(stableSentTo);
            return new WithStableSent(flags, copy, flags.persistNearbyOnly());
        }

        static CoordinationFlags none()
        {
            return of(ExecuteFlags.none());
        }

        static CoordinationFlags of(ExecuteFlags flags)
        {
            return new SmallCoordinationFlags(flags, SmallCoordinationFlags.NO_IDS);
        }

        static CoordinationFlags empty(SortedList<Node.Id> list)
        {
            return list.size() <= 64 ? new SmallCoordinationFlags(ExecuteFlags.none(), list) : new LargeCoordinationFlags(ExecuteFlags.none(), list);
        }
    }

    static class WithStableSent implements CoordinationFlags
    {
        final CoordinationFlags wrapped;
        final IntHashSet stableSentTo;
        final boolean persistNearbyOnly;

        WithStableSent(CoordinationFlags wrapped, IntHashSet stableSentTo, boolean persistNearbyOnly)
        {
            this.wrapped = wrapped instanceof WithStableSent ? ((WithStableSent) wrapped).wrapped : wrapped;
            this.stableSentTo = stableSentTo;
            this.persistNearbyOnly = persistNearbyOnly;
        }

        @Override public boolean isReadyToExecute(Node.Id node) { return wrapped.isReadyToExecute(node); }
        @Override public boolean isAnyReadyToExecute() { return wrapped.isAnyReadyToExecute(); }
        @Override public ExecuteFlags all() { return wrapped.all(); }
        @Override public void setNoWait() { wrapped.setNoWait(); }
        @Override public void add(Node.Id node, ExecuteFlags flags) { wrapped.add(node, flags); }
        @Override public ExecuteFlags get(Node.Id node) { return wrapped.get(node); }
        @Override public boolean isStableSent(Node.Id node) { return stableSentTo.contains(node.id); }
        @Override public boolean persistNearbyOnly() { return persistNearbyOnly; }

        @Override
        public String toString()
        {
            return wrapped + "{stableSentTo:" + stableSentTo + (persistNearbyOnly ? ", persistNearbyOnly" : "") + '}';
        }
    }

    static class SmallCoordinationFlags extends SortedListSet.SmallSortedListSet<Node.Id> implements CoordinationFlags
    {
        private static final SortedList<Node.Id> NO_IDS = new SortedArrays.SortedArrayList<>(new Node.Id[0]);

        private ExecuteFlags all;
        private SmallCoordinationFlags(ExecuteFlags all, SortedList<Node.Id> list)
        {
            super(list);
            if (!list.isEmpty())
                all = all.with(HAS_UNIQUE_HLC);
            this.all = all;
        }

        @Override
        public boolean isReadyToExecute(Node.Id node)
        {
            return contains(node);
        }

        @Override
        public boolean isAnyReadyToExecute()
        {
            return !isEmpty();
        }

        @Override
        public ExecuteFlags all()
        {
            return all;
        }

        @Override
        public void setNoWait()
        {
            all = all.with(NO_WAIT);
        }

        @Override
        public void add(Node.Id node, ExecuteFlags flags)
        {
            if (all != flags)
                all = all.and(flags);
            if (flags.contains(READY_TO_EXECUTE))
                add(node);
        }
    }

    static class LargeCoordinationFlags extends SortedListSet.LargeSortedListSet<Node.Id> implements CoordinationFlags
    {
        ExecuteFlags all;
        private LargeCoordinationFlags(ExecuteFlags all, SortedList<Node.Id> list)
        {
            super(list);
            this.all = all.with(HAS_UNIQUE_HLC);
        }

        @Override
        public boolean isReadyToExecute(Node.Id node)
        {
            return contains(node);
        }

        @Override
        public boolean isAnyReadyToExecute()
        {
            return !isEmpty();
        }

        @Override
        public ExecuteFlags all()
        {
            return all;
        }

        @Override
        public void setNoWait()
        {
            all = all.with(NO_WAIT);
        }

        @Override
        public void add(Node.Id node, ExecuteFlags flags)
        {
            if (all != flags)
                all = all.and(flags);
            if (flags.contains(READY_TO_EXECUTE))
                add(node);
        }
    }
}
