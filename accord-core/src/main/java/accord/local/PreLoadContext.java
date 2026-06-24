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
import accord.api.VisibleForImplementation;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routables;
import accord.primitives.Routables.Slice;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import accord.primitives.Unseekables;
import accord.utils.Invariants;
import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.AbstractList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import static accord.local.LoadKeys.NONE;
import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.WRITE;

/**
 * Lists txnids and keys of commands and commands for key that will be needed for an operation. Used
 * to ensure the necessary state is in memory for an operation before it executes.
 *
 * TODO (desired): rename to simply Context, or LoadContext
 */
public interface PreLoadContext
{
    @Nullable TxnId primaryTxnId();
    String reason();

    /**
     * @return ids of the {@link Command} objects that need to be loaded into memory before this operation is run
     *
     * This should ONLY be non-null if primaryTxnId() is non-null
     *
     * TODO (expected): this is used for Apply, NotifyWaitingOn and listenerContexts; others only use a single txnId
     *  The information we need in memory is super minimal for secondary transactions (mostly just SaveStatus?).
     */
    default @Nullable TxnId additionalTxnId() { return null; }

    default List<TxnId> txnIds()
    {
        TxnId primaryTxnId = primaryTxnId();
        TxnId additionalTxnId = additionalTxnId();
        Invariants.require(primaryTxnId != null || additionalTxnId == null);
        return new AbstractList<>()
        {
            @Override
            public TxnId get(int index)
            {
                return index == 0 ? primaryTxnId : additionalTxnId;
            }

            @Override
            public int size()
            {
                return primaryTxnId == null ? 0 : additionalTxnId == null ? 1 : 2;
            }
        };
    }

    @Inline
    default void forEachId(Consumer<TxnId> consumer)
    {
        TxnId primaryTxnId = primaryTxnId();
        if (primaryTxnId != null)
            consumer.accept(primaryTxnId);
        TxnId additionalTxnId = additionalTxnId();
        if (additionalTxnId != null)
            consumer.accept(additionalTxnId);
    }

    default PreLoadContext slice(Ranges ranges, Slice slice)
    {
        Unseekables<?> keys = keys();
        int size = keys.size();
        if (size == 0 || loadKeys() == NONE)
            return this;

        Unseekables<?> newKeys = keys.slice(ranges, slice);
        if (newKeys == keys)
            return this;

        return new OverrideKeys(this, newKeys);
    }

    /**
     * @return keys of the {@link CommandsForKey} objects that need to be loaded into memory before this operation is run
     */
    default Unseekables<?> keys() { return RoutingKeys.EMPTY; }

    default LoadKeys loadKeys() { return NONE; }

    default LoadKeysFor loadKeysFor() { return WRITE; }

    default Timestamp executeAt() { return primaryTxnId(); }

    default boolean isEmpty()
    {
        boolean isEmpty = primaryTxnId() == null && keys().isEmpty();
        Invariants.require(additionalTxnId() == null);
        return isEmpty;
    }
    
    /**
     * Is the provided PreLoadContext guaranteed to have a superset of our requested information?
     * Note that for this calculation we are asking if all the information we want is known to be available,
     * not whether a subset has been requested - that is, a superset with INCR or ASYNC key information
     * cannot be relied upon for serving INCR or ASYNC subsets in this calculation.
     */
    default boolean isSubsetOf(PreLoadContext superset)
    {
        Unseekables<?> keys = keys();
        if (!keys.isEmpty())
        {
            LoadKeys requiredHistory = loadKeys();
            if (requiredHistory != NONE)
            {
                if (requiredHistory.compareTo(SYNC) < 0)
                    requiredHistory = SYNC;
                if (requiredHistory.compareTo(superset.loadKeys()) < 0)
                    return false;
                if (loadKeysFor().compareTo(superset.loadKeysFor()) > 0)
                    return false;
            }

            Unseekables<?> supersetKeys = superset.keys();
            if (supersetKeys.domain() != keys.domain() || !supersetKeys.containsAll(keys()))
                return false;
        }

        TxnId primaryId = primaryTxnId();
        TxnId additionalId = additionalTxnId();
        if (additionalId == null)
        {
            return primaryId == null || primaryId.equals(superset.primaryTxnId()) || primaryId.equals(superset.additionalTxnId());
        }
        else
        {
            Invariants.require(primaryId != null);
            TxnId supersetPrimaryId = superset.primaryTxnId();
            TxnId supersetAdditionalId = superset.additionalTxnId();
            return (primaryId.equals(supersetPrimaryId) || primaryId.equals(supersetAdditionalId)) && (additionalId.equals(supersetAdditionalId) || additionalId.equals(supersetPrimaryId));
        }
    }

    default String describe()
    {
        List<TxnId> txnIds = txnIds();
        Unseekables<?> keys = keys();
        return reason() + (txnIds.isEmpty() ? "" : " for " + txnIds) + (keys.isEmpty() ? "" : (txnIds.isEmpty() ? " for " : " and ") + keys());
    }

    class Wrapped implements PreLoadContext
    {
        final PreLoadContext wrapped;
        public Wrapped(PreLoadContext wrapped)
        {
            this.wrapped = wrapped;
        }
        @Nullable @Override public TxnId primaryTxnId() { return wrapped.primaryTxnId(); }
        @Nullable @Override public TxnId additionalTxnId() { return wrapped.additionalTxnId(); }
        @Override public Unseekables<?> keys() { return wrapped.keys(); }
        @Override public LoadKeys loadKeys() { return wrapped.loadKeys(); }
        @Override public LoadKeysFor loadKeysFor() { return wrapped.loadKeysFor(); }
        @Override public Timestamp executeAt() { return wrapped.executeAt(); }
        @Override public String reason() { return wrapped.reason(); }
        @Override public String describe() { return wrapped.describe(); }
        @Override public String toString() { return wrapped.toString(); }
    }

    class OverrideKeys extends Wrapped
    {
        final Unseekables<?> keys;
        public OverrideKeys(PreLoadContext wrapped, Unseekables<?> keys)
        {
            super(wrapped);
            this.keys = keys;
        }

        @Override public Unseekables<?> keys() { return keys; }
    }

    static PreLoadContext contextFor(@Nullable TxnId primary, @Nullable TxnId additional, Unseekables<?> keys, LoadKeys loadKeys, LoadKeysFor loadKeysFor, String reason)
    {
        Invariants.require(primary == null ? additional == null : !primary.equals(additional));
        return new PreLoadContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return primary; }
            @Override public @Nullable TxnId additionalTxnId() { return additional; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return loadKeys; }
            @Override public LoadKeysFor loadKeysFor() { return loadKeysFor; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    default boolean contains(TxnId txnId)
    {
        TxnId primaryTxnId = primaryTxnId();
        return primaryTxnId != null && (txnId.equals(primaryTxnId) || txnId.equals(additionalTxnId()));
    }

    static PreLoadContext contextFor(TxnId primary, TxnId additional, String reason)
    {
        return new PreLoadContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return primary; }
            @Override public @Nullable TxnId additionalTxnId() { return additional; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    static PreLoadContext contextFor(TxnId primary, String reason)
    {
        return new PreLoadContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return primary; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    static PreLoadContext contextFor(TxnId txnId, Unseekables<?> keys, LoadKeys loadKeys, LoadKeysFor loadKeysFor, String reason)
    {
        return new PreLoadContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return txnId; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return loadKeys; }
            @Override public LoadKeysFor loadKeysFor() { return loadKeysFor; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    static PreLoadContext contextFor(RoutingKey key, LoadKeys loadKeys, LoadKeysFor loadKeysFor, String describe)
    {
        return contextFor(RoutingKeys.of(key), loadKeys, loadKeysFor, describe);
    }

    // we don't currently permit range queries without an associated TxnId
    static PreLoadContext contextFor(AbstractUnseekableKeys keys, LoadKeys loadKeys, LoadKeysFor loadKeysFor, String reason)
    {
        Invariants.require(keys.domain() == Routable.Domain.Key);
        return new PreLoadContext()
        {
            @Override public @Nullable TxnId primaryTxnId() { return null; }
            @Override public Unseekables<?> keys() { return keys; }
            @Override public LoadKeys loadKeys() { return loadKeys; }
            @Override public LoadKeysFor loadKeysFor() { return loadKeysFor; }
            @Override public String reason() { return reason; }
            @Override public String toString() { return describe(); }
        };
    }

    interface Empty extends PreLoadContext
    {
        @Override default @Nullable TxnId primaryTxnId() { return null; }
    }
}
