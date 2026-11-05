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

package accord.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import accord.api.Write;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import javax.annotation.Nullable;

public class Writes
{
    public final TxnId txnId;
    public final Timestamp executeAt;
    /**
     * The keys for which {@link #write} may carry data, i.e. the context against which {@link #write} is serialized.
     * This is NOT necessarily the set of keys the transaction writes, as some (or all) of these writes may be
     * derivable from the transaction definition (and so are not included in {@link #write}); a serialized Writes may
     * include only those keys whose writes are not derivable. It MUST NOT be used to decide which keys to apply
     * (see {@link #keysToApply}).
     */
    public final Seekables<?, ?> keys;
    @Nullable public final Write write;

    public Writes(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Write write)
    {
        this.txnId = txnId;
        this.executeAt = executeAt;
        this.keys = keys;
        this.write = write;
    }

    /**
     * NOTE: ignores {@link #keys}, which is only the context against which {@link #write} is serialized, and may
     * legitimately differ between equivalent Writes (e.g. once serialized with only the keys whose writes are not derivable)
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Writes writes = (Writes) o;
        return txnId.equals(writes.txnId) && executeAt.equals(writes.executeAt) && Objects.equals(write, writes.write);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(txnId, executeAt, write);
    }

    /**
     * The keys we must apply. NOTE: these are derived from our own definition of the transaction, NOT {@link #keys},
     * which describes only the keys for which these Writes were computed. A replica that executes a multi key
     * transaction itself computes Writes only for the keys it executes, but these Writes may then be propagated to
     * (or recovered by) other replicas, whose keys they do not include. Since any write that is derivable from the
     * transaction definition is not included in {@link #write}, every replica may apply its own keys given its
     * (necessarily complete for the keys it executes) {@link PartialTxn}.
     */
    public Seekables<?, ?> keysToApply(@Nullable PartialTxn txn)
    {
        if (write == null)
            return keys;

        // we must know the definition to apply anything, as keys does not tell us which keys to apply
        Invariants.require(txn != null && txn.update() != null, "Cannot apply %s without the transaction definition", this);
        return txn.update().keys();
    }

    public boolean appliesAny(@Nullable PartialTxn txn, Participants<?> executes)
    {
        return write != null && !executes.isEmpty() && keysToApply(txn).intersects(executes);
    }

    public AsyncChain<Void> apply(SafeCommandStore safeStore, Participants<?> executes, PartialTxn txn)
    {
        return apply(Write::apply, safeStore, executes, txn);
    }

    public AsyncChain<Void> applyDirect(CommandStore unsafeStore, Participants<?> executes, PartialTxn txn)
    {
        return apply(Write::applyDirect, unsafeStore, executes, txn);
    }

    public interface ApplyWrite<W, P>
    {
        AsyncChain<Void> apply(W write, P param, Seekable seekable, TxnId txnId, Timestamp executeAt, PartialTxn txn);
    }

    public <W extends Write, P> AsyncChain<Void> apply(ApplyWrite<W, P> f, P param, Participants<?> executes, PartialTxn txn)
    {
        if (write == null || executes.isEmpty())
            return AsyncChains.success(null);

        Seekables<?, ?> keys = keysToApply(txn).overlapping(executes);
        int count = keys.size();
        switch (count)
        {
            case 0: return AsyncChains.success(null);
            case 1: return f.apply((W)write, param, keys.get(0), txnId, executeAt, txn);
            default:
            {
                List<AsyncChain<Void>> futures = new ArrayList<>(keys.size());
                for (int i = 0 ; i < count ; ++i)
                    futures.add(f.apply((W)write, param, keys.get(i), txnId, executeAt, txn));
                return AsyncChains.reduce(futures, (l, r) -> null);
            }
        }
    }

    @Override
    public String toString()
    {
        return "TxnWrites{" +
               "txnId:" + txnId +
               ", executeAt:" + executeAt +
               ", keys:" + keys +
               ", write:" + write +
               '}';
    }
}
