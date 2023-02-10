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

import accord.api.*;
import accord.api.ProgressLog;
import accord.api.DataStore;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.primitives.*;
import accord.utils.ReducingRangeMap;
import org.apache.cassandra.utils.concurrent.Future;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int id,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpochHolder rangesForEpoch);
    }

    private final int id; // unique id

    // TODO (expected): schedule regular pruning of this collection
    @Nullable ReducingRangeMap<Timestamp> rejectBefore;

    public CommandStore(int id)
    {
        this.id = id;
    }

    public int id()
    {
        return id;
    }

    protected void setRejectBefore(ReducingRangeMap<Timestamp> newRejectBefore)
    {
        this.rejectBefore = newRejectBefore;
    }

    Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys, SafeCommandStore safeStore)
    {
        NodeTimeService time = safeStore.time();
        boolean reject = agent().isExpired(txnId, safeStore.time().now());
        if (rejectBefore != null && !reject)
            reject = null == rejectBefore.foldl(keys, (rejectIfBefore, test) -> rejectIfBefore.compareTo(test) >= 0 ? null : test, txnId, Objects::isNull);

        if (reject)
            return time.uniqueNow(txnId).asRejected();

        if (txnId.rw() == ExclusiveSyncPoint)
        {
            Ranges ranges = (Ranges)keys;
            ReducingRangeMap<Timestamp> newRejectBefore = rejectBefore != null ? rejectBefore : new ReducingRangeMap<>(Timestamp.NONE);
            newRejectBefore = ReducingRangeMap.add(newRejectBefore, ranges, txnId, Timestamp::max);
            setRejectBefore(newRejectBefore);
        }

        Timestamp maxConflict = safeStore.maxConflict(keys, safeStore.ranges().at(txnId.epoch()));
        if (txnId.compareTo(maxConflict) > 0 && txnId.epoch() >= time.epoch())
            return txnId;

        return time.uniqueNow(maxConflict);
    }

    public abstract Agent agent();
    public abstract Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    public abstract <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply);
    public abstract void shutdown();
}
