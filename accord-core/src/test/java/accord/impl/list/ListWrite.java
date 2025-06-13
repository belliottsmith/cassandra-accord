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

package accord.impl.list;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import accord.primitives.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.Write;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.api.AsyncExecutor;

public class ListWrite extends TreeMap<Key, int[]> implements Write.InMemoryWrite
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);

    private final Function<? super CommandStore, AsyncExecutor> executor;

    public ListWrite(Function<? super CommandStore, AsyncExecutor> executor)
    {
        this.executor = executor;
    }

    @Override
    public AsyncChain<Void> apply(SafeCommandStore safeStore, Seekable key, TxnId txnId, Timestamp executeAt, PartialTxn txn)
    {
        return applyDirect(safeStore.commandStore(), key, txnId, executeAt, txn);
    }

    @Override
    public AsyncChain<Void> applyDirect(CommandStore unsafeStore, Seekable key, TxnId txnId, Timestamp executeAt, PartialTxn txn)
    {
        ListStore dataStore = (ListStore) unsafeStore.unsafeGetDataStore();
        logger.trace("submitting WRITE on {} at {} key:{}", dataStore.node, executeAt, key);
        return executor.apply(unsafeStore).build(() -> {
            applySync(unsafeStore, key, txnId, executeAt, txn);
            return null;
        });
    }

    @Override
    public AsyncChain<Void> applySync(CommandStore unsafeStore, Seekable key, TxnId txnId, Timestamp executeAt, PartialTxn txn)
    {
        ListStore dataStore = (ListStore) unsafeStore.unsafeGetDataStore();
        int[] data = get(key);
        dataStore.write((Key)key, executeAt, data);
        logger.trace("WRITE on {} at {} key:{} -> {}", dataStore.node, executeAt, key, data);
        return AsyncChains.success(null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) return true;
        if (!(o instanceof ListWrite)) return false;
        ListWrite other = (ListWrite) o;
        // Can not rely on Map.equals as our value is an array: (new int[] {2}).equals(new int[] {2}) == false!
        if (!Sets.difference(keySet(), other.keySet()).isEmpty()
            || !Sets.difference(other.keySet(), keySet()).isEmpty())
            return false;
        // keys match
        for (Key k : keySet())
        {
            if (!Arrays.equals(get(k), other.get(k)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + ":" + Arrays.toString(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
