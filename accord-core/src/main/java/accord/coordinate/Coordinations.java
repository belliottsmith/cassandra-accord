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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.coordinate.Coordination.CoordinationKind;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import accord.utils.FoldToLong;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;

public class Coordinations implements Iterable<Coordination>
{
    static class Entries
    {
        Coordination[] coordinations = new Coordination[1];
        int count;

        synchronized void add(Coordination coordination)
        {
            Invariants.require(coordination != null);
            if (count == coordinations.length)
                coordinations = Arrays.copyOf(coordinations, count * 2);
            coordinations[count++] = coordination;
        }

        synchronized long foldl(FoldToLong<Coordination> fold, long accumulate)
        {
            for (int i = 0 ; i < count ; ++i)
                accumulate = fold.apply(coordinations[i], accumulate);
            return accumulate;
        }

        synchronized boolean remove(Coordination coordination)
        {
            for (int i = 0 ; i < count ; ++i)
            {
                if (coordination == coordinations[i])
                {
                    removeIndex(i);
                    return true;
                }
            }
            return false;
        }

        synchronized boolean remove(long coordinationId)
        {
            for (int i = 0 ; i < count ; ++i)
            {
                if (coordinationId == coordinations[i].coordinationId())
                {
                    removeIndex(i);
                    return true;
                }
            }
            return false;
        }

        private void removeIndex(int index)
        {
            coordinations[index] = coordinations[count - 1];
            coordinations[count - 1] = null;
            --count;
        }

        private synchronized Coordination[] snapshot()
        {
            return Arrays.copyOf(coordinations, count);
        }

        boolean isEmpty()
        {
            return count == 0;
        }

        @Override
        public String toString()
        {
            return Stream.of(coordinations).limit(count).map(Object::toString).collect(Collectors.joining(", ", "[", "]"));
        }
    }

    final ConcurrentHashMap<TxnId, Entries> coordinations = new ConcurrentHashMap<>();

    public void forEach(TxnId txnId, Consumer<Coordination> consumer)
    {
        coordinations.compute(txnId, (ignore, e) -> {
            if (e == null)
                return null;

            for (int i = 0 ; i < e.count ; ++i)
                consumer.accept(e.coordinations[i]);
            return e;
        });
    }

    public void register(Coordination coordination)
    {
        coordinations.compute(coordination.txnId(), (txnId, entries) -> {
            if (entries == null) entries = new Entries();
            entries.add(coordination);
            return entries;
        });
    }

    public void unregister(Coordination coordination)
    {
        coordinations.compute(coordination.txnId(), (txnId, entries) -> {
            if (entries != null && entries.remove(coordination) && entries.isEmpty())
                return null;
            return entries;
        });
    }

    public void unregister(TxnId txnId, long coordinationId)
    {
        coordinations.compute(txnId, (ignore, entries) -> {
            if (entries != null && entries.remove(coordinationId) && entries.isEmpty())
                return null;
            return entries;
        });
    }

    public long mostRecent(TxnId txnId, TinyEnumSet<CoordinationKind> kinds, Ballot ballot)
    {
        Entries entries = coordinations.get(txnId);
        if (entries == null)
            return Long.MIN_VALUE;
        return entries.foldl((c, v) -> {
            if (!kinds.contains(c.kind()) || !ballot.equals(c.ballot()))
                return v;
            return Math.max(v, c.coordinationId());
        }, Long.MIN_VALUE);
    }

    public Stream<Coordination> stream()
    {
        return coordinations.values().stream().flatMap(e -> Arrays.stream(e.snapshot()));
    }

    @Override
    public Iterator<Coordination> iterator()
    {
        return stream().iterator();
    }
}
