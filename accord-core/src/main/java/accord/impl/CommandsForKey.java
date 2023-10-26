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

package accord.impl;

import accord.api.Key;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.local.*;
import accord.primitives.*;
import com.google.common.collect.ImmutableSortedMap;

import java.util.*;
import java.util.function.Consumer;

import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreCommitted;

public class CommandsForKey implements DomainCommands
{
    public static class SerializerSupport
    {
        public static Listener listener(Key key)
        {
            return new Listener(key);
        }

        public static  <D> CommandsForKey create(Key key,
                                                 CommandLoader<D> loader,
                                                 ImmutableSortedMap<Timestamp, D> byId,
                                                 ImmutableSortedMap<Timestamp, D> byExecuteAt)
        {
            return new CommandsForKey(key, loader, byId, byExecuteAt);
        }
    }

    public static class Listener implements Command.DurableAndIdempotentListener
    {
        protected final Key listenerKey;

        public Listener(Key listenerKey)
        {
            this.listenerKey = listenerKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerKey.equals(that.listenerKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerKey);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerKey + '}';
        }

        public Key key()
        {
            return listenerKey;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            CommandsForKeys.listenerUpdate((AbstractSafeCommandStore<?,?,?,?>) safeStore, listenerKey, safeCommand.current());
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller, Keys.of(listenerKey));
        }
    }

    private final Key key;
    private final CommandTimeseries<?> byId;
    // TODO (expected): we probably do not need to separately maintain byExecuteAt - probably better to just filter byId
    private final CommandTimeseries<?> byExecuteAt;

    <D> CommandsForKey(Key key,
                       CommandTimeseries<D> byId,
                       CommandTimeseries<D> byExecuteAt)
    {
        this.key = key;
        this.byId = byId;
        this.byExecuteAt = byExecuteAt;
    }

    <D> CommandsForKey(Key key,
                       CommandLoader<D> loader,
                       ImmutableSortedMap<Timestamp, D> committedById,
                       ImmutableSortedMap<Timestamp, D> committedByExecuteAt)
    {
        this(key,
             new CommandTimeseries<>(key, loader, committedById),
             new CommandTimeseries<>(key, loader, committedByExecuteAt));
    }

    public <D> CommandsForKey(Key key, CommandLoader<D> loader)
    {
        this.key = key;
        this.byId = new CommandTimeseries<>(key, loader);
        this.byExecuteAt = new CommandTimeseries<>(key, loader);
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return key.equals(that.key)
               && byId.equals(that.byId)
               && byExecuteAt.equals(that.byExecuteAt);
    }

    @Override
    public final int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public Key key()
    {
        return key;
    }

    @Override
    public CommandTimeseries<?> byId()
    {
        return byId;
    }

    @Override
    public CommandTimeseries<?> byExecuteAt()
    {
        return byExecuteAt;
    }

    public <D> CommandsForKey update(CommandTimeseries.Update<TxnId, D> byId,
                                     CommandTimeseries.Update<Timestamp, D> byExecuteAt)
    {
        if (byId.isEmpty() && byExecuteAt.isEmpty())
            return this;

        return new CommandsForKey(key, byId.apply((CommandTimeseries<D>) this.byId), byExecuteAt.apply((CommandTimeseries<D>) this.byExecuteAt));
    }

    public boolean hasRedundant(TxnId redundantBefore)
    {
        return byId.minTimestamp().compareTo(redundantBefore) < 0;
    }

    public CommandsForKey withoutRedundant(TxnId redundantBefore)
    {
        Timestamp removeExecuteAt = byId.maxExecuteAtBefore(redundantBefore);

        return new CommandsForKey(key,
                                  (CommandTimeseries)byId.unbuild().removeBefore(redundantBefore).build(),
                                  (CommandTimeseries)byExecuteAt.unbuild().removeBefore(removeExecuteAt).build()
                                 );
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<TxnId> consumer)
    {
        byId.between(minTs, maxTs, status -> status.hasBeen(PreAccepted)).forEach(consumer);
        byExecuteAt.between(minTs, maxTs, status -> status.hasBeen(PreCommitted)).forEach(consumer);
    }

    public interface Update {}

}
