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

import accord.impl.CommandTimeseries.CommandLoader;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import java.util.Objects;

/**
 * Contains updates for a single commands for key dataset (ie: deps OR all)
 * @param <D>
 */
public abstract class CommandsForKeyUpdater<D>
{
    public abstract CommandTimeseries.Update<TxnId, D> byId();

    public abstract CommandTimeseries.Update<Timestamp, D> byExecuteAt();

    public static class Mutable<D> extends CommandsForKeyUpdater<D>
    {
        private final CommandTimeseries.MutableUpdate<TxnId, D> byId;
        private final CommandTimeseries.MutableUpdate<Timestamp, D> byExecuteAt;

        Mutable(CommandTimeseries.MutableUpdate<TxnId, D> byId, CommandTimeseries.MutableUpdate<Timestamp, D> byExecuteAt)
        {
            this.byId = byId;
            this.byExecuteAt = byExecuteAt;
        }

        public Mutable(CommandLoader<D> loader)
        {
            this(new CommandTimeseries.MutableUpdate<>(loader), new CommandTimeseries.MutableUpdate<>(loader));
        }

        @Override public CommandTimeseries.MutableUpdate<TxnId, D> byId() { return byId; }
        @Override public CommandTimeseries.MutableUpdate<Timestamp, D> byExecuteAt() { return byExecuteAt; }

        public Immutable<D> toImmutable()
        {
            return new Immutable<>(byId.toImmutable(), byExecuteAt.toImmutable());
        }
    }

    public static class Immutable<D> extends CommandsForKeyUpdater<D>
    {
        private static final Immutable<Object> EMPTY = new Immutable<>(CommandTimeseries.ImmutableUpdate.empty(), CommandTimeseries.ImmutableUpdate.empty());
        private final CommandTimeseries.ImmutableUpdate<TxnId, D> byId;
        private final CommandTimeseries.ImmutableUpdate<Timestamp, D> byExecuteAt;

        public Immutable(CommandTimeseries.ImmutableUpdate<TxnId, D> byId, CommandTimeseries.ImmutableUpdate<Timestamp, D> byExecuteAt)
        {
            this.byId = byId;
            this.byExecuteAt = byExecuteAt;
        }

        @Override
        public String toString()
        {
            return "Immutable{" +
                    "byId=" + byId +
                    ", byExecuteAt=" + byExecuteAt +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Immutable<?> immutable = (Immutable<?>) o;
            return Objects.equals(byId, immutable.byId) && Objects.equals(byExecuteAt, immutable.byExecuteAt);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(byId, byExecuteAt);
        }

        public static <D> Immutable<D> empty()
        {
            return (Immutable<D>) EMPTY;
        }

        @Override public CommandTimeseries.ImmutableUpdate<TxnId, D> byId() { return byId; }
        @Override public CommandTimeseries.ImmutableUpdate<Timestamp, D> byExecuteAt() { return byExecuteAt; }

        public Mutable<D> toMutable(CommandLoader<D> loader)
        {
            return new Mutable<>(byId.toMutable(loader), byExecuteAt.toMutable(loader));
        }
    }

    public int totalChanges()
    {
        return byId().numChanges() + byExecuteAt().numChanges();
    }

    public boolean isEmpty()
    {
        return byId().isEmpty() && byExecuteAt().isEmpty();
    }

    public CommandsForKey apply(CommandsForKey current)
    {
        if (byId().isEmpty() && byExecuteAt().isEmpty())
            return current;
        return current.update(byId(), byExecuteAt());
    }
}
