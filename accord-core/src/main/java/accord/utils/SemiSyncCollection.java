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

package accord.utils;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

public abstract class SemiSyncCollection<V, E, ES> extends SemiSyncValue<V, SemiSyncCollection.Edit<E>>
{
    protected static class Edit<E> extends SemiSyncValue.Edit<Edit<E>>
    {
        public final Object group;
        public final E with;
        public final E replace;

        public Edit(Object group, E with, E replace)
        {
            this.group = group;
            this.with = with;
            this.replace = replace;
        }

        @Nonnull
        @Override
        protected Object group()
        {
            return group;
        }

        @Override
        protected Edit<E> merge(Edit<E> next)
        {
            return new Edit<>(next.group, next.with, replace);
        }
    }

    protected abstract ES merge(List<E> es);
    protected abstract V applyOne(V value, E add, E remove);
    protected abstract V applyMultiple(V value, ES add, ES remove);

    protected V merge(V value, Edit<E> edit)
    {
        return applyOne(value, edit.with, edit.replace);
    }

    protected V merge(V value, Collection<Edit<E>> edits)
    {
        ES add, remove;
        try (ArrayBuffers.BufferList<E> adds = new ArrayBuffers.BufferList<>();
             ArrayBuffers.BufferList<E> removes = new ArrayBuffers.BufferList<>())
        {
            for (Edit<E> edit : edits)
            {
                if (edit.with != null)
                    adds.add(edit.with);
                if (edit.replace != null)
                    removes.add(edit.replace);
            }
            add = merge(adds);
            remove = merge(removes);
        }
        return applyMultiple(value, add, remove);
    }

    protected SemiSyncCollection(V initialValue)
    {
        super(initialValue);
    }
}
