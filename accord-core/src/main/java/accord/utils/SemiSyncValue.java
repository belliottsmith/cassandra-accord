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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

/**
 * A value that may be updated asynchronously. Edits to the value are queued, and when prompted by the implementation
 * an attempt is made to exclusively drain any pending edits. Before reading the value any pending edits are
 * drained synchronously.
 *
 * Depending on the underlying collection once a read has been served it may be possible to update a collection
 * before the read is complete (i.e. if the collection is immutable/copy-on-write). It is up to the implementation
 * or user to ensure any mutual exclusivity required for correctness otherwise.
 */
public abstract class SemiSyncValue<V, E extends SemiSyncValue.Edit<E>> implements Runnable
{
    protected static abstract class Edit<E extends Edit<E>> extends IntrusiveStack<E>
    {
        static <E extends Edit<E>> boolean push(E edit, SemiSyncValue<?, E> owner)
        {
            return null == IntrusiveStack.getAndPush(pendingEditsUpdater, (SemiSyncValue)owner, (Edit)edit);
        }

        // may be some constant; if multiple pending edits, group them by this identity before applying
        protected abstract @Nonnull Object group();

        // merge two Edits for the same group
        protected abstract E merge(E next);
    }

    protected V value;
    protected volatile E pendingEdits;
    private final ReentrantLock drainPendingEditsLock = new ReentrantLock();
    private static final AtomicReferenceFieldUpdater<SemiSyncValue, Edit> pendingEditsUpdater = AtomicReferenceFieldUpdater.newUpdater(SemiSyncValue.class, Edit.class, "pendingEdits");

    /**
     * We have added an edit onto an empty queue; the implementation may request an immediate or asynchronous flush if helpful
     */
    protected void onNewEdits() {}

    /**
     * We have completed a drain and found more edits pending; the implementation may request an immediate or asynchronous flush if helpful
     */
    protected void onRemainingEdits() {}

    protected abstract V merge(V value, E edit);

    protected V merge(V value, Collection<E> edits)
    {
        for (E edit : edits)
            value = merge(value, edit);
        return value;
    }

    protected SemiSyncValue(V initialValue)
    {
        this.value = initialValue;
    }

    protected void pushEdit(E edit)
    {
        if (Edit.push(edit, this))
            onNewEdits();
    }

    @Override
    public void run()
    {
        tryDrainPendingEdits();
    }

    protected V get()
    {
        return drainPendingEdits();
    }

    protected boolean tryDrainPendingEdits()
    {
        if (!tryLock())
            return false;

        try
        {
            drainPendingEditsExclusive();
            return true;
        }
        finally
        {
            unlock();
        }
    }

    protected V drainPendingEdits()
    {
        lock();
        try
        {
            drainPendingEditsExclusive();
            return value;
        }
        finally
        {
            unlock();
        }
    }

    protected void drainPendingEditsExclusive()
    {
        E edits = (E) pendingEditsUpdater.getAndSet(this, null);
        if (edits == null)
            return;

        edits = edits.reverse();
        Map<Object, E> editMap = null;
        E pending = edits;
        Object pendingKey = pending.group();
        for (E next = edits.next; next != null ; next = next.next)
        {
            Object nextKey = next.group();
            if (!pendingKey.equals(nextKey))
            {
                if (editMap == null) editMap = new HashMap<>();
                editMap.merge(pendingKey, pending, Edit::merge);
                pending = next;
                pendingKey = nextKey;
            }
            else
            {
                pending = pending.merge(next);
            }
        }

        if (editMap == null)
        {
            value = merge(value, pending);
        }
        else
        {
            editMap.merge(pendingKey, pending, Edit::merge);
            value = merge(value, editMap.values());
        }
    }

    protected final void lock()
    {
        drainPendingEditsLock.lock();
    }

    protected final boolean tryLock()
    {
        return drainPendingEditsLock.tryLock();
    }

    protected final void unlock()
    {
        drainPendingEditsLock.unlock();
        postUnlock();
    }

    private void postUnlock()
    {
        if (pendingEdits != null && !drainPendingEditsLock.isHeldByCurrentThread())
            onRemainingEdits();
    }
}
