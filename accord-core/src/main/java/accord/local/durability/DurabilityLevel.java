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

package accord.local.durability;

import java.util.Objects;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncRemote.NoRemote;

public class DurabilityLevel
{
    public static final DurabilityLevel NONE = new DurabilityLevel(NoLocal, NoRemote, null, null);

    public final SyncLocal local;
    public final SyncRemote remote;
    public final @Nullable SortedArrayList<Node.Id> including;
    public final @Nullable SortedArrayList<Node.Id> excluding;

    public DurabilityLevel(SyncLocal local, SyncRemote remote, @Nullable SortedArrayList<Node.Id> including)
    {
        this(local, remote, including, null);
    }

    public DurabilityLevel(SyncLocal local, SyncRemote remote, @Nullable SortedArrayList<Node.Id> including, @Nullable SortedArrayList<Node.Id> excluding)
    {
        this.local = local;
        this.remote = remote;
        this.including = including;
        this.excluding = excluding;
    }

    public boolean equals(Object that)
    {
        return that instanceof DurabilityLevel && equals((DurabilityLevel) that);
    }

    private boolean equals(DurabilityLevel that)
    {
        return this.local == that.local
               && this.remote == that.remote
               && Objects.equals(this.including, that.including)
               && Objects.equals(this.excluding, that.excluding);
    }

    @Override
    public String toString()
    {
        return "{" +
               "local=" + local +
               ", remote=" + remote +
               ", including=" + including +
               ", excluding=" + excluding +
               '}';
    }

    public static DurabilityLevel min(DurabilityLevel a, DurabilityLevel b)
    {
        SyncLocal local = min(a.local, b.local);
        SyncRemote remote = min(a.remote, b.remote);
        SortedArrayList<Node.Id> including = union(a.including, b.including);
        SortedArrayList<Node.Id> excluding = union(a.excluding, b.excluding);
        if (including != null && excluding != null)
            including = including.without(excluding);
        return new DurabilityLevel(local, remote, including, excluding);
    }

    public static DurabilityLevel max(DurabilityLevel a, DurabilityLevel b)
    {
        SyncLocal local = max(a.local, b.local);
        SyncRemote remote = max(a.remote, b.remote);
        SortedArrayList<Node.Id> including = union(a.including, b.including);
        SortedArrayList<Node.Id> excluding = subtract(a.excluding, b.excluding);
        if (including != null && excluding != null)
            including = including.without(excluding);
        return new DurabilityLevel(local, remote, including, excluding);
    }

    private static SortedArrayList<Node.Id> union(SortedArrayList<Node.Id> a, SortedArrayList<Node.Id> b)
    {
        if (a == null || b == null)
        {
            if (a == null && b == null)
                return null;
            return a == null ? b : a;
        }
        return a.with(b);
    }

    private static SortedArrayList<Node.Id> subtract(SortedArrayList<Node.Id> a, SortedArrayList<Node.Id> b)
    {
        if (a == null)
            return null;
        if (b == null)
            return a;
        return a.without(b);
    }

    private static <E extends Enum<E>> E min(E a, E b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static <E extends Enum<E>> E max(E a, E b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    public boolean isSatisfiedBy(DurabilityLevel satisfies)
    {
        if (satisfies.local.compareTo(local) < 0 || satisfies.remote.compareTo(remote) < 0)
            return false;

        if (including == null)
            return true;

        return satisfies.including != null && satisfies.including.containsAll(including);
    }
}
