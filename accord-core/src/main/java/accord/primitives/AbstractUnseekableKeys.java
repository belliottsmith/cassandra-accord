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

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.SortedArrays;

import java.util.Arrays;

import static accord.primitives.Routable.Kind.UnseekableKey;
import static accord.utils.ArrayBuffers.cachedRoutingKeys;

// TODO: do we need this class?
public abstract class AbstractUnseekableKeys extends AbstractKeys<RoutingKey>
implements Iterable<RoutingKey>, Unseekables<RoutingKey>, Participants<RoutingKey>
{
    AbstractUnseekableKeys(RoutingKey[] keys)
    {
        super(keys);
    }

    @Override
    public final Routable.Kind domainKind()
    {
        return UnseekableKey;
    }

    @Override
    public final int indexOf(RoutingKey key)
    {
        return Arrays.binarySearch(keys, key);
    }

    @Override
    public final boolean contains(RoutingKey key)
    {
        return Arrays.binarySearch(keys, key) >= 0;
    }

    @Override
    public final boolean contains(Key key)
    {
        return Arrays.binarySearch(keys, key, RoutableKey::compareAsRoutingKey) >= 0;
    }

    @Override
    public final int find(RoutingKey key, SortedArrays.Search search)
    {
        return SortedArrays.binarySearch(keys, 0, keys.length, key, RoutingKey::compareTo, search);
    }

    @Override
    public final int findNext(int thisIndex, RoutingKey key, SortedArrays.Search search)
    {
        return SortedArrays.exponentialSearch(keys, thisIndex, keys.length, key, RoutingKey::compareTo, search);
    }

    @Override
    public long findNextIntersection(int thisIndex, Keys with, int withIndex)
    {
        return SortedArrays.findNextIntersection(this.keys, thisIndex, with.keys, withIndex, RoutableKey::compareAsRoutingKey);
    }

    @Override
    public final boolean containsAll(AbstractUnseekableKeys that)
    {
        return containsAllSameKind(that);
    }

    @Override
    public final boolean containsAll(Keys that)
    {
        return that.size() == SortedArrays.foldlIntersection(0, RoutableKey::compareAsRoutingKey, keys, 0, keys.length, that.keys, 0, that.keys.length, (k, p, v, l, r) -> v + 1, 0, 0, 0);
    }

    @Override
    public boolean containsAll(AbstractRanges ranges)
    {
        // TODO (desired): something more efficient
        return toRanges().containsAll(ranges);
    }

    @Override
    public AbstractUnseekableKeys intersecting(Unseekables<?> intersecting, Slice slice)
    {
        return intersecting(intersecting);
    }

    @Override
    public AbstractUnseekableKeys intersecting(Unseekables<?> intersecting)
    {
        switch (intersecting.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + intersecting.domain());
            case Key:
            {
                AbstractUnseekableKeys that = (AbstractUnseekableKeys) intersecting;
                return weakWrap(intersecting(that, cachedRoutingKeys()), that);
            }
            case Range:
            {
                AbstractRanges that = (AbstractRanges) intersecting;
                return wrap(intersecting(that, cachedRoutingKeys()));
            }
        }
    }

    @Override
    public AbstractUnseekableKeys intersecting(Seekables<?, ?> intersecting)
    {
        switch (intersecting.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + intersecting.domain());
            case Key:
            {
                Keys that = (Keys) intersecting;
                return wrap(intersecting((k, k2) -> -k2.compareAsRoutingKey(k), that, cachedRoutingKeys()));
            }
            case Range:
            {
                AbstractRanges that = (AbstractRanges) intersecting;
                return wrap(intersecting(that, cachedRoutingKeys()));
            }
        }
    }

    @Override
    public AbstractUnseekableKeys intersecting(Seekables<?, ?> intersecting, Slice slice)
    {
        return intersecting(intersecting);
    }

    @Override
    public Participants<RoutingKey> without(Unseekables<?> keysOrRanges)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Key:
            {
                AbstractUnseekableKeys that = (AbstractUnseekableKeys) keysOrRanges;
                return weakWrap(SortedArrays.linearSubtract(this.keys, that.keys, cachedRoutingKeys()), that);
            }
            case Range:
            {
                return without((AbstractRanges)keysOrRanges);
            }
        }
    }

    @Override
    public Participants<RoutingKey> without(Ranges ranges)
    {
        return without((AbstractRanges) ranges);
    }

    private Participants<RoutingKey> without(AbstractRanges ranges)
    {
        RoutingKey[] output = subtract(ranges, keys, RoutingKey[]::new);
        return output == keys ? this : new RoutingKeys(output);
    }

    public Ranges toRanges()
    {
        Range[] ranges = new Range[keys.length];
        for (int i = 0 ; i < keys.length ; ++i)
            ranges[i] = keys[i].asRange();
        return Ranges.ofSortedAndDeoverlapped(ranges);
    }

    private AbstractUnseekableKeys weakWrap(RoutingKey[] wrap, AbstractUnseekableKeys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new RoutingKeys(wrap);
    }

    private AbstractUnseekableKeys wrap(RoutingKey[] wrap)
    {
        return wrap == keys ? this : new RoutingKeys(wrap);
    }
}
