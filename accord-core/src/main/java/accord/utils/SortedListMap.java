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

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public class SortedListMap<K extends Comparable<? super K>, V> extends AbstractMap<K, V>
{
    final SortedList<K> list;
    final Object[] values;
    int size;

    public SortedListMap(SortedList<K> list, IntFunction<V[]> allocator)
    {
        this.list = list;
        this.values = allocator.apply(list.size());
    }

    public SortedListMap(SortedList<K> list, Object[] values, int size)
    {
        Invariants.require(values.length == list.size());
        this.list = list;
        this.values = values;
        this.size = size;
    }

    @Override
    public V get(Object key)
    {
        int i = list.find((K)key);
        if (i < 0) return null;
        return (V)values[i];
    }

    @Override
    public boolean containsValue(Object value)
    {
        for (Object v : values)
        {
            if (v == value) return true;
        }
        return false;
    }

    @Override
    public boolean containsKey(Object key)
    {
        int index = list.indexOf((K)key);
        return index >= 0 && values[index] != null;
    }

    public V putAtIndex(int index, V value)
    {
        V prev = (V)values[index];
        values[index] = value;
        if (prev == null)
            ++size;
        return prev;
    }

    @Override
    public V put(K key, V value)
    {
        Invariants.requireArgument(value != null);
        int i = list.find(key);
        if (i < 0)
            throw new IllegalArgumentException(key + " is not in the SortedList of keys");
        return putAtIndex(i, value);
    }

    @Override
    public V remove(Object key)
    {
        int i = list.find((K)key);
        if (i < 0) return null;
        V prev = (V) values[i];
        values[i] = null;
        return prev;
    }

    @Override
    public SortedList<K> keySet()
    {
        return list;
    }

    @Override
    public Collection<V> values()
    {
        return new CollectionView<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return new Iter<V>()
                {
                    @Override
                    V get(K key, V value)
                    {
                        return value;
                    }
                };
            }

            @Override
            public Stream<V> stream()
            {
                return (Stream<V>) Arrays.stream(values).filter(Objects::nonNull);
            }
        };
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet()
    {
        return new SetView<Entry<K, V>>()
        {
            @Override
            public Iterator<Entry<K, V>> iterator()
            {
                return new Iter<Map.Entry<K, V>>()
                {
                    @Override
                    Map.Entry<K, V> get(K key, V value)
                    {
                        return new AbstractMap.SimpleImmutableEntry<>(key, value);
                    }
                };
            }
        };
    }

    abstract public class SetView<T> extends AbstractSet<T>
    {
        @Override
        public int size()
        {
            return size;
        }
    }

    abstract public class CollectionView<T> extends AbstractCollection<T>
    {
        @Override
        public int size()
        {
            return size;
        }
    }

    abstract class Iter<T> implements Iterator<T>
    {
        int cur = -1, next = -1;

        abstract T get(K key, V value);

        @Override
        public void remove()
        {
            if (values[cur] != null)
            {
                --size;
                values[cur] = null;
            }
        }

        @Override
        public boolean hasNext()
        {
            while (++cur < values.length)
            {
                if (values[cur] != null)
                {
                    next = cur;
                    return true;
                }
            }
            return false;
        }

        @Override
        public T next()
        {
            T result = get(list.get(next), (V)values[next]);
            next = -1;
            return result;
        }
    }

    public List<V> valuesAsNullableList()
    {
        return (List<V>)Arrays.asList(values);
    }

    public Stream<V> valuesAsNullableStream()
    {
        return (Stream<V>)Stream.of(values);
    }

    public int domainSize()
    {
        return list.size();
    }

    public SortedList<K> domain()
    {
        return list;
    }

    public K getKey(int keyIndex)
    {
        return list.get(keyIndex);
    }

    public V getValue(int keyIndex)
    {
        return (V) values[keyIndex];
    }

    public List<V> lazySelect(List<K> select)
    {
        return (List<V>)list.lazySelect(values, select);
    }

    public <O> O foldlValues(BiFunction<V, O, O> foldl, O zero)
    {
        return foldl((f, k, v, cur) -> f.apply(v, cur), foldl, zero);
    }

    public <O> O foldlNonNullValues(BiFunction<V, O, O> foldl, O zero)
    {
        return foldlNonNull((f, k, v, cur) -> f.apply(v, cur), foldl, zero);
    }

    public <O> O foldlNonNullValues(SortedList<K> subset, BiFunction<V, O, O> foldl, O zero)
    {
        if (subset.size() == domainSize())
            return foldlNonNullValues(foldl, zero);

        if (subset.isEmpty())
            return zero;

        int j = list.find(subset.get(0));
        Invariants.require(j >= 0, "%s is not a subset of %s", subset, list);

        O result = zero;
        if (values[j] != null)
            result = foldl.apply((V)values[j], result);

        for (int i = 1 ; i < subset.size() ; ++i)
        {
            j = list.findNext(j + 1, subset.get(i));
            Invariants.require(j >= 0, "%s is not a subset of %s", subset, list);
            if (values[j] != null)
                result = foldl.apply((V)values[j], result);
        }
        return result;
    }

    public long foldlNonNullValues(FoldToLong<V> foldl, long zero)
    {
        return Functions.foldlNonNull(values, foldl, zero);
    }

    public <O> O foldlNonNull(TriFunction<K, V, O, O> foldl, O zero)
    {
        return foldlNonNull(TriFunction::apply, foldl, zero);
    }

    public <O, P1> O foldlNonNull(QuadFunction<P1, K, V, O, O> foldl, P1 p1, O zero)
    {
        O result = zero;
        for (int i = 0 ; i < values.length ; ++i)
        {
            if (values[i] != null)
                result = foldl.apply(p1, list.get(i), (V)values[i], result);
        }
        return result;
    }

    public <O, P1> O foldl(QuadFunction<P1, K, V, O, O> foldl, P1 p1, O zero)
    {
        O result = zero;
        for (int i = 0 ; i < values.length ; ++i)
            result = foldl.apply(p1, list.get(i), (V)values[i], result);
        return result;
    }
}
