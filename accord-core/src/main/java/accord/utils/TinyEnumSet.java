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

import java.util.function.IntFunction;

public class TinyEnumSet<E extends Enum<E>>
{
    protected final int bitset;

    public TinyEnumSet(Enum<E> ... values)
    {
        this.bitset = encode(values);
    }

    protected TinyEnumSet(int bitset)
    {
        this.bitset = bitset;
    }

    public static <E extends Enum<E>> int encode(Enum<E> ... values)
    {
        int bitset = 0;
        for (Enum<E> v : values)
        {
            Invariants.requireArgument(v.ordinal() < 32);
            bitset |= 1 << v.ordinal();
        }
        return bitset;
    }

    public static <E extends Enum<E>> int encode(Enum<E> value)
    {
        return 1 << value.ordinal();
    }

    public static <E extends Enum<E>> boolean contains(int bitset, E value)
    {
        return contains(bitset, value.ordinal());
    }

    public static boolean contains(int bitset, int ordinal)
    {
        return 0 != (bitset & (1 << ordinal));
    }

    public boolean contains(E value)
    {
        return contains(bitset, value.ordinal());
    }

    public boolean contains(int ordinal)
    {
        return contains(bitset, ordinal);
    }

    public TinyEnumSet<E> or(TinyEnumSet<E> or)
    {
        return or(this, or, TinyEnumSet::new);
    }

    public static <S extends TinyEnumSet<?>> S or(S a, S b, IntFunction<S> constructor)
    {
        int newBitset = a.bitset | b.bitset;
        return newBitset == a.bitset ? a : newBitset == b.bitset ? b : constructor.apply(newBitset);
    }

    public boolean test(E kind)
    {
        return testOrdinal(kind.ordinal());
    }

    public boolean testOrdinal(int ordinal)
    {
        return 0 != (bitset & (1 << ordinal));
    }
}

