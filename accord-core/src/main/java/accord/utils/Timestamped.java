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

import java.util.function.BiPredicate;
import java.util.function.Function;

import accord.primitives.Timestamp;

public class Timestamped<T>
{
    public final Timestamp timestamp;
    public final T data;
    private final Function<? super T, ? extends String> toString;

    public Timestamped(Timestamp timestamp, T data, Function<? super T, ? extends String> toString)
    {
        this.timestamp = timestamp;
        this.data = data;
        this.toString = toString;
    }

    public static <T> Timestamped<T> merge(Timestamped<T> a, Timestamped<T> b)
    {
        return a.timestamp.compareTo(b.timestamp) < 0 ? b : a;
    }

    public static <T> Timestamped<T> merge(Timestamped<T> a, Timestamped<T> b, BiPredicate<T, T> testPrefix, BiPredicate<T, T> testEquality)
    {
        int c = a.timestamp.compareTo(b.timestamp);
        int c2 = Long.compare(a.timestamp.uniqueHlc(), b.timestamp.uniqueHlc());
        Invariants.checkState(normaliseCmp(c) == normaliseCmp(c2));
        validateCmp(c, a, b, testPrefix, testEquality, true);
        validateCmp(c2, a, b, testPrefix, testEquality, true);
        return c >= 0 ? a : b;
    }

    private static int normaliseCmp(int c)
    {
        return c < 0 ? -1 : c > 0 ? 1 : 0;
    }

    public static <T> Timestamped<T> mergeNew(Timestamped<T> a, Timestamped<T> b, BiPredicate<T, T> testPrefix, BiPredicate<T, T> testEquality)
    {
        int c = a.timestamp.compareTo(b.timestamp);
        validateCmp(c, a, b, testPrefix, testEquality, false);
        validateCmp(Long.compare(a.timestamp.uniqueHlc(), b.timestamp.uniqueHlc()), a, b, testPrefix, testEquality, false);
        return c >= 0 ? a : b;
    }

    private static <T> void validateCmp(int cmp, Timestamped<T> a, Timestamped<T> b, BiPredicate<T, T> testPrefix, BiPredicate<T, T> testEquality, boolean permitBackwards)
    {
        if (cmp == 0) Invariants.checkArgument(testEquality.test(a.data, b.data), "%s != %s", a, b);
        else if (cmp < 0) Invariants.checkArgument(testPrefix.test(a.data, b.data), "%s >= %s", a, b);
        else if (permitBackwards) Invariants.checkArgument(testPrefix.test(b.data, a.data), "%s <= %s", a, b);
        else Invariants.illegalState("Trying to write old data %s < %s", b, a);
    }

    public static <T> Timestamped<T> mergeEqual(Timestamped<T> a, Timestamped<T> b, BiPredicate<T, T> testEquality)
    {
        int c = a.timestamp.compareTo(b.timestamp);
        Invariants.checkState(c == 0 && testEquality.test(a.data, b.data), "%s != %s", a, b);
        return a;
    }

    @Override
    public String toString()
    {
        return toString.apply(data);
    }

    public boolean equals(Timestamped<T> that, BiPredicate<T, T> equality)
    {
        return this.timestamp.equals(that.timestamp) && equality.test(this.data, that.data);
    }
}
