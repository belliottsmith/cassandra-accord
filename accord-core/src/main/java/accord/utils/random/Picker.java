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

package accord.utils.random;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.utils.Invariants;
import accord.utils.RandomSource;

public class Picker
{
    public enum Distribution
    {
        UNIFORM, WEIGHTED;

        public IntPicker get(RandomSource random, int[] values)
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled distribution: " + this);
                case UNIFORM: return new UniformIntPicker(random, values);
                case WEIGHTED: return new WeightedIntPicker(random, values);
            }
        }

        public LongPicker get(RandomSource random, long[] values)
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled distribution: " + this);
                case UNIFORM: return new UniformLongPicker(random, values);
                case WEIGHTED: return new WeightedLongPicker(random, values);
            }
        }

        public <T> ObjectPicker<T> get(RandomSource random, T[] values)
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled distribution: " + this);
                case UNIFORM: return new UniformObjectPicker<>(random, values);
                case WEIGHTED: return new WeightedObjectPicker<>(random, values);
            }
        }
    }

    public interface IntPicker extends IntSupplier
    {
        int pick();
        void cycle();

        @Override
        default int getAsInt()
        {
            return pick();
        }
    }

    public interface LongPicker extends LongSupplier
    {
        long pick();
        void cycle();

        @Override
        default long getAsLong()
        {
            return pick();
        }
    }

    public interface ObjectPicker<T> extends Supplier<T>
    {
        T pick();
        void cycle();

        @Override
        default T get()
        {
            return pick();
        }
    }

    final RandomSource random;

    public Picker(RandomSource random)
    {
        this.random = random;
    }

    public static class UniformIntPicker extends Picker implements IntPicker
    {
        final int[] values;

        public UniformIntPicker(RandomSource random, int[] values)
        {
            super(random);
            this.values = Invariants.checkArgument(values, values.length > 0);
        }

        @Override
        public int pick()
        {
            return values[random.nextInt(values.length)];
        }
    }

    public static class UniformLongPicker extends Picker implements LongPicker
    {
        final long[] values;

        public UniformLongPicker(RandomSource random, long[] values)
        {
            super(random);
            this.values = Invariants.checkArgument(values, values.length > 0);
        }

        @Override
        public long pick()
        {
            return values[random.nextInt(values.length)];
        }
    }

    public static class UniformObjectPicker<T> extends Picker implements ObjectPicker<T>
    {
        final T[] values;

        public UniformObjectPicker(RandomSource random, T[] values)
        {
            super(random);
            this.values = Invariants.checkArgument(values, values.length > 0);
        }

        @Override
        public T pick()
        {
            return values[random.nextInt(values.length)];
        }
    }

    static abstract class Weighted extends Picker
    {
        final float[] weights;

        public Weighted(RandomSource random, int length)
        {
            super(random);
            this.weights = new float[length - 1];
            cycle();
        }

        public void cycle()
        {
            float sum = 0;
            for (int i = 0 ; i < weights.length ; ++i)
                weights[i] = sum += random.nextFloat();
            sum += random.nextFloat();
            for (int i = 0 ; i < weights.length ; ++i)
                weights[i] /= sum;
        }

        int pickIndex()
        {
            int i = Arrays.binarySearch(weights, random.nextFloat());
            if (i < 0) i = -1 - i;
            return i;
        }
    }

    public static class WeightedIntPicker extends Weighted implements IntPicker
    {
        final int[] values;

        public WeightedIntPicker(RandomSource random, int[] values)
        {
            super(random, values.length);
            this.values = values;
        }

        @Override
        public int pick()
        {
            return values[pickIndex()];
        }
    }

    public static class WeightedLongPicker extends Weighted implements LongPicker
    {
        final long[] values;

        public WeightedLongPicker(RandomSource random, long[] values)
        {
            super(random, values.length);
            this.values = values;
        }

        @Override
        public long pick()
        {
            return values[pickIndex()];
        }
    }

    public static class WeightedObjectPicker<T> extends Weighted implements ObjectPicker<T>
    {
        final T[] values;

        public WeightedObjectPicker(RandomSource random, T[] values)
        {
            super(random, values.length);
            this.values = values;
        }

        @Override
        public T pick()
        {
            return values[pickIndex()];
        }
    }

    public void cycle()
    {
    }

    public int pick(int first, int second, int... rest)
    {
        int offset = random.nextInt(0, rest.length + 2);
        switch (offset)
        {
            case 0:  return first;
            case 1:  return second;
            default: return rest[offset - 2];
        }
    }

    public int pick(int[] array)
    {
        return pick(array, 0, array.length);
    }

    public int pick(int[] array, int offset, int length)
    {
        Invariants.checkIndexInBounds(array.length, offset, length);
        if (length == 1)
            return array[offset];
        return array[random.nextInt(offset, offset + length)];
    }

    public long pick(long first, long second, long... rest)
    {
        int offset = random.nextInt(0, rest.length + 2);
        switch (offset)
        {
            case 0:  return first;
            case 1:  return second;
            default: return rest[offset - 2];
        }
    }

    public long pick(long[] array)
    {
        return pick(array, 0, array.length);
    }

    public long pick(long[] array, int offset, int length)
    {
        Invariants.checkIndexInBounds(array.length, offset, length);
        if (length == 1)
            return array[offset];
        return array[random.nextInt(offset, offset + length)];
    }

    public <T> T pick(T first, T second, T... rest)
    {
        int offset = random.nextInt(0, rest.length + 2);
        switch (offset)
        {
            case 0:  return first;
            case 1:  return second;
            default: return rest[offset - 2];
        }
    }

    public <T> T pick(List<T> values)
    {
        return pick(values, 0, values.size());
    }

    public <T> T pick(List<T> values, int offset, int length)
    {
        Invariants.checkIndexInBounds(values.size(), offset, length);
        if (length == 1)
            return values.get(offset);
        return values.get(random.nextInt(offset, offset + length));
    }
}
