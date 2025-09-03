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

public final class SimpleBitSets
{
    private SimpleBitSets() {}

    public static AllSetSimpleBitSet allSet()
    {
        return AllSetSimpleBitSet.instance;
    }

    public static AllUnsetSimpleBitSet allUnset()
    {
        return AllUnsetSimpleBitSet.instance;
    }

    private static class AbstractAllSimpleBitSet implements SimpleBitSet
    {
        private final boolean set;

        private AbstractAllSimpleBitSet(boolean set)
        {
            this.set = set;
        }

        @Override
        public boolean get(int i)
        {
            return set;
        }

        @Override
        public boolean set(int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean unset(int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty()
        {
            return !set;
        }

        @Override
        public int nextSetBit(int fromIndex)
        {
            return set ? fromIndex : -1;
        }
    }

    public static class AllSetSimpleBitSet extends AbstractAllSimpleBitSet
    {
        public static final AllSetSimpleBitSet instance = new AllSetSimpleBitSet();

        private AllSetSimpleBitSet()
        {
            super(true);
        }
    }

    public static class AllUnsetSimpleBitSet extends AbstractAllSimpleBitSet
    {
        public static final AllUnsetSimpleBitSet instance = new AllUnsetSimpleBitSet();

        private AllUnsetSimpleBitSet()
        {
            super(false);
        }
    }
}
