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

package accord.coordinate;

import accord.utils.TinyEnumSet;

public enum ExecuteFlag
{
    READY_TO_EXECUTE, HAS_UNIQUE_HLC;

    public static final class ExecuteFlags extends TinyEnumSet<ExecuteFlag>
    {
        private static final ExecuteFlags[] LOOKUP = new ExecuteFlags[1 << ExecuteFlag.values().length];
        static
        {
            for (int i = 0 ; i < LOOKUP.length ; ++i)
                LOOKUP[i] = new ExecuteFlags(i);
        }
        public static ExecuteFlags none() { return LOOKUP[0]; }
        public static ExecuteFlags all() { return LOOKUP[LOOKUP.length - 1]; }
        public static ExecuteFlags get(int bits) { return LOOKUP[bits]; }
        public static ExecuteFlags get(ExecuteFlag a) { return LOOKUP[encode(a)]; }
        public static ExecuteFlags get(ExecuteFlag a, ExecuteFlag b) { return LOOKUP[encode(a) | encode(b)]; }
        public ExecuteFlags with(ExecuteFlag a) { return LOOKUP[bitset | encode(a)]; }
        public ExecuteFlags or(ExecuteFlags that) { return LOOKUP[this.bitset | that.bitset]; }
        public ExecuteFlags and(ExecuteFlags that) { return LOOKUP[this.bitset & that.bitset]; }
        public boolean isEmpty() { return bitset == 0; }
        public int bits() { return bitset; }
        private ExecuteFlags(int bits) { super(bits); }
    }
}
