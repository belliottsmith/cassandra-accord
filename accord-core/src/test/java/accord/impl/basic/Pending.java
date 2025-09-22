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

package accord.impl.basic;

import accord.utils.Invariants;

public interface Pending
{
    Pending origin();

    // a slightly hacky but easy way to track transitive recurring tasks
    class Global
    {
        public static final Pending NONE = () -> null;
        private static final ThreadLocal<Pending> activeOrigin = new ThreadLocal<>();

        public static void setActiveOrigin(Pending newActive)
        {
            Invariants.require(activeOrigin.get() == null);
            activeOrigin.set(newActive.origin());
            Invariants.require(activeOrigin.get() != null);
        }

        public static void unsafeSetActiveOrigin(Pending newActiveOrigin)
        {
            activeOrigin.set(newActiveOrigin);
        }

        public static void clearActiveOrigin()
        {
            activeOrigin.set(null);
        }

        public static void setNoActiveOrigin()
        {
            activeOrigin.set(NONE);
        }

        public static Pending activeOrigin()
        {
            return Invariants.nonNull(activeOrigin.get());
        }
    }
}
