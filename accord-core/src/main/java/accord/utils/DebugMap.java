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

import java.util.List;

import com.google.common.collect.Lists;

import accord.coordinate.Timeout;
import accord.local.Node;

public class DebugMap extends SortedListMap<Node.Id, Object>
{
    private static final Timeout timeoutInstance = Timeout.unsafeTimeout(null, null);

    public DebugMap(SortedList<Node.Id> list)
    {
        super(list, Object[]::new);
    }

    public void debug(Node.Id from, Object reply)
    {
        if (reply == null)
            reply = timeoutInstance;
        merge(from, reply, (a, b) -> a instanceof List<?> ? ((List<Object>) a).add(b) : Lists.newArrayList(a, b));
    }
}
