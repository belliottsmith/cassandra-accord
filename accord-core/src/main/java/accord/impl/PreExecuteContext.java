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

package accord.impl;

import accord.local.Command;
import accord.local.PreLoadContext;
import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import java.util.Map;

public interface PreExecuteContext extends PreLoadContext
{
    Map<TxnId, Command> commands();
    Map<RoutableKey, CommandsForKey> commandsForKey();

    static PreExecuteContext of(PreLoadContext preLoadContext, Map<TxnId, Command> commands, Map<RoutableKey, CommandsForKey> commandsForKey)
    {
        return new PreExecuteContext()
        {
            @Override
            public Map<TxnId, Command> commands()
            {
                return commands;
            }

            @Override
            public Map<RoutableKey, CommandsForKey> commandsForKey()
            {
                return commandsForKey;
            }

            @Override
            public Iterable<TxnId> txnIds()
            {
                return preLoadContext.txnIds();
            }

            @Override
            public Seekables<?, ?> keys()
            {
                return preLoadContext.keys();
            }
        };
    }
}
