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

import accord.api.Key;
import accord.local.Command;
import accord.impl.CommandsForKey;
import accord.impl.PreExecuteContext;
import accord.primitives.Keys;
import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import java.util.HashMap;
import java.util.Map;

public class TestContext implements PreExecuteContext
{
    private final Map<TxnId, Command> commands = new HashMap<>();
    private final Map<RoutableKey, CommandsForKey> commandsForKey = new HashMap<>();

    public void add(Command command)
    {
        commands.put(command.txnId(), command);
    }

    public void addEmpty(TxnId txnId)
    {
        commands.put(txnId, Command.EMPTY);
    }

    public void addTxnIds(Iterable<TxnId> txnIds)
    {
        txnIds.forEach(this::addEmpty);
    }

    public void add(CommandsForKey cfk)
    {
        commandsForKey.put(cfk.key(), cfk);
    }

    public void addEmpty(RoutableKey key)
    {
        commandsForKey.put(key, CommandsForKey.EMPTY);
    }

    public void addKeys(Iterable<RoutableKey> keys)
    {
        keys.forEach(this::addEmpty);
    }

    public void addKeys(Seekables<?, ?> keys)
    {
        keys.forEach(k -> addEmpty((RoutableKey) k));
    }

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
        return commands.keySet();
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.of(commandsForKey.keySet(), rk -> (Key) rk);
    }
}
