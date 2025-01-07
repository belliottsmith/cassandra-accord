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

import java.util.function.BiConsumer;

import accord.api.Journal;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.Commands;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;

import static accord.local.Cleanup.Input.FULL;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;

public abstract class AbstractLoader implements Journal.Loader
{
    protected Command loadInternal(Command command, SafeCommandStore safeStore)
    {
        TxnId txnId = command.txnId();
        if (command.status() != Truncated && command.status() != Invalidated)
        {
            Cleanup cleanup = Cleanup.shouldCleanup(FULL, safeStore, command, command.participants());
            switch (cleanup)
            {
                case NO:
                    break;
                case INVALIDATE:
                case TRUNCATE_WITH_OUTCOME:
                case TRUNCATE:
                case ERASE:
                    command = Commands.purge(safeStore, command, cleanup);
            }
        }

        return safeStore.unsafeGetNoCleanup(txnId).update(safeStore, command);
    }

    protected void applyWrites(TxnId txnId, SafeCommandStore safeStore, BiConsumer<SafeCommand, Command> apply)
    {
        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        Command command = safeCommand.current();
        if (command.is(Stable) || command.saveStatus() == PreApplied)
        {
            Commands.maybeExecute(safeStore, safeCommand, command, true, true);
        }
        else if (command.saveStatus().compareTo(Applying) >= 0 && !command.hasBeen(Truncated))
        {
            apply.accept(safeCommand, command);
        }
    }
}
