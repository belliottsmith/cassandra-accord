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

import accord.api.Journal;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Txn.Kind.Write;

public abstract class AbstractLoader implements Journal.Loader
{
    protected void maybeApplyWrites(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        Command command = safeCommand.current();
        if (command.saveStatus().compareTo(SaveStatus.Stable) >= 0 && command.saveStatus().compareTo(PreApplied) <= 0)
        {
            if (Commands.maybeExecute(safeStore, safeCommand, command, true, true))
                return;
        }
        else if (command.saveStatus().compareTo(Applying) >= 0 && command.saveStatus().compareTo(TruncatedApplyWithOutcome) <= 0)
        {
            if (command.txnId().is(Write))
            {
                CommandStore unsafeStore = safeStore.commandStore();
                Command.Executed executed = command.asExecuted();
                Participants<?> executes = executed.participants().stillExecutes();
                if (!executes.isEmpty())
                {
                    command.writes()
                           .apply(safeStore, executes, command.partialTxn())
                           .invoke(() -> unsafeStore.build(txnId, ss -> {
                               Commands.postApply(ss, txnId, -1, true);
                           }))
                           .begin(safeStore.agent());
                    return;
                }
            }
            else Invariants.expect(command.hasBeen(Applied));
        }
        safeCommand.update(safeStore, command, true);
        safeStore.notifyListeners(safeCommand, null);
    }
}
