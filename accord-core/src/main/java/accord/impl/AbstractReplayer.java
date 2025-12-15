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

import java.util.Objects;

import javax.annotation.Nullable;

import accord.api.Journal;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.Status.Applied;
import static accord.primitives.Txn.Kind.Write;

public abstract class AbstractReplayer implements Journal.Replayer
{
    public final RedundantBefore redundantBefore;
    public final TxnId minReplay;

    protected AbstractReplayer(CommandStore commandStore, @Nullable TxnId minReplay)
    {
        this.redundantBefore = commandStore.unsafeGetRedundantBefore();
        Invariants.require(redundantBefore.ranges(Objects::nonNull).containsAll(commandStore.unsafeGetRangesForEpoch().all()));
        this.minReplay = TxnId.noneIfNull(redundantBefore.foldl((b, v) -> TxnId.nonNullOrMin(v, b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE)), minReplay, ignore -> false));
    }

    protected boolean maybeShouldReplay(TxnId txnId)
    {
        return txnId.compareTo(minReplay) >= 0;
    }

    protected boolean shouldReplay(TxnId txnId, StoreParticipants participants)
    {
        Participants<?> search = participants.route();
        if (search == null) search = participants.hasTouched();
        return redundantBefore.foldl(search, (b, v, id) -> v || b.maxBoundBoth(LOCALLY_DURABLE_TO_COMMAND_STORE, LOCALLY_DURABLE_TO_DATA_STORE).compareTo(id) <= 0, false, txnId, i -> i);
    }

    protected void initialiseState(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
        {
            Command command = safeCommand.current();
            if (command.saveStatus().compareTo(SaveStatus.Stable) >= 0 && command.saveStatus().compareTo(PreApplied) <= 0)
            {
                Commands.maybeExecute(safeStore, safeCommand, command, false, true);
            }
            else if (command.saveStatus().compareTo(Applying) >= 0 && command.saveStatus().compareTo(TruncatedApplyWithOutcome) <= 0)
            {
                if (command.txnId().is(Write))
                {
                    Commands.applyChain(safeStore, command)
                            .begin(safeStore.agent());
                }
                else Invariants.expect(command.hasBeen(Applied), "%s is Applying but is not a Write transaction", txnId);
            }
        }
        safeCommand.update(safeStore, safeCommand.current(), true);
        safeStore.notifyListeners(safeCommand, null);
    }
}
