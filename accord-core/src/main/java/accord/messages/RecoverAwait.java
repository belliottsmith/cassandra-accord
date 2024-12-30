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

package accord.messages;

import accord.api.ProgressLog;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.primitives.Routables.Slice.Minimal;

public class RecoverAwait extends Await
{
    final TxnId recoverId;
    private transient boolean rejects;

    public RecoverAwait(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> participants, ProgressLog.BlockedUntil blockedUntil, int callbackId, boolean notifyProgressLog, TxnId recoverId)
    {
        super(to, topologies, txnId, participants, blockedUntil, callbackId, notifyProgressLog);
        this.recoverId = recoverId;
    }

    public RecoverAwait(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> participants, ProgressLog.BlockedUntil blockedUntil, boolean notifyProgressLog, TxnId recoverId)
    {
        super(to, topologies, txnId, participants, blockedUntil, notifyProgressLog);
        this.recoverId = recoverId;
    }

    @Override
    protected boolean checkOneSynchronousAwait(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (rejects)
            return true;

        if (!super.checkOneSynchronousAwait(safeStore, safeCommand))
            return false;

        Command command = safeCommand.current();
        KnownDeps knownDeps = command.known().deps();
        if (!knownDeps.hasProposedOrDecidedDeps())
            return true;

        if ((knownDeps.hasCommittedOrDecidedDeps() ? command.executeAt() : command.txnId()).compareTo(recoverId) < 0)
            return true;

        Participants<?> participants = scope.intersecting(command.participants().owns(), Minimal);
        if (!command.partialDeps().participants(recoverId).containsAll(participants))
        {
            rejects = true;
            node.reply(replyTo, replyContext, SimpleReply.Nack, null);
        }
        return true;
    }

    @Override
    protected void onSynchronousAwaitComplete()
    {
        if (!rejects)
            node.reply(replyTo, replyContext, SimpleReply.Ok, null);
    }
}
