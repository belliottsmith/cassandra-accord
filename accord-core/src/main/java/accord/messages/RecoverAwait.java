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
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;

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

    protected void onNotWaiting(SafeCommandStore safeCommandStore, SafeCommand safeCommand)
    {
        if (rejects)
            return;

        Command command = safeCommand.current();
        KnownDeps knownDeps = command.known().deps();
        if (!knownDeps.hasProposedOrDecidedDeps())
            return;

        if ((knownDeps.hasCommittedOrDecidedDeps() ? command.executeAt() : txnId).compareTo(recoverId) < 0)
        {
            if (txnId.is(Txn.Kind.ExclusiveSyncPoint) && txnId.hlc() > recoverId.hlc() && command.executeAt().is(HLC_BOUND))
            {
                rejects = true;
                node.reply(replyTo, replyContext, SimpleReply.Nack, null);
            }
            return;
        }

        Participants<?> participants = scope.intersecting(command.participants().owns(), Minimal);
        if (!command.partialDeps().participants(recoverId).containsAll(participants))
        {
            rejects = true;
            node.reply(replyTo, replyContext, SimpleReply.Nack, null);
        }
    }

    @Override
    protected void onSynchronousAwaitComplete()
    {
        if (!rejects)
            node.reply(replyTo, replyContext, SimpleReply.Ok, null);
    }
}
