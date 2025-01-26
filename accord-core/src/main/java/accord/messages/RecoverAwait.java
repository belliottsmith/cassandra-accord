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

import java.util.EnumMap;

import accord.api.ProgressLog;
import accord.coordinate.Recover.InferredFastPath;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Ballot;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Participants;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.api.ProgressLog.BlockedUntil.CommittedOrNotFastPathCommit;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;

public class RecoverAwait extends Await
{
    public static class SerializerSupport
    {
        public static RecoverAwait create(TxnId txnId, Participants<?> scope, ProgressLog.BlockedUntil blockedUntil, boolean notifyProgressLog, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, TxnId recoverId)
        {
            return new RecoverAwait(txnId, scope, blockedUntil, minAwaitEpoch, maxAwaitEpoch, callbackId, notifyProgressLog, recoverId);
        }
    }

    public final TxnId recoverId;
    private transient boolean rejects;
    private transient boolean cannotAccept;

    public RecoverAwait(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> participants, ProgressLog.BlockedUntil blockedUntil, boolean notifyProgressLog, TxnId recoverId)
    {
        super(to, topologies, txnId, participants, blockedUntil, notifyProgressLog);
        this.recoverId = recoverId;
    }

    RecoverAwait(TxnId txnId, Participants<?> scope, ProgressLog.BlockedUntil blockedUntil, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, boolean notifyProgressLog, TxnId recoverId)
    {
        super(txnId, scope, blockedUntil, minAwaitEpoch, maxAwaitEpoch, callbackId, notifyProgressLog);
        this.recoverId = recoverId;
    }

    protected void onNotWaiting(SafeCommandStore safeCommandStore, SafeCommand safeCommand)
    {
        if (rejects)
            return;

        Command command = safeCommand.current();
        KnownDeps knownDeps = command.known().deps();
        if (!knownDeps.hasProposedOrDecidedDeps())
        {
            if (blockedUntil == CommittedOrNotFastPathCommit && node.id().equals(txnId.node) && command.promised().equals(Ballot.ZERO))
                cannotAccept = true;
            return;
        }

        if ((knownDeps.hasCommittedOrDecidedDeps() ? command.executeAt() : txnId).compareTo(recoverId) < 0)
        {
            if (txnId.is(Txn.Kind.ExclusiveSyncPoint) && txnId.hlc() > recoverId.hlc() && command.executeAt().is(HLC_BOUND))
            {
                rejects = true;
                node.reply(replyTo, replyContext, RecoverAwaitOk.Reject, null);
            }
            return;
        }

        Participants<?> participants = scope.intersecting(command.participants().owns(), Minimal);
        if (!command.partialDeps().participants(recoverId).containsAll(participants))
        {
            rejects = true;
            node.reply(replyTo, replyContext, RecoverAwaitOk.Reject, null);
        }
    }

    @Override
    protected void onSynchronousAwaitComplete()
    {
        if (!rejects)
            node.reply(replyTo, replyContext, cannotAccept ? RecoverAwaitOk.Unknown : RecoverAwaitOk.Accept, null);
    }

    @Override
    public MessageType type()
    {
        return MessageType.RECOVER_AWAIT_REQ;
    }

    public enum RecoverAwaitOk implements Reply
    {
        Accept(InferredFastPath.Accept), Unknown(InferredFastPath.Unknown), Reject(InferredFastPath.Reject);

        private static final EnumMap<InferredFastPath, RecoverAwaitOk> lookup = new EnumMap<>(InferredFastPath.class);
        static
        {
            for (RecoverAwaitOk ok : values())
                lookup.put(ok.inferredFastPath, ok);
        }

        public static RecoverAwaitOk get(InferredFastPath inferredFastPath)
        {
            return lookup.get(inferredFastPath);
        }

        public final InferredFastPath inferredFastPath;

        RecoverAwaitOk(InferredFastPath inferredFastPath)
        {
            this.inferredFastPath = inferredFastPath;
        }


        @Override
        public MessageType type()
        {
            return MessageType.RECOVER_AWAIT_RSP;
        }
    }
}
