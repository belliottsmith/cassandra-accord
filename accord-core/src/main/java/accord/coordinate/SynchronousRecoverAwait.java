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

package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.ProgressLog.BlockedUntil;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.RecoverAwait;
import accord.messages.SimpleReply;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.messages.SimpleReply.Nack;

/**
 * Synchronously await some set of replicas reaching a given wait condition.
 * This may or may not be a condition we expect to reach promptly, but we will wait only until the timeout passes
 * at which point we will report failure.
 */
public class SynchronousRecoverAwait extends ReadCoordinator<SimpleReply>
{
    final Participants<?> participants;
    final BlockedUntil blockedUntil;
    final boolean notifyProgressLog;
    final TxnId recoverId;

    final BiConsumer<Boolean, Throwable> callback;

    private boolean rejects;
    public SynchronousRecoverAwait(Node node, Topologies topologies, TxnId txnId, Participants<?> participants, BlockedUntil blockedUntil, boolean notifyProgressLog, TxnId recoverId, BiConsumer<Boolean, Throwable> callback)
    {
        super(node, topologies, txnId);
        this.participants = participants;
        this.blockedUntil = blockedUntil;
        this.notifyProgressLog = notifyProgressLog;
        this.recoverId = recoverId;
        this.callback = callback;
    }

    public static SynchronousRecoverAwait awaitAny(Node node, Topologies topologies, TxnId txnId, BlockedUntil blockedUntil, boolean notifyProgressLog, Participants<?> participants, TxnId recoverId, BiConsumer<Boolean, Throwable> callback)
    {
        SynchronousRecoverAwait result = new SynchronousRecoverAwait(node, topologies, txnId, participants, blockedUntil, notifyProgressLog, recoverId, callback);
        result.start();
        return result;
    }

    public static AsyncResult<Boolean> awaitAny(Node node, Topologies topologies, TxnId txnId, BlockedUntil blockedUntil, boolean notifyProgressLog, Participants<?> participants, TxnId recoverId)
    {
        AsyncResult.Settable<Boolean> result = AsyncResults.settable();
        awaitAny(node, topologies, txnId, blockedUntil, notifyProgressLog, participants, recoverId, result.settingCallback());
        return result;
    }

    @Override
    protected Action process(Id from, SimpleReply reply)
    {
        if (reply == Nack)
        {
            rejects = true;
            onDone(null, null);
            return Action.Aborted;
        }
        return Action.Approve;
    }

    @Override
    protected void onDone(ReadCoordinator.Success success, Throwable failure)
    {
        if (failure == null) callback.accept(rejects, null);
        else callback.accept(null, failure);
    }

    @Override
    protected void contact(Id to)
    {
        node.send(to, new RecoverAwait(to, topologies, txnId, participants, blockedUntil, notifyProgressLog, recoverId), this);
    }
}

