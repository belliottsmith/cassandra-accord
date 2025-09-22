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

package accord.maelstrom;

import java.util.concurrent.TimeUnit;

import accord.api.Agent;
import accord.api.CoordinatorEventListener;
import accord.api.OwnershipEventListener;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.ReplyContext;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MaelstromAgent implements Agent, CoordinatorEventListener, OwnershipEventListener
{
    static final MaelstromAgent INSTANCE = new MaelstromAgent();

    @Override
    public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result success, Throwable fail)
    {
        if (fail != null)
        {
            Invariants.require(success == null, "fail (%s) and success (%s) are both not null", fail, success);
            // We don't really process errors for Recover here even though it is provided in the interface
        }
        if (success != null)
        {
            MaelstromResult result = (MaelstromResult) success;
            node.reply(result.client, MaelstromReplyContext.contextFor(result.requestId), new MaelstromReply(result.requestId, result), null);
        }
    }

    @Override
    public void onFailedBootstrap(int attempt, String phase, Ranges ranges, Runnable retry, Runnable fail, Throwable failure)
    {
        throw new AssertionError();
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
    }

    @Override
    public OwnershipEventListener ownershipEvents()
    {
        return this;
    }

    @Override
    public CoordinatorEventListener coordinatorEvents()
    {
        return this;
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
    }

    @Override
    public void onCaughtException(Throwable t, String context)
    {
    }

    @Override
    public long cfkHlcPruneDelta()
    {
        return 1000;
    }

    @Override
    public long maxConflictsPruneInterval()
    {
        return 0;
    }

    @Override
    public int cfkPruneInterval()
    {
        return 1;
    }

    @Override
    public long maxConflictsHlcPruneDelta()
    {
        return 500;
    }

    @Override
    public Txn emptySystemTxn(Txn.Kind kind, Domain domain)
    {
        return new Txn.InMemory(kind, domain == Domain.Key ? Keys.EMPTY : Ranges.EMPTY, new MaelstromRead(Keys.EMPTY, Keys.EMPTY), new MaelstromQuery(Node.Id.NONE, -1), null);
    }

    @Override
    public boolean rejectPreAccept(TimeService time, TxnId txnId)
    {
        return false;
    }

    @Override
    public long expiresAt(ReplyContext replyContext, TimeUnit units)
    {
        return -1;
    }

    @Override
    public AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean isRequested)
    {
        return AsyncChains.success(staleId);
    }

    @Override
    public long minStaleHlc(Node node, boolean requested)
    {
        return node.now() - SECONDS.toMillis(1);
    }

    @Override
    public long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int attempt)
    {
        return units.convert(1L, SECONDS);
    }

    @Override
    public boolean isSlowCoordinator(long elapsed, TimeUnit units, TxnId txnId, int attempt)
    {
        return units.toSeconds(elapsed) > 1;
    }

    @Override
    public long slowReplicaDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, ProgressLog.BlockedUntil blockedUntil, TimeUnit units)
    {
        return units.convert(1L, SECONDS);
    }

    @Override
    public long slowAwaitDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, ProgressLog.BlockedUntil retrying, TimeUnit units)
    {
        return units.convert(1L, SECONDS);
    }

    @Override
    public long retrySyncPointDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(1L, MINUTES);
    }

    @Override
    public long retryDurabilityDelay(Node node, int attempt, TimeUnit units)
    {
        return units.convert(1L, MINUTES);
    }

    @Override
    public long expireEpochWait(TimeUnit units)
    {
        return units.convert(1L, MINUTES);
    }

    @Override
    public long selfSlowAt(TxnId txnId, Status.Phase phase, TimeUnit unit)
    {
        return unit.convert(100L, MICROSECONDS);
    }

    @Override
    public long selfExpiresAt(TxnId txnId, Status.Phase phase, TimeUnit unit)
    {
        return unit.convert(1L, SECONDS);
    }
}
