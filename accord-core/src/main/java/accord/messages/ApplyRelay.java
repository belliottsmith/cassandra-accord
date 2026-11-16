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

import javax.annotation.Nullable;

import accord.api.Result.PersistableResult;
import accord.coordinate.PersistTxn;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.APPLY_RELAY_REQ;
import static accord.messages.MessageType.StandardMessage.APPLY_RELAY_RSP;

/**
 * Sent by a coordinator persisting a transaction to one replica in another DC (see
 * {@link accord.api.ProtocolModifiers#relayRemoteApply()}), asking it to apply the transaction to {@code forwardTo}
 * (which may include itself), all of which are in its DC, and to reply with which have applied it. This carries the
 * transaction's writes over the WAN once per DC, rather than once per replica.
 * <p>
 * The relay sends {@link Apply.Kind#NoDeps} to {@code noDepsAt}, which the coordinator expects to have recorded the
 * transaction as Stable, and {@code kind} to the others. Its definition and deps cover (at least) the replicas we
 * forward to, so that the relay may resend {@link Apply.Kind#Maximal} to any that reply Insufficient.
 * <p>
 * The relay takes no responsibility for durability: if it does not reply, the coordinator contacts the replicas itself.
 * Note: this message was introduced later, and so may only be sent once every replica understands it.
 */
public class ApplyRelay implements Request
{
    public final TxnId txnId;
    public final FullRoute<?> route;
    public final Ballot ballot;
    public final Apply.Kind kind;
    public final Timestamp executeAt;
    public final @Nullable PartialTxn txn;
    public final PartialDeps deps;
    public final @Nullable Writes writes;
    public final @Nullable PersistableResult result;
    public final SortedArrayList<Node.Id> forwardTo, noDepsAt;
    // the epochs the coordinator persists in, which we use too, so that we construct the same messages it would
    public final long minEpoch, maxEpoch;

    public ApplyRelay(TxnId txnId, FullRoute<?> route, Ballot ballot, Apply.Kind kind, Timestamp executeAt, @Nullable PartialTxn txn, PartialDeps deps,
                      @Nullable Writes writes, @Nullable PersistableResult result, SortedArrayList<Node.Id> forwardTo, SortedArrayList<Node.Id> noDepsAt,
                      long minEpoch, long maxEpoch)
    {
        this.minEpoch = minEpoch;
        this.maxEpoch = maxEpoch;
        this.txnId = txnId;
        this.route = route;
        this.ballot = ballot;
        this.kind = kind;
        this.executeAt = executeAt;
        this.txn = txn;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
        this.forwardTo = forwardTo;
        this.noDepsAt = noDepsAt;
    }

    @Override
    public Cancellable process(Node on, Node.Id from, ReplyContext replyContext)
    {
        on.withEpochAtLeast(maxEpoch, null, (ignore, fail) -> on.reply(from, replyContext, null, fail, null), () -> {
            Topologies topologies;
            try
            {
                topologies = on.topology().active().preciseEpochs(route, minEpoch, maxEpoch, txnId.selectsShards());
            }
            catch (Throwable t)
            {
                on.reply(from, replyContext, null, t, null);
                return;
            }
            PersistTxn.relay(on, topologies, this, (appliedAt, failedAt) -> on.reply(from, replyContext, new ApplyRelayReply(appliedAt, failedAt), null, null));
        });
        return null;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public MessageType type()
    {
        return APPLY_RELAY_REQ;
    }

    @Override
    public String toString()
    {
        return "ApplyRelay{" + txnId + ", kind=" + kind + ", forwardTo=" + forwardTo + ", noDepsAt=" + noDepsAt + '}';
    }

    public static class ApplyRelayReply implements Reply
    {
        public final SortedArrayList<Node.Id> appliedAt, failedAt;

        public ApplyRelayReply(SortedArrayList<Node.Id> appliedAt, SortedArrayList<Node.Id> failedAt)
        {
            this.appliedAt = appliedAt;
            this.failedAt = failedAt;
        }

        @Override
        public MessageType type()
        {
            return APPLY_RELAY_RSP;
        }

        @Override
        public String toString()
        {
            return "ApplyRelayReply{appliedAt=" + appliedAt + ", failedAt=" + failedAt + '}';
        }
    }
}
