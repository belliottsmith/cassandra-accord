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

import accord.api.RoutingKey;
import accord.local.TxnOperation;
import accord.primitives.*;
import accord.utils.VisibleForImplementation;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.topology.Topologies;
import com.google.common.collect.Iterables;

import java.util.Collections;

import static accord.local.TxnOperation.emptyScope;
import static accord.local.TxnOperation.scopeFor;
import static accord.messages.MessageType.APPLY_REQ;
import static accord.messages.MessageType.APPLY_RSP;

public class Apply extends TxnRequest
{
    public final long untilEpoch;
    public final TxnId txnId;
    public final Timestamp executeAt;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;

    @VisibleForImplementation
    public Apply(Id to, Topologies sendTo, Topologies applyTo, long untilEpoch, TxnId txnId, AbstractRoute route, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(to, sendTo, route);
        this.untilEpoch = untilEpoch;
        this.txnId = txnId;
        // TODO: we shouldn't send deps unless we need to (but need to implement fetching them if they're not present)
        KeyRanges slice = applyTo == sendTo ? scope.covering : applyTo.computeRangesForNode(to);
        this.deps = deps.slice(slice);
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        // note, we do not also commit here if txnId.epoch != executeAt.epoch, as the scope() for a commit would be different
        ApplyReply reply = node.mapReduceLocal(this, txnId.epoch, untilEpoch, instance -> {
            Command command = instance.command(txnId);
            switch (command.apply(untilEpoch, scope, executeAt, deps, writes, result))
            {
                default:
                case Insufficient:
                    return ApplyReply.Insufficient;
                case Redundant:
                    return ApplyReply.Redundant;
                case Success:
                    return ApplyReply.Applied;
                case OutOfRange:
                    throw new IllegalStateException();
            }
        }, (r1, r2) -> r1.compareTo(r2) >= 0 ? r1 : r2);

        if (reply == ApplyReply.Applied)
        {
            node.ifLocal(emptyScope(), scope.homeKey, txnId.epoch, instance -> {
                instance.progressLog().durableLocal(txnId);
                return null;
            });
        }

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Iterables.concat(Collections.singleton(txnId), deps.txnIds());
    }

    @Override
    public Iterable<? extends RoutingKey> keys()
    {
        return Collections.emptyList();
    }

    @Override
    public MessageType type()
    {
        return APPLY_REQ;
    }

    public enum ApplyReply implements Reply
    {
        Applied, Redundant, Insufficient;

        @Override
        public MessageType type()
        {
            return APPLY_RSP;
        }

        @Override
        public String toString()
        {
            return "Apply" + name();
        }

        @Override
        public boolean isFinal()
        {
            return this != Insufficient;
        }
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
