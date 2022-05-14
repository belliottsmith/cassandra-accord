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

import java.util.Collections;
import java.util.Set;

import accord.local.TxnOperation;
import accord.primitives.*;
import accord.utils.VisibleForImplementation;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.topology.Topology;

import static accord.local.Status.PreAccepted;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData
{
    public final @Nullable PartialTxn partialTxn;
    public final PartialDeps partialDeps;
    public final @Nullable Route route;
    public final boolean read;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private transient Defer defer;

    public enum Kind { Minimal, Maximal }

    // TODO: cleanup passing of topologies here - maybe fetch them afresh from Node? Or perhaps introduce well-named
    //       classes to represent different topology combinations
    public Commit(Kind kind, Id to, Topology coordinateTopology, Topologies topologies, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, boolean read)
    {
        super(to, topologies, txnId, route, executeAt);

        Route sendRoute = null;
        PartialTxn partialTxn = null;
        if (kind == Kind.Maximal)
        {
            boolean isHome = coordinateTopology.rangesForNode(to).contains(route.homeKey);
            partialTxn = txn.slice(scope.covering, isHome);
            if (isHome)
                sendRoute = route;
        }
        else if (executeAt.epoch != txnId.epoch)
        {
            KeyRanges coordinateRanges = coordinateTopology.rangesForNode(to);
            KeyRanges executeRanges = topologies.computeRangesForNode(to);
            KeyRanges extraRanges = executeRanges.difference(coordinateRanges);
            if (!extraRanges.isEmpty())
                partialTxn = txn.slice(extraRanges, coordinateRanges.contains(route.homeKey));
        }

        this.partialTxn = partialTxn;
        this.partialDeps = deps.slice(scope.covering);
        this.route = sendRoute;
        this.read = read;
    }

    // TODO (soon): accept Topology not Topologies
    // TODO: do not commit if we're already ready to execute (requires extra info in Accept responses)
    public static void commitMinimalAndRead(Node node, Topologies executeTopologies, TxnId txnId, Txn txn, Route route, Timestamp executeAt, Deps deps, Set<Id> readSet, Callback<ReadReply> callback)
    {
        Topologies allTopologies = executeTopologies;
        if (txnId.epoch != executeAt.epoch)
            allTopologies = node.topology().preciseEpochs(route, txnId.epoch, executeAt.epoch);

        Topology executeTopology = executeTopologies.forEpoch(executeAt.epoch);
        Topology coordinateTopology = allTopologies.forEpoch(txnId.epoch);
        for (Node.Id to : executeTopology.nodes())
        {
            boolean read = readSet.contains(to);
            Commit send = new Commit(Kind.Minimal, to, coordinateTopology, allTopologies, txnId, txn, route, executeAt, deps, read);
            if (read) node.send(to, send, callback);
            else node.send(to, send);
        }
        if (coordinateTopology != executeTopology)
        {
            for (Node.Id to : allTopologies.nodes())
            {
                if (!executeTopology.contains(to))
                    node.send(to, new Commit(Kind.Minimal, to, coordinateTopology, allTopologies, txnId, txn, route, executeAt, deps, false));
            }
        }
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<? extends RoutingKey> keys()
    {
        return Collections.emptyList();
    }

    public void process(Node node, Id from, ReplyContext replyContext)
    {
        RoutingKey progressKey = node.trySelectProgressKey(txnId, scope);
        ReadNack reply = node.mapReduceLocal(this, txnId.epoch, executeAt.epoch, instance -> {
            Command command = instance.command(txnId);
            switch (command.commit(route != null ? route : scope, progressKey, partialTxn, executeAt, partialDeps))
            {
                default:
                case Success:
                case Redundant:
                    return null;

                case Insufficient:
                    Preconditions.checkState(!command.hasBeenWitnessed());
                    if (defer == null)
                        defer = new Defer(PreAccepted, this, node, from, replyContext);
                    defer.add(command, instance);
                    return ReadNack.NotCommitted;
            }
        }, (r1, r2) -> r1 != null ? r1 : r2);

        if (reply != null)
            node.reply(from, replyContext, reply);
        else if (read)
            super.process(node, from, replyContext);
    }

    @Override
    public MessageType type()
    {
        return MessageType.COMMIT_REQ;
    }

    @Override
    public String toString()
    {
        return "Commit{txnId: " + txnId +
               ", executeAt: " + executeAt +
               ", deps: " + partialDeps +
               ", read: " + read +
               '}';
    }

    public static class Invalidate implements EpochRequest, TxnOperation
    {
        public static void commitInvalidate(Node node, TxnId txnId, RoutingKeys someKeys, Timestamp until)
        {
            commitInvalidate(node, txnId, someKeys, until.epoch);
        }

        public static void commitInvalidate(Node node, TxnId txnId, RoutingKeys someKeys, long untilEpoch)
        {
            // TODO: this kind of check needs to be inserted in all equivalent methods
            Preconditions.checkState(untilEpoch >= txnId.epoch);
            Preconditions.checkState(node.topology().hasEpoch(untilEpoch));
            Topologies commitTo = node.topology().preciseEpochs(someKeys, txnId.epoch, untilEpoch);
            commitInvalidate(node, commitTo, txnId, someKeys);
        }

        public static void commitInvalidate(Node node, Topologies commitTo, TxnId txnId, RoutingKeys someKeys)
        {
            for (Node.Id to : commitTo.nodes())
            {
                Invalidate send = new Invalidate(to, commitTo, txnId, someKeys);
                node.send(to, send);
            }
        }

        final TxnId txnId;
        final RoutingKeys scope;
        final long waitForEpoch;
        final long invalidateUntilEpoch;

        @VisibleForImplementation
        public Invalidate(Id to, Topologies topologies, TxnId txnId, RoutingKeys someKeys)
        {
            this.txnId = txnId;
            this.invalidateUntilEpoch = topologies.currentEpoch();
            int latestRelevantIndex = latestRelevantEpochIndex(to, topologies, someKeys);
            this.waitForEpoch = computeWaitForEpoch(to, topologies, latestRelevantIndex);
            this.scope = computeScope(to, topologies, someKeys, latestRelevantIndex, RoutingKeys::slice, RoutingKeys::union);
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Iterable<? extends RoutingKey> keys()
        {
            return Collections.emptyList();
        }

        @Override
        public long waitForEpoch()
        {
            return waitForEpoch;
        }

        public void process(Node node, Id from, ReplyContext replyContext)
        {
            node.forEachLocal(this, scope, txnId.epoch, invalidateUntilEpoch, instance -> instance.command(txnId).commitInvalidate());
        }

        @Override
        public MessageType type()
        {
            return MessageType.COMMIT_REQ;
        }

        @Override
        public String toString()
        {
            return "CommitInvalidate{txnId: " + txnId + '}';
        }
    }
}
