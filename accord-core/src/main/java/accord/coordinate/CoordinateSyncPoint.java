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

import accord.local.Node;
import accord.messages.Apply;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.*;
import accord.primitives.Txn.Kind;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.List;

import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
import static accord.primitives.Timestamp.mergeMax;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Functions.foldl;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint extends CoordinatePreAccept<SyncPoint>
{
    private CoordinateSyncPoint(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(node, txnId, txn, route);
    }

    public static Future<SyncPoint> exclusive(Node node, Seekables<?, ?> keysOrRanges)
    {
        return coordinate(ExclusiveSyncPoint, node, keysOrRanges);
    }

    public static Future<SyncPoint> inclusive(Node node, Seekables<?, ?> keysOrRanges)
    {
        return coordinate(Kind.SyncPoint, node, keysOrRanges);
    }

    private static Future<SyncPoint> coordinate(Kind kind, Node node, Seekables<?, ?> keysOrRanges)
    {
        TxnId txnId = node.nextTxnId(kind, keysOrRanges.domain());
        FullRoute<?> route = node.computeRoute(txnId, keysOrRanges);
        CoordinateSyncPoint coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(kind, keysOrRanges), route);
        coordinate.start();
        return coordinate;
    }

    void onPreAccepted(List<PreAcceptOk> successes)
    {
        Deps deps = Deps.merge(successes, ok -> ok.deps);
        Timestamp executeAt = foldl(successes, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (executeAt.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt, this);
        }
        else
        {
            // SyncPoint transactions always propose their own txnId as their executeAt, as they are not really executed.
            // They only create happens-after relationships wrt their dependencies, which represent all transactions
            // that *may* execute before their txnId, so once these dependencies apply we can say that any action that
            // awaits these dependencies applies after them. In the case of ExclusiveSyncPoint, we additionally guarantee
            // that no lower TxnId can later apply.
            new Propose<SyncPoint>(node, tracker.topologies(), Ballot.ZERO, txnId, txn, route, deps, txnId, this)
            {
                @Override
                void onAccepted()
                {
                    node.send(tracker.nodes(), id -> new Apply(id, tracker.topologies(), tracker.topologies(), txnId.epoch(), txnId, route, txnId, deps, txn.execute(txnId, null), txn.result(txnId, null)));
                    accept(new SyncPoint(txnId, deps), null);
                }
            }.start();
        }
    }
}
