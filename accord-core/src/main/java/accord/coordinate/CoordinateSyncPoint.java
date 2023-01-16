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
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.*;
import accord.primitives.Routable.Domain;
import accord.utils.Invariants;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.List;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Invariants.checkArgument;

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
        TxnId txnId = node.nextTxnId(ExclusiveSyncPoint, Domain.Range);
        checkArgument(txnId.rw() == ExclusiveSyncPoint);
        FullRoute<?> route = node.computeRoute(txnId, keysOrRanges);
        CoordinateSyncPoint coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(ExclusiveSyncPoint, keysOrRanges), route);
        coordinate.start();
        return coordinate;
    }

    void onPreAccepted(List<PreAcceptOk> successes)
    {
        Deps deps = Deps.merge(successes, ok -> ok.deps);
        trySuccess(new SyncPoint(txnId, deps));

        // TODO (expected): should we commit (or commitInvalidate)? We probably want to special-case recovery anyway
        //  as we don't need to recover these transactions
    }
}
