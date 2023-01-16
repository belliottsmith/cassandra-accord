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

import java.util.*;

import accord.api.Result;
import accord.primitives.*;
import accord.topology.Topologies;
import accord.local.Node;
import accord.messages.PreAccept.PreAcceptOk;

import org.apache.cassandra.utils.concurrent.Future;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.Commit.Invalidate.commitInvalidate;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class Coordinate extends CoordinatePreAccept<Result>
{
    private Coordinate(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(node, txnId, txn, route);
    }

    public static Future<Result> coordinate(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        Coordinate coordinate = new Coordinate(node, txnId, txn, route);
        coordinate.start();
        return coordinate;
    }

    void onPreAccepted(List<PreAcceptOk> successes)
    {
        if (tracker.hasFastPathAccepted())
        {
            Deps deps = Deps.merge(successes, ok -> ok.witnessedAt.equals(txnId) ? ok.deps : null);
            Execute.execute(node, txnId, txn, route, txnId, deps, this);
        }
        else
        {
            Deps deps = Deps.merge(successes, ok -> ok.deps);
            Timestamp executeAt; {
                Timestamp accumulate = Timestamp.NONE;
                for (PreAcceptOk preAcceptOk : successes)
                    accumulate = Timestamp.mergeMax(accumulate, preAcceptOk.witnessedAt);
                executeAt = accumulate;
            }

            // TODO (low priority, efficiency): perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //                                  but by sending accept we rule out hybrid fast-path
            // TODO (low priority, efficiency): if we receive an expired response, perhaps defer to permit at least one other
            //                                  node to respond before invalidating
            if (executeAt.isRejected() || node.agent().isExpired(txnId, executeAt.hlc()))
            {
                proposeInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), (success, fail) -> {
                    if (fail != null)
                    {
                        accept(null, fail);
                    }
                    else
                    {
                        node.withEpoch(executeAt.epoch(), () -> {
                            commitInvalidate(node, txnId, route, executeAt);
                            accept(null, new Invalidated(txnId, route.homeKey()));
                        });
                    }
                });
            }
            else
            {
                node.withEpoch(executeAt.epoch(), () -> {
                    Topologies topologies = tracker.topologies();
                    if (executeAt.epoch() > txnId.epoch())
                        topologies = node.topology().withUnsyncedEpochs(route, txnId.epoch(), executeAt.epoch());
                    Propose.propose(node, topologies, Ballot.ZERO, txnId, txn, route, executeAt, deps, this);
                });
            }
        }
    }
}
