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

package accord.coordinate.tracking;

import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.coordinate.Recover.InferredFastPath;
import accord.impl.IntKey;
import accord.local.Node.Id;
import accord.primitives.Participants;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.topology.Shard;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topologies;
import static accord.Utils.topology;
import static accord.impl.IntKey.range;

/**
 * fastPathReliesOnUnwitnessedCoordinatorVote must compare against the fast path electorate, not the replication factor.
 *
 * Recover.awaitSupersedingCoord only waits for a later PrivilegedCoordinatorWithDeps
 * transaction T' on shards where RecoveryShardTracker.fastPathReliesOnUnwitnessedCoordinatorVote is true.
 * That predicate is meant to hold exactly when the recovering txn R could still have taken the fast path
 * but only if every elector not known to reject voted for it - i.e. when ONE more fast-path reject would
 * make Shard.rejectsFastPath true. Shard.rejectsFastPath counts against the fast-path electorate
 *, but the predicate compares with shard.nodes.size() (rf, RecoveryTracker).
 *
 * The two scenarios below are the same recovery from the point of view of R's fast path: shard {1,2,3},
 * R coordinated by node 1 (privileged, with deps), recovery quorum {2,3}, node 2 (T''s coordinator) votes
 * for R, node 3 does not vote for R, and replicas report R's coordinator vote. In both, R's fast path is
 * possible only with the votes of exactly {1, 2}. The only difference is whether node 3 is in the
 * electorate (and so cast a reject) or not (and so cannot vote).
 */
public class RecoveryCoordVoteElectorateTest
{
    private static final TxnId RECOVER_ID = new TxnId(1, 100, FastPath.PrivilegedCoordinatorWithDeps.bits, Kind.Write, Domain.Key, id(1));
    private static final Participants<?> SELF_COORD_VOTES = RoutingKeys.of(IntKey.routing(50));

    private static RecoveryTracker recover(Set<Id> electorate)
    {
        Shard shard = shard(range(0, 100), idList(1, 2, 3), electorate);
        RecoveryTracker tracker = new RecoveryTracker(topologies(topology(1, shard)));
        tracker.recordSuccess(id(2), true);   // T''s coordinator: voted for R
        tracker.recordSuccess(id(3), false);  // did not vote for R (reject if an elector, no-op otherwise)
        Assertions.assertTrue(tracker.get(0, 0).hasReachedQuorum());
        return tracker;
    }

    private static void check(String name, Set<Id> electorate)
    {
        RecoveryTracker tracker = recover(electorate);
        RecoveryTracker.RecoveryShardTracker st = tracker.get(0, 0);
        Shard shard = st.shard;

        // Recovery does not reject R's fast path, so Recover reaches awaitSupersedingCoord
        InferredFastPath inferred = tracker.inferFastPathDecision(RECOVER_ID, SELF_COORD_VOTES, null);
        Assertions.assertEquals(InferredFastPath.Accept, inferred, name + ": inferFastPathDecision");

        // Intended: R's fast path relies on every remaining elector (incl. T''s coordinator) iff one more
        // reject would make the code's own threshold reject the fast path.
        int rejects = st.fastPathRejects;
        boolean expected = !shard.rejectsFastPath(RECOVER_ID, rejects) && shard.rejectsFastPath(RECOVER_ID, rejects + 1);
        boolean actual = st.fastPathReliesOnUnwitnessedCoordinatorVote(RECOVER_ID, SELF_COORD_VOTES);
        Assertions.assertEquals(expected, actual,
                                name + ": fastPathReliesOnUnwitnessedCoordinatorVote (rf=" + shard.rf
                                + ", electorate=" + shard.fastPathElectorateSize
                                + ", fastQuorum=" + shard.fastQuorumSize(RECOVER_ID)
                                + ", fastPathRejects=" + rejects
                                + "): R's fast path needs every remaining elector, so Recover must wait for T'; "
                                + "predicate=" + actual + " means awaitSupersedingCoord returns Accept without waiting");
    }

    @Test
    public void fullElectorateWaitsForLaterCoordinator()
    {
        check("electorate {1,2,3}", idSet(1, 2, 3));
    }

    @Test
    public void subsetElectorateWaitsForLaterCoordinator()
    {
        check("electorate {1,2}", idSet(1, 2));
    }
}
