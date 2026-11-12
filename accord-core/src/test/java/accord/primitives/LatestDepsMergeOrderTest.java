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

package accord.primitives;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import accord.impl.IntKey;
import accord.local.Node;

import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsProposed;
import static accord.primitives.Known.KnownDeps.DepsProposedFixed;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LatestDeps merging must not depend on reply order, nor drop coordinator deps on a PreAccept-phase tie.
 *
 * Recovery (Recover.propose -> LatestDeps.merge(okList, ok -> ok.deps).mergeProposal(), Recover 625-643)
 * folds the RecoverOk replies in node-id order (okList comes from a SortedListMap<Id, RecoverOk>). Each reply's
 * LatestDeps is built by BeginRecovery.applyInternal:
 *  - r1 never received the original PreAccept, so Commands.recover leaves it PreAccepted: known=DepsUnknown,
 *    ballot ZERO, coordinatedDeps = command.partialDeps() = null, localDeps = freshly calculated deps;
 *  - r2 received the privileged coordinator's PreAccept with deps (PrivilegedCoordinatorWithDeps -> SaveStatus
 *    PreAcceptedWithDeps): known=DepsFromCoordinator, ballot ZERO, coordinatedDeps = coordinator's deps {t1},
 *    localDeps = its own freshly calculated deps (which do not contain t1: only the coordinator witnessed t1).
 * Both are PreAccept phase, so AbstractEntry.reduce keeps the LEFT entry; when r1 (DepsUnknown) is left the
 * coordinator deps are discarded and forProposal(DepsUnknown) emits only local deps.
 *
 * This test builds exactly those two LatestDeps values with the real public API and runs the real merge.
 */
public class LatestDepsMergeOrderTest
{
    private static final Ranges RANGE = Ranges.of(IntKey.range(0, 100));
    private static final IntKey.Routing KEY = IntKey.routing(10);

    private static TxnId txnId(int hlc, int node)
    {
        return new TxnId(1, hlc, Write, Key, new Node.Id(node));
    }

    private static Deps deps(TxnId... ids)
    {
        try (Deps.Builder builder = Deps.builder(false))
        {
            for (TxnId id : ids)
                builder.add(KEY, id);
            return builder.build();
        }
    }

    // t0: a dependency both replicas witnessed; t1: a dependency only the privileged coordinator witnessed
    private static final TxnId T0 = txnId(5, 3);
    private static final TxnId T1 = txnId(7, 3);

    // reply of r1 (lower node id): missed the original PreAccept (BeginRecovery coordinatedDeps == null)
    private static LatestDeps r1()
    {
        return LatestDeps.create(RANGE, DepsUnknown, Ballot.ZERO, null, deps(T0));
    }

    // reply of r2: PreAcceptedWithDeps from the privileged coordinator, coordinator deps {t1}
    private static LatestDeps r2()
    {
        return LatestDeps.create(RANGE, DepsFromCoordinator, Ballot.ZERO, deps(T1), deps(T0));
    }

    // reply of r3: accepted (slow path) an interrupted recovery's proposal {t0}, which lacked the coordinator deps
    private static LatestDeps r3()
    {
        return LatestDeps.create(RANGE, DepsProposed, Ballot.fromValues(1, 1, new Node.Id(4)), deps(T0), null);
    }

    private static Deps propose(List<LatestDeps> okList)
    {
        // identical to Recover.mergeDeps(okList).mergeProposal()
        return LatestDeps.merge(okList, d -> d).mergeProposal();
    }

    @Test
    public void proposalIndependentOfReplyOrder()
    {
        Deps p12 = propose(Arrays.asList(r1(), r2()));
        Deps p21 = propose(Arrays.asList(r2(), r1()));
        assertEquals(p21.txnIds(), p12.txnIds(),
                     "recovery proposal depends on reply order: (r1,r2) -> " + p12.txnIds() + ", (r2,r1) -> " + p21.txnIds());
    }

    @Test
    public void coordinatorDepsIncludedInProposal()
    {
        // the order Recover actually uses: node-id order, r1 < r2
        Deps proposal = propose(Arrays.asList(r1(), r2()));
        assertTrue(proposal.contains(T1),
                   "coordinator deps " + T1 + " reported by a DepsFromCoordinator reply are missing from the recovery proposal " + proposal.txnIds());
    }

    @Test
    public void coordinatorDepsSurviveHigherPhaseEntry()
    {
        for (List<LatestDeps> okList : Arrays.asList(Arrays.asList(r2(), r3()), Arrays.asList(r3(), r2())))
        {
            Deps proposal = propose(okList);
            assertTrue(proposal.contains(T1),
                       "coordinator deps " + T1 + " were dropped in favour of an interrupted recovery's DepsProposed: " + proposal.txnIds());
        }
    }

    @Test
    public void fixedDepsAreNotMerged()
    {
        LatestDeps fixed = LatestDeps.create(RANGE, DepsProposedFixed, Ballot.fromValues(1, 1, new Node.Id(4)), deps(T0), null);
        for (List<LatestDeps> okList : Arrays.asList(Arrays.asList(r2(), fixed), Arrays.asList(fixed, r2())))
        {
            Deps proposal = propose(okList);
            assertEquals(deps(T0).txnIds(), proposal.txnIds(), "medium path fixed deps must not be merged with coordinator deps");
        }
    }
}
