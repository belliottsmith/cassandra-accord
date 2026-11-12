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

package accord.local;

import org.junit.jupiter.api.Test;

import accord.local.RedundantStatus.SomeStatus;
import accord.primitives.FullKeyRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.impl.IntKey.range;
import static accord.impl.IntKey.routing;
import static accord.local.StoreParticipants.Filter.UPDATE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A range marked stale until at least some point must be bootstrapped again, after which earlier transactions are
 * pre-bootstrap there. So, as for pre-bootstrap ranges, a transaction before that point that has been applied by the
 * shard no longer owns or touches the range.
 */
public class StaleParticipantsTest
{
    private static final Range STALE = range(0, 50), LIVE = range(50, 150);
    private static final FullKeyRoute ROUTE = RoutingKeys.of(routing(10), routing(100)).toRoute(routing(10));

    private static final TxnId BEFORE = txnId(100), STALE_UNTIL = txnId(200), AFTER = txnId(300);
    private static final TxnId SHARD_APPLIED_BEFORE = new TxnId(1, 250, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, Node.Id.NONE);

    @Test
    public void shardAppliedBeforeStaleDoesNotTouchStaleRange()
    {
        RedundantBefore redundantBefore = redundantBefore(SHARD_APPLIED_BEFORE);
        assertOnlyTouchesLive(filter(redundantBefore, BEFORE));
        assertOnlyTouchesLive(filter(redundantBefore, STALE_UNTIL));
    }

    @Test
    public void afterStaleStillTouchesStaleRange()
    {
        assertTouchesBoth(filter(redundantBefore(SHARD_APPLIED_BEFORE), AFTER));
    }

    @Test
    public void notShardAppliedStillTouchesStaleRange()
    {
        assertTouchesBoth(filter(redundantBefore(TxnId.NONE), BEFORE));
    }

    private static void assertOnlyTouchesLive(StoreParticipants participants)
    {
        assertFalse(participants.stillOwns().contains(routing(10)), participants.toString());
        assertFalse(participants.stillTouches().contains(routing(10)), participants.toString());
        assertTrue(participants.stillOwns().contains(routing(100)), participants.toString());
        assertTrue(participants.stillTouches().contains(routing(100)), participants.toString());
    }

    private static void assertTouchesBoth(StoreParticipants participants)
    {
        assertTrue(participants.stillOwns().contains(routing(10)), participants.toString());
        assertTrue(participants.stillTouches().contains(routing(10)), participants.toString());
        assertTrue(participants.stillTouches().contains(routing(100)), participants.toString());
    }

    private static StoreParticipants filter(RedundantBefore redundantBefore, TxnId txnId)
    {
        return StoreParticipants.all(ROUTE, true).filter(UPDATE, redundantBefore, txnId, null);
    }

    private static RedundantBefore redundantBefore(TxnId shardAppliedBefore)
    {
        RedundantBefore owned = RedundantBefore.create(Ranges.of(STALE, LIVE), TxnId.NONE, SomeStatus.NONE);
        if (shardAppliedBefore != TxnId.NONE)
            owned = RedundantBefore.merge(owned, RedundantBefore.create(Ranges.of(STALE, LIVE), shardAppliedBefore, SomeStatus.SHARD_APPLIED_ONLY));
        return RedundantBefore.merge(owned, RedundantBefore.createStale(Ranges.of(STALE), STALE_UNTIL));
    }

    private static TxnId txnId(long hlc)
    {
        return new TxnId(1, hlc, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
    }
}
