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

package accord.local.cfk;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.local.Node;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED_DURABLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.PREACCEPTED_WITH_DEPS;
import static accord.local.cfk.CommandsForKey.InternalStatus.STABLE;
import static accord.local.cfk.CommandsForKey.NO_BOUNDS_INFO;

/**
 * CommandsForKey.hasUniqueHlcAndIsReadyToExecute must consider the newest byId entry.
 *
 * CommandsForKey.hasUniqueHlcAndIsReadyToExecute starts its byId cursor at byId.length - 1 and
 * pre-decrements it before the first access, so the highest byId
 * entry is never examined. Commands.updateWaitingOn removes the
 * WaitingOnKey bit when this method returns true.
 */
public class CommandsForKeyReadyNewestEntryTest
{
    private static final RoutingKey KEY = IntKey.routing(1);
    private static final Node.Id NODE = new Node.Id(1);

    private static TxnId write(long hlc)
    {
        return new TxnId(1, hlc, Txn.Kind.Write, Domain.Key, NODE);
    }

    private static CommandsForKey cfk(TxnInfo... byId)
    {
        // public reconstruction entry point used when a CFK is loaded from storage (Serialize)
        return CommandsForKey.SerializerSupport.create(KEY, byId, 0, new Unmanaged[0], TxnId.NONE, NO_BOUNDS_INFO, true);
    }

    private static Deps depsOn(TxnId... ids)
    {
        try (Deps.Builder builder = new Deps.Builder(true))
        {
            for (TxnId id : ids)
                builder.add(KEY, id);
            return builder.build();
        }
    }

    private static TxnInfo applied(TxnId id)   { return TxnInfo.create(id, APPLIED_DURABLE, true, id, Ballot.ZERO); }
    private static TxnInfo stable(TxnId id, Timestamp at) { return TxnInfo.create(id, STABLE, true, at, Ballot.ZERO); }
    private static TxnInfo preaccepted(TxnId id) { return TxnInfo.create(id, PREACCEPTED_WITH_DEPS, true, id, Ballot.ZERO); }

    /**
     * byId = [d (applied, T's only dep), T (stable), e (newest, undecided, managed, witnessed by T)].
     * The method's own c > 0 rule says it must return false for e; with e in last position it is
     * never visited and the method returns true.
     */
    @Test
    @Timeout(10)
    public void newestUndecidedWitnessedEntryIsChecked()
    {
        TxnId d = write(10);
        TxnId t = write(20);
        Timestamp tExecuteAt = Timestamp.fromValues(1, 40, NODE);
        TxnId e = write(30);

        Deps deps = depsOn(d);

        // Control: the same undecided e, but not in the last byId slot (an applied f follows it).
        TxnId f = write(50);
        CommandsForKey control = cfk(applied(d), stable(t, tExecuteAt), preaccepted(e), applied(f));
        Assertions.assertFalse(control.hasUniqueHlcAndIsReadyToExecute(t, tExecuteAt, deps.keyDeps),
                               "control: undecided managed e (not last in byId) blocks T, per the c > 0 rule");

        // Bug: e is the newest entry in byId.
        CommandsForKey bug = cfk(applied(d), stable(t, tExecuteAt), preaccepted(e));
        Assertions.assertFalse(bug.hasUniqueHlcAndIsReadyToExecute(t, tExecuteAt, deps.keyDeps),
                               "hasUniqueHlcAndIsReadyToExecute returned true although the newest byId entry " + e
                               + " is undecided, managed and witnessed by " + t + ": byId[byId.length-1] was never examined");
    }

    /**
     * Companion manifestation of the same off-by-one: T's only dep d is the newest byId entry and is
     * APPLIED, so T is ready on this key; the method never reaches d and returns false.
     */
    @Test
    @Timeout(10)
    public void newestEntryAsAppliedDepIsFound()
    {
        TxnId t = write(10);
        TxnId d = write(20);                       // d > T's id but < T's executeAt, so it is in T's deps
        Timestamp tExecuteAt = Timestamp.fromValues(1, 40, NODE);

        CommandsForKey cfk = cfk(stable(t, tExecuteAt), applied(d));
        Assertions.assertTrue(cfk.hasUniqueHlcAndIsReadyToExecute(t, tExecuteAt, depsOn(d).keyDeps),
                              "T's only dep " + d + " on the key is APPLIED but the method returned false: it never reached byId[byId.length-1]");
    }
}
