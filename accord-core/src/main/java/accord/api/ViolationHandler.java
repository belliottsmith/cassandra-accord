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

package accord.api;

import java.util.function.Supplier;

import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.utils.Functions.alwaysFalse;
import static accord.utils.Invariants.illegalState;

public interface ViolationHandler
{
    /**
     * For use by implementations to decide what to do about timestamp inconsistency, i.e. two different timestamps
     * committed for the same transaction. This is a protocol consistency violation, potentially leading to non-linearizable
     * histories. In test cases this is used to fail the transaction, whereas in real systems this likely will be used for
     * reporting the violation, as it is no more correct at this point to refuse the operation than it is to complete it.
     *
     * Should throw an exception if the inconsistent timestamp should not be applied
     */
    default void onTimestampViolation(@Nullable SafeCommandStore safeStore, Command command, Participants<?> otherParticipants, @Nullable Route<?> otherRoute, Timestamp otherExecuteAt) { throw illegalState(timestampViolationMessage(safeStore, command, otherParticipants, otherRoute, otherExecuteAt)); }
    default void onTimestampViolation(@Nullable SafeCommandStore safeStore, Command command, StoreParticipants otherParticipants, Timestamp otherExecuteAt) { onTimestampViolation(safeStore, command, otherParticipants.owns(), otherParticipants.route(), otherExecuteAt); }

    default void onDependencyViolation(Participants<?> participants, TxnId notWitnessed, Timestamp notWitnessedExecuteAt, TxnId by, Timestamp byExecuteAt) { throw illegalState(dependencyViolationMessage(participants, notWitnessed, notWitnessedExecuteAt, by, byExecuteAt)); }

    static String timestampViolationMessage(@Nullable SafeCommandStore safeStore, Command command, Participants<?> otherParticipants, @Nullable Route<?> otherRoute, Timestamp otherExecuteAt)
    {
        String message = "Linearizability violation for " + command.txnId() + " on " + otherParticipants + (otherRoute != null && otherRoute.size() != otherParticipants.size() ? " (" + otherRoute + ')' : "") + ": "
               + command.txnId() + " has already been " + (command.is(Status.Invalidated) ? " invalidated " : " committed with timestamp " + command.executeAt())
               + " and is now being " + (otherExecuteAt.equals(Timestamp.NONE) ? " invalidated)" : " committed with timestamp " + otherExecuteAt);
        if (safeStore != null)
        {
            message += ". RedundantBefore={";
            Participants<?> participants = Participants.merge(Participants.merge(otherParticipants, (Participants)otherRoute), command.route());
            message += safeStore.redundantBefore().foldlWithBounds(participants, (b, m, s, e) -> (m.isEmpty() ? "[" : ", [") + s + ',' + e + "]:" + b, "", alwaysFalse()) + '}';
        }
        return message;
    }

    static String dependencyViolationMessage(Participants<?> participants, TxnId notWitnessed, Timestamp notWitnessedExecuteAt, TxnId by, Timestamp byExecuteAt)
    {
        return "Linearizability violation on " + participants + ": "
               + notWitnessed + " is committed to execute (at " + notWitnessedExecuteAt + ") before "
               + by + " that should witness it but has already applied (at " + byExecuteAt + ')';

    }

    class ViolationHandlerHolder
    {
        private static volatile Supplier<ViolationHandler> global = () -> new ViolationHandler() {};
        public static ViolationHandler get() { return global.get(); }
        public static void set(Supplier<ViolationHandler> agent) { global = agent; }
    }
}
