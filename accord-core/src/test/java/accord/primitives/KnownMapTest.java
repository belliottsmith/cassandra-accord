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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;

import static accord.impl.IntKey.routing;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.Nothing;
import static accord.primitives.Status.PreApplied;

/**
 * {@link KnownMap#knownFor(Known, Participants)} reports the participants for which the merged peer replies
 * demonstrate at least the required knowledge. A participant that no reply covers has no knowledge at all,
 * so it must never be reported as satisfying the requirement - in the same way that the
 * {@link KnownMap#knownFor(Routables, Routables)} overload folds uncovered participants with
 * {@link KnownMap.MinAndMaxKnown#Nothing}.
 *
 * This matters to {@code Propagate.tryUpgradeTruncated}, which uses the result to decide which participants are
 * able to reach PreApplied (and so are not stale).
 */
public class KnownMapTest
{
    static final RoutingKey K0 = routing(10);
    static final RoutingKey K1 = routing(20);

    /**
     * A single map gives the same verdict for a participant as the other overload: an uncovered key
     * satisfies no knowledge.
     */
    @Test
    public void knownForDoesNotReturnParticipantsNoReplyCovers()
    {
        Participants<?> expect = RoutingKeys.of(K0, K1);
        KnownMap map = KnownMap.create(RoutingKeys.of(K0), new KnownMap.MinAndMaxKnown(Known.DepsOnly));
        Participants<?> satisfied = map.knownFor(Known.DepsOnly, expect);
        Assertions.assertTrue(satisfied.contains(K0), "K0 is covered by the map: " + satisfied);
        Assertions.assertFalse(satisfied.contains(K1),
                                "knownFor(DepsOnly, " + expect + ") over map " + map + " returned " + satisfied
                                + ": K1 has no entry in the map, so nothing is known for it");
    }

    /**
     * The two knownFor overloads must agree about an uncovered participant.
     */
    @Test
    public void knownForAgreesWithKnownForOwnsTouchesOnUncoveredKey()
    {
        KnownMap map = KnownMap.create(RoutingKeys.of(K0), new KnownMap.MinAndMaxKnown(Known.DepsOnly));
        Participants<?> k1 = RoutingKeys.of(K1);
        Known knownForK1 = map.knownFor(k1, k1);
        Assertions.assertFalse(Known.DepsOnly.isSatisfiedBy(knownForK1), "precondition: " + knownForK1);
        Participants<?> satisfied = map.knownFor(Known.DepsOnly, k1);
        Assertions.assertTrue(satisfied.isEmpty(),
                              "knownFor(owns,touches) for K1 = " + knownForK1 + " (does not satisfy DepsOnly), but knownFor(DepsOnly, [K1]) = " + satisfied);
    }

    /**
     * A participant covered by the map satisfies the requirement when its entry does, so the fix must not
     * make knownFor overly conservative.
     */
    @Test
    public void knownForReturnsCoveredParticipantsThatSatisfyRequirement()
    {
        KnownMap map = KnownMap.create(RoutingKeys.of(K0, K1), new KnownMap.MinAndMaxKnown(Known.DepsOnly));
        Participants<?> satisfied = map.knownFor(Known.DepsOnly, RoutingKeys.of(K0, K1));
        Assertions.assertTrue(satisfied.contains(K0) && satisfied.contains(K1), "both keys are covered and DepsKnown: " + satisfied);
        Assertions.assertTrue(map.knownFor(PreApplied.minKnown, RoutingKeys.of(K0, K1)).isEmpty(),
                              "neither key is known to be pre-applied");
    }

    /**
     * Mirrors Propagate.tryUpgradeTruncated's stale computation: a key that no reply covers cannot be
     * upgraded to PreApplied, so it must be considered stale rather than "not stale".
     */
    @Test
    public void staleComputationCountsUncoveredKeyAsStale()
    {
        Participants<?> stillTouches = RoutingKeys.of(K0, K1);
        Participants<?> stillOwnsOrMayExecute = stillTouches; // not a sync point: the store owns both keys
        KnownMap known = KnownMap.create(RoutingKeys.of(K0), SaveStatus.TruncatedApply);

        Assertions.assertTrue(known.hasAnyFullyTruncated(stillTouches), "precondition: " + known);

        Known commandKnown = Nothing; // lagging store: has not witnessed the txn
        Known required = PreApplied.minKnown;
        Known requireExtra = required.subtract(commandKnown);
        Participants<?> notStaleTouches = known.knownFor(Nothing.with(requireExtra.deps()), stillTouches);
        Participants<?> notStaleOwnsOrMayExecute = known.knownFor(requireExtra.with(DepsUnknown), stillOwnsOrMayExecute);
        Participants<?> staleTouches = stillTouches.without(notStaleTouches);
        Participants<?> staleOwnsOrMayExecute = stillOwnsOrMayExecute.without(notStaleOwnsOrMayExecute);

        String state = "map=" + known + " notStaleTouches=" + notStaleTouches + " notStaleOwnsOrMayExecute=" + notStaleOwnsOrMayExecute
                       + " staleTouches=" + staleTouches + " staleOwnsOrMayExecute=" + staleOwnsOrMayExecute;
        Assertions.assertTrue(staleOwnsOrMayExecute.contains(K1),
                              "K1 is covered by no reply, so it cannot be upgraded to PreApplied and must be stale; " + state);
        Assertions.assertTrue(staleTouches.contains(K1),
                              "K1 is covered by no reply, so its deps cannot be known; " + state);
    }

    /**
     * The knowledge that is valid for all participants (which may be upgraded after merging replies)
     * applies to uncovered participants too, so they must still be returned in that case.
     */
    @Test
    public void knownForHonoursValidForAllOnUncoveredParticipants()
    {
        KnownMap map = KnownMap.create(RoutingKeys.of(K0), new KnownMap.MinAndMaxKnown(Known.DepsOnly))
                               .with(Known.DefinitionAndRoute);
        Participants<?> satisfied = map.knownFor(Known.DefinitionOnly, RoutingKeys.of(K0, K1));
        Assertions.assertTrue(satisfied.contains(K1), "validForAll knows the definition for every key: " + satisfied);
    }
}
