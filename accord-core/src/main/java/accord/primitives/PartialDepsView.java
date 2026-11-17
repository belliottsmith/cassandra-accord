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

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.local.cfk.CommandsForKey.managesExecution;

/**
 * A (possibly lazily materialised) view of a {@link PartialDeps}.
 *
 * {@link PartialDeps} implements this interface trivially, but an implementation may instead retain the deps in their
 * serialized form, answering some limited queries directly from this form and deserializing (and memoising) only the
 * portions of the collection that are required to answer other queries. This permits commands to be loaded, shrunk
 * and evicted without paying the full cost of deserializing (and reserializing) their dependencies.
 *
 * Callers on hot paths that are agnostic to the representation should prefer the narrowest accessor available here:
 *  - {@link #keys()}, {@link #covering()} and {@link #isEmpty()} are expected to be cheap;
 *  - {@link #isStableDepCovering} and {@link #participants(TxnId)} for {@link Routable.Domain#Key} {@code TxnId} may
 *    be answered directly from a serialized form;
 *  - {@link #keyDeps()} and {@link #rangeDeps()} may require deserializing only that portion of the collection;
 *  - {@link #materialize()} may require deserializing the whole collection.
 *
 * Any memoised state may be discarded by {@link #unmemoize()}.
 *
 * Note that the identity of a view is used to detect if the deps of a command have changed, so a command that is
 * transitioned to a new state without modifying its deps should propagate the view, rather than its materialised form.
 */
public interface PartialDepsView
{
    /**
     * @return the full deps; may require deserializing the collection, but will be memoised until {@link #unmemoize()}
     */
    PartialDeps materialize();

    /**
     * @return the KeyDeps; may require deserializing this portion of the collection, but will be memoised until {@link #unmemoize()}
     */
    KeyDeps keyDeps();

    /**
     * @return the RangeDeps; may require deserializing this portion of the collection, and will be memoised
     */
    RangeDeps rangeDeps();

    /**
     * @return the participants these deps were computed for; see {@link PartialDeps#covering}
     */
    Participants<?> covering();

    /**
     * @return the keys of {@link #keyDeps()}, i.e. {@code keyDeps().keys()}
     */
    RoutingKeys keys();

    /**
     * @return true iff no TxnId is mapped to any key or range; see {@link Deps#isEmpty()}
     */
    boolean isEmpty();

    /**
     * @return the number of TxnId in the collection; see {@link Deps#txnIdCount()}
     */
    int txnIdCount();

    /**
     * Equivalent to:
     * {@code int i = indexOf(txnId); return i >= 0 && isStable(i) && participants(i).containsAll(intersecting) }
     */
    boolean isStableDepCovering(TxnId txnId, Unseekables<?> intersecting);

    /**
     * @return true iff {@code txnId} is a stable dependency for all of {@code participants} (so true if there are none);
     * see {@link Deps#isStableDep(TxnId, Unseekables)}
     */
    default boolean isStableDep(TxnId txnId, Unseekables<?> participants)
    {
        return participants.isEmpty() || isStableDepCovering(txnId, participants);
    }

    /**
     * Discard any memoised (deserialized) state that can be cheaply recomputed.
     */
    void unmemoize();

    /**
     * @return true if {@code that} may be considered to be the same collection as this one, without comparing their contents
     */
    default boolean isSameAs(@Nullable PartialDepsView that)
    {
        return this == that;
    }

    default boolean covers(Unseekables<?> participants)
    {
        return covering().containsAll(participants);
    }

    default boolean covers(RoutingKey key)
    {
        return covering().contains(key);
    }

    /**
     * @return the participants for txnId; see {@link Deps#participants(TxnId)}
     */
    default Participants<?> participants(TxnId txnId)
    {
        switch (txnId.domain())
        {
            default: throw new UnhandledEnum(txnId.domain());
            case Key:
                Invariants.require(managesExecution(txnId));
                return keyDeps().participants(txnId);
            case Range: return rangeDeps().participants(txnId);
        }
    }

    static boolean isSameAs(@Nullable PartialDepsView a, @Nullable PartialDepsView b)
    {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.isSameAs(b) || b.isSameAs(a);
    }
}
