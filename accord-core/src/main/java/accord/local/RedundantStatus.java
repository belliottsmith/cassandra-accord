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

import accord.utils.Invariants;
import accord.utils.TinyEnumSet;

import static accord.local.RedundantStatus.Cmp.LT;
import static accord.local.RedundantStatus.Cmp.LE;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.RedundantStatus.Coverage.SOME;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.Property.REVERSE_PROPERTIES;
import static accord.local.RedundantStatus.Property.SHARD_AND_LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.SHARD_ONLY_APPLIED;
import static accord.local.RedundantStatus.Property.WAS_OWNED;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Integer.numberOfTrailingZeros;

// TODO (testing): validate that we never lose a status previously held,
//  i.e. once any particular property holds for a TxnId, it should continue to hold in perpetuity
public class RedundantStatus
{
    public enum Coverage
    {
        NONE,
        SOME,
        ALL;

        public boolean atLeast(Coverage coverage)
        {
            return compareTo(coverage) >= 0;
        }
    }

    enum Cmp
    {
        LE, LT;
    }

    public enum Property
    {
        NOT_OWNED                          (false, false, LE),
        // applied or pre-bootstrap or stale or was owned
        LOCALLY_REDUNDANT                  (true,  true,  LE),

        // was owned OR pre-bootstrap or stale
        LOCALLY_DEFUNCT                    (true,  true,  LE, LOCALLY_REDUNDANT),
        WAS_OWNED                          (true,  false, LE, LOCALLY_DEFUNCT),

        /**
         * We can bootstrap ranges at different times, and have a transaction that participates in both ranges -
         * in this case one of the portions of the transaction may be totally unordered with respect to other transactions
         * in that range because both occur prior to the bootstrappedAt point, so their (local) dependencies are entirely erased.
         * We can also re-bootstrap the same range because bootstrap failed, and leave dangling transactions to execute
         * which then execute in an unordered fashion.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         */
        PRE_BOOTSTRAP_OR_STALE             (false, true,  LT, LOCALLY_DEFUNCT),
        PRE_BOOTSTRAP                      (false, true,  LT, PRE_BOOTSTRAP_OR_STALE),

        LOCALLY_WITNESSED                  (true,  true,  LE),
        // we've applied a sync point locally covering the transaction, but the transaction itself may not have applied
        LOCALLY_SYNCED                     (true,  true,  LE, LOCALLY_REDUNDANT),
        LOCALLY_APPLIED                    (false, false, LE, LOCALLY_SYNCED),

        /**
         * We have fully executed until across all healthy non-bootstrapping replicas for the range in question,
         * but not necessarily ourselves.
         */
        SHARD_ONLY_APPLIED                 (true,  true,  LE),
        SHARD_APPLIED_AND_LOCALLY_REDUNDANT(true,  true,  LE, SHARD_ONLY_APPLIED,                  LOCALLY_REDUNDANT),
        SHARD_APPLIED_AND_LOCALLY_SYNCED   (true,  true,  LE, SHARD_APPLIED_AND_LOCALLY_REDUNDANT, LOCALLY_SYNCED),

        /**
         * We have fully executed across all healthy non-bootstrapping replicas, including ourselves.
         *
         * Note that in some cases we can safely use this property in place of gcBefore for cleaning up or inferring
         * invalidations, but remember that if we are erasing data we may report to peers then we must provide an RX
         * in place of that data to prevent a stale peer thinking they have enough information.
         */
        SHARD_AND_LOCALLY_APPLIED          (false, false, LE, SHARD_APPLIED_AND_LOCALLY_SYNCED,    LOCALLY_APPLIED),
        TRUNCATE_BEFORE                    (false,  true, LT, SHARD_APPLIED_AND_LOCALLY_SYNCED),

        GC_BEFORE                          (false,  true, LT, TRUNCATE_BEFORE),
        ;


        static final Property[] PROPERTIES = values();
        static final Property[] REVERSE_PROPERTIES = values();
        static
        {
            // we have 32 integer bits to use, and we use 2 bits per property. If we exceed this number of properties we need to bump to long.
            Invariants.require(PROPERTIES[PROPERTIES.length - 1].ordinal() < 16);
            for (int i = 0 ; i < REVERSE_PROPERTIES.length / 2 ; ++i)
            {
                REVERSE_PROPERTIES[i] = REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)];
                REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)] = PROPERTIES[i];
            }
        }

        final boolean mergeWithWasOwned;
        final boolean mergeWithPreBootstrapOrStale;
        final Cmp cmp;
        final int compareLessEqual;
        final Property[] implies;

        Property(boolean mergeWithWasOwned, boolean mergeWithPreBootstrapOrStale, Cmp cmp, Property ... implies)
        {
            this.mergeWithWasOwned = mergeWithWasOwned;
            this.mergeWithPreBootstrapOrStale = mergeWithPreBootstrapOrStale;
            this.cmp = cmp;
            this.compareLessEqual = cmp == Cmp.LT ? -1 : 0;
            this.implies = implies;
        }

        final int shift()
        {
            return ordinal() * 2;
        }
    }

    public static final RedundantStatus NONE = new RedundantStatus(0);
    public static final RedundantStatus NOT_OWNED_ONLY = oneSlow(NOT_OWNED);

    public static final RedundantStatus WAS_OWNED_ONLY = oneSlow(WAS_OWNED);
    public static final RedundantStatus WAS_OWNED_LOCALLY_RETIRED = multi(WAS_OWNED, LOCALLY_SYNCED);
    public static final RedundantStatus WAS_OWNED_SHARD_RETIRED = multi(WAS_OWNED, SHARD_APPLIED_AND_LOCALLY_SYNCED);

    public static final RedundantStatus PRE_BOOTSTRAP_ONLY = oneSlow(PRE_BOOTSTRAP);
    public static final RedundantStatus PRE_BOOTSTRAP_OR_STALE_ONLY = oneSlow(PRE_BOOTSTRAP_OR_STALE);

    public static final RedundantStatus LOCALLY_WITNESSED_ONLY = oneSlow(LOCALLY_WITNESSED);
    public static final RedundantStatus LOCALLY_APPLIED_ONLY = oneSlow(LOCALLY_APPLIED);
    public static final RedundantStatus SHARD_ONLY_APPLIED_ONLY = oneSlow(SHARD_ONLY_APPLIED);
    public static final RedundantStatus GC_BEFORE_AND_LOCALLY_APPLIED = multi(GC_BEFORE, LOCALLY_APPLIED);

    final int encoded;
    RedundantStatus(int encoded)
    {
        this.encoded = encoded;
    }

    public RedundantStatus mergeShards(RedundantStatus that)
    {
        int merged = mergeShards(this.encoded, that.encoded);
        return selectOrCreate(merged, this, that);
    }

    public RedundantStatus add(RedundantStatus that)
    {
        int bits = this.encoded | that.encoded;
        return selectOrCreate(bits, this, that);
    }

    static RedundantStatus addHistory(RedundantStatus add, RedundantStatus history)
    {
        int addEncoded = add.encoded;
        if (history.any(PRE_BOOTSTRAP_OR_STALE))
            addEncoded &= PRE_BOOTSTRAP_MERGE_MASK;
        int encoded = addEncoded | history.encoded;
        return selectOrCreate(encoded, history, add);
    }

    static int addHistory(int add, int history)
    {
        if (decode(history, PRE_BOOTSTRAP_OR_STALE) != Coverage.NONE)
            add &= PRE_BOOTSTRAP_MERGE_MASK;
        int merged = add | history;
        // upgrade any combination of LOCAL and SHARD to SHARD_AND_LOCAL
        int upgradeMask = -((merged >>> SHARD_ONLY_APPLIED.shift()) & 1);
        merged |= (upgradeMask & merged & UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_MASK) << UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_SHIFT;
        merged |= (upgradeMask & merged & UPGRADE_SHARD_LOCAL_REDUNDANT_MASK) << UPGRADE_SHARD_LOCAL_REDUNDANT_SHIFT;
        return merged;
    }

    static RedundantStatus selectOrCreate(int encoded, RedundantStatus a, RedundantStatus b)
    {
        if (encoded == a.encoded) return a;
        if (encoded == b.encoded) return b;
        return new RedundantStatus(encoded);
    }

    public Coverage get(Property property)
    {
        return get(encoded, property);
    }

    public static Coverage get(int encoded, Property property)
    {
        return decode(encoded, property);
    }

    public boolean all(Property property)
    {
        return all(encoded, property);
    }

    public static boolean all(int encoded, Property property)
    {
        return get(encoded, property) == ALL;
    }

    public boolean none(Property property)
    {
        return none(encoded, property);
    }

    public static boolean none(int encoded, Property property)
    {
        return get(encoded, property) == Coverage.NONE;
    }

    public boolean any(Property property)
    {
        return any(encoded, property);
    }

    public static boolean any(int encoded, Property property)
    {
        return get(encoded, property) != Coverage.NONE;
    }

    public static boolean matchesMask(int encoded, int propertyMask)
    {
        return (encoded & propertyMask) == propertyMask;
    }

    public static int mask(Property property, Coverage coverage)
    {
        int mask = coverage.ordinal();
        mask |= mask >>> 1;
        return mask << property.shift();
    }

    public static Coverage decode(long encoded, Property property)
    {
        int coverage = (int)((encoded >>> property.shift()) & 0x3);
        switch (coverage)
        {
            default: throw new IllegalStateException("Invalid Coverage value encoded for " + property + ": " + coverage);
            case 0: return Coverage.NONE;
            case 1: return SOME;
            case 3: return ALL;
        }
    }

    private static final int ALL_BITS = 0xAAAAAAAA;
    private static final int ANY_BITS = 0x55555555;
    private static final int WAS_OWNED_NO_MERGE_MASK;
    static final int PRE_BOOTSTRAP_MERGE_MASK;
    static final int ONLY_LE_MASK;
    private static final int WAS_OWNED_MASK = 0x3 << WAS_OWNED.shift();
    private static final int NOT_OWNED_MASK = 0x3 << NOT_OWNED.shift();
    private static final int UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_MASK, UPGRADE_SHARD_LOCAL_REDUNDANT_MASK;
    private static final int UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_SHIFT, UPGRADE_SHARD_LOCAL_REDUNDANT_SHIFT;
    static
    {
        int wasOwnedMask = 0, preBootstrapMask = 0, leMask = 0;
        for (Property property : Property.values())
        {
            if (!property.mergeWithWasOwned)
                wasOwnedMask |= encode(property);
            if (property.mergeWithPreBootstrapOrStale)
                preBootstrapMask |= encode(property);
            if (property.cmp == LE)
                leMask |= encode(property);
        }
        WAS_OWNED_NO_MERGE_MASK = wasOwnedMask;
        PRE_BOOTSTRAP_MERGE_MASK = preBootstrapMask;
        ONLY_LE_MASK = leMask;

        int localAppliedSyncedMask = encode(LOCALLY_APPLIED) | encode(LOCALLY_SYNCED);
        int localRedundantMask = encode(LOCALLY_REDUNDANT);
        int shardAndLocalAppliedSyncedMask = encode(SHARD_AND_LOCALLY_APPLIED) | encode(SHARD_APPLIED_AND_LOCALLY_SYNCED);
        int shardAndLocalRedundantMask = encode(SHARD_APPLIED_AND_LOCALLY_REDUNDANT);
        Invariants.require(numberOfLeadingZeros(localAppliedSyncedMask) + numberOfTrailingZeros(localAppliedSyncedMask) == 28);
        Invariants.require(numberOfLeadingZeros(shardAndLocalAppliedSyncedMask) + numberOfTrailingZeros(shardAndLocalAppliedSyncedMask) == 28);
        UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_MASK = localAppliedSyncedMask;
        UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_SHIFT = numberOfTrailingZeros(shardAndLocalAppliedSyncedMask) - numberOfTrailingZeros(localAppliedSyncedMask);
        UPGRADE_SHARD_LOCAL_REDUNDANT_MASK = localRedundantMask;
        UPGRADE_SHARD_LOCAL_REDUNDANT_SHIFT = numberOfTrailingZeros(shardAndLocalRedundantMask) - numberOfTrailingZeros(localRedundantMask);
        Invariants.require(encode(LOCALLY_APPLIED) << UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_SHIFT == encode(SHARD_AND_LOCALLY_APPLIED));
        Invariants.require(encode(LOCALLY_SYNCED) << UPGRADE_SHARD_LOCAL_APPLIED_SYNCED_SHIFT == encode(SHARD_APPLIED_AND_LOCALLY_SYNCED));
        Invariants.require(encode(LOCALLY_REDUNDANT) << UPGRADE_SHARD_LOCAL_REDUNDANT_SHIFT == encode(SHARD_APPLIED_AND_LOCALLY_REDUNDANT));
    }

    public static int mergeShards(int a, int b)
    {
        int either = a | b;
        Invariants.require((either & NOT_OWNED_MASK) == 0);
        int all = (a & b) & ALL_BITS;
        int any = either & ANY_BITS;
        int result = all | any;
        if ((either & WAS_OWNED_MASK) == WAS_OWNED_MASK)
            result |= either & WAS_OWNED_NO_MERGE_MASK;
        return result;
    }

    public static RedundantStatus oneSlow(Property property)
    {
        return new RedundantStatus(transitiveClosure(property));
    }

    private static RedundantStatus multi(Property ... properties)
    {
        int encoded = 0;
        for (Property property : properties)
            encoded |= transitiveClosure(property);
        return new RedundantStatus(encoded);
    }

    private static int transitiveClosure(Property property)
    {
        int encoded = encode(property);
        for (Property implied : property.implies)
            encoded |= transitiveClosure(implied);
        return encoded;
    }

    static int encode(Property property)
    {
        return 0x3 << (property.shift());
    }

    private static final Coverage[] COVERAGE_TO_STRING = new Coverage[] { ALL, SOME };
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        boolean firstCoverage = true;
        for (Coverage coverage : COVERAGE_TO_STRING)
        {
            if (!firstCoverage) builder.append(',');
            firstCoverage = false;
            builder.append(coverage);
            builder.append(":[");
            int implied = 0;
            boolean firstProperty = true;
            for (Property property : REVERSE_PROPERTIES)
            {
                if (TinyEnumSet.contains(implied, property))
                {
                    implied |= TinyEnumSet.encode(property.implies);
                }
                else if (get(property) == coverage)
                {
                    if (!firstProperty) builder.append(",");
                    firstProperty = false;
                    builder.append(property);
                }
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    public boolean equals(Object that)
    {
        return that != null && that.getClass() == RedundantStatus.class && encoded == ((RedundantStatus) that).encoded;
    }
}
