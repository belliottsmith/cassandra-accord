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

import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.RedundantStatus.Coverage.SOME;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.Property.REVERSE_PROPERTIES;
import static accord.local.RedundantStatus.Property.SHARD_AND_LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.TRUNCATE_BEFORE;
import static accord.local.RedundantStatus.Property.WAS_OWNED;

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

    public enum Property
    {
        NOT_OWNED                          (false),
        // applied or pre-bootstrap or stale or was owned
        LOCALLY_REDUNDANT                  ( true),

        // was owned OR pre-bootstrap or stale
        LOCALLY_DEFUNCT                    ( true, LOCALLY_REDUNDANT),
        WAS_OWNED                          ( true, LOCALLY_DEFUNCT),
        PRE_BOOTSTRAP_OR_STALE             (false, LOCALLY_DEFUNCT),

        // we've applied a sync point locally covering the transaction, but the transaction itself may not have applied
        LOCALLY_SYNCED                     ( true, LOCALLY_REDUNDANT),
        LOCALLY_APPLIED                    (false, LOCALLY_SYNCED),

        SHARD_APPLIED_ONLY                 ( true),
        SHARD_APPLIED_AND_LOCALLY_REDUNDANT( true, SHARD_APPLIED_ONLY,                  LOCALLY_REDUNDANT),
        SHARD_APPLIED_AND_LOCALLY_SYNCED   ( true, SHARD_APPLIED_AND_LOCALLY_REDUNDANT, LOCALLY_SYNCED),
        SHARD_AND_LOCALLY_APPLIED          (false, SHARD_APPLIED_AND_LOCALLY_SYNCED,    LOCALLY_APPLIED),
        TRUNCATE_BEFORE                    (false, SHARD_AND_LOCALLY_APPLIED),
        GC_BEFORE                          (false, TRUNCATE_BEFORE),
        ;

        static final Property[] PROPERTIES = values();
        static final Property[] REVERSE_PROPERTIES = values();
        static
        {
            for (int i = 0 ; i < REVERSE_PROPERTIES.length / 2 ; ++i)
            {
                REVERSE_PROPERTIES[i] = REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)];
                REVERSE_PROPERTIES[REVERSE_PROPERTIES.length - (1 + i)] = PROPERTIES[i];
            }
        }

        final boolean mergeWithWasOwned;
        final Property[] implies;

        Property(boolean mergeWithWasOwned, Property ... implies)
        {
            this.mergeWithWasOwned = mergeWithWasOwned;
            this.implies = implies;
        }

        final int shift()
        {
            return ordinal() * 2;
        }
    }

    public static final RedundantStatus NONE = new RedundantStatus(0);
    public static final RedundantStatus NOT_OWNED_ONLY = one(NOT_OWNED);

    public static final RedundantStatus WAS_OWNED_ONLY = one(WAS_OWNED);
    public static final RedundantStatus WAS_OWNED_LOCALLY_RETIRED = multi(WAS_OWNED, LOCALLY_SYNCED);
    public static final RedundantStatus WAS_OWNED_SHARD_RETIRED = multi(WAS_OWNED, SHARD_APPLIED_AND_LOCALLY_SYNCED);

    public static final RedundantStatus PRE_BOOTSTRAP_OR_STALE_ONLY = one(PRE_BOOTSTRAP_OR_STALE);
    public static final RedundantStatus LOCALLY_SYNCED_AND_PRE_BOOTSTRAP_OR_STALE = multi(LOCALLY_SYNCED, PRE_BOOTSTRAP_OR_STALE);
    public static final RedundantStatus SHARD_APPLIED_AND_LOCALLY_SYNCED_AND_PRE_BOOTSTRAP_OR_STALE = multi(SHARD_APPLIED_AND_LOCALLY_SYNCED, PRE_BOOTSTRAP_OR_STALE);
    public static final RedundantStatus SHARD_ONLY_APPLIED_AND_PRE_BOOTSTRAP_OR_STALE = multi(SHARD_APPLIED_AND_LOCALLY_REDUNDANT, PRE_BOOTSTRAP_OR_STALE);

    public static final RedundantStatus LOCALLY_REDUNDANT_ONLY = one(LOCALLY_REDUNDANT);
    public static final RedundantStatus SHARD_AND_LOCALLY_APPLIED_ONLY = one(SHARD_AND_LOCALLY_APPLIED);
    public static final RedundantStatus TRUNCATE_BEFORE_ONLY = one(TRUNCATE_BEFORE);
    public static final RedundantStatus GC_BEFORE_ONLY = one(GC_BEFORE);

    final long encoded;
    RedundantStatus(long encoded)
    {
        this.encoded = encoded;
    }

    public RedundantStatus merge(RedundantStatus that)
    {
        long merged = merge(this.encoded, that.encoded);
        if (merged == this.encoded)
            return this;
        if (merged == that.encoded)
            return that;
        return new RedundantStatus(merged);
    }

    public RedundantStatus add(RedundantStatus that)
    {
        return new RedundantStatus(this.encoded | that.encoded);
    }

    public Coverage get(Property property)
    {
        return decode(encoded, property);
    }

    public boolean all(Property property)
    {
        return get(property) == ALL;
    }

    public boolean none(Property property)
    {
        return get(property) == Coverage.NONE;
    }

    public boolean any(Property property)
    {
        return get(property) != Coverage.NONE;
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

    private static final long ALL_BITS = 0xAAAAAAAAAAAAAAAAL;
    private static final long ANY_BITS = 0x5555555555555555L;
    private static final long WAS_OWNED_MERGE_MASK;
    private static final long WAS_OWNED_MASK = 0x3L << WAS_OWNED.shift();
    private static final long NOT_OWNED_MASK = 0x3L << NOT_OWNED.shift();
    static
    {
        long mask = 0;
        for (Property property : Property.values())
        {
            if (!property.mergeWithWasOwned)
                mask |= (0x3L << property.shift());
        }
        WAS_OWNED_MERGE_MASK = mask;
    }

    public static long merge(long a, long b)
    {
        long either = a | b;
        Invariants.require((either & NOT_OWNED_MASK) == 0);
        long all = (a & b) & ALL_BITS;
        long any = either & ANY_BITS;
        long result = all | any;
        if ((either & WAS_OWNED_MASK) == WAS_OWNED_MASK)
            result |= either & WAS_OWNED_MERGE_MASK;
        return result;
    }

    private static RedundantStatus one(Property property)
    {
        return new RedundantStatus(transitiveClosure(property));
    }

    private static RedundantStatus multi(Property ... properties)
    {
        long encoded = 0;
        for (Property property : properties)
            encoded |= transitiveClosure(property);
        return new RedundantStatus(encoded);
    }

    private static long transitiveClosure(Property property)
    {
        long encoded = 0x3L << (property.shift());
        for (Property implied : property.implies)
            encoded |= transitiveClosure(implied);
        return encoded;
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
}
