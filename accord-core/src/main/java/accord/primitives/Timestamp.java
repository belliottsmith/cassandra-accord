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

import accord.local.Node.Id;
import accord.utils.Invariants;
import javax.annotation.Nonnull;

import static accord.primitives.Timestamp.Flag.REJECTED;

/**
 * TxnId flag bits:
 *      [0..1)    - TxnId Domain
 *      [1..4)    - TxnId Kind
 *      [4..6)    - TxnId uses FastPath
 *      [6..8)    - TxnId uses Medium Path
 *      [11..12)  - TxnId is unstable - used by Dep handling only
 *
 * Timestamp flag bits
 *      [11..12)  - isRejected
 *
 */
public class Timestamp implements Comparable<Timestamp>, EpochSupplier
{
    public static final Timestamp MAX = new Timestamp(Long.MAX_VALUE, Long.MAX_VALUE, Id.MAX);
    public static final Timestamp NONE = new Timestamp(0, 0, 0, Id.NONE);

    interface Factory<T extends Timestamp>
    {
        T create(long msb, long lsb, Id id);
    }

    public enum Flag
    {
        /**
         * To be used only by executeAt responses during PreAccept. Can never be taken at the same time as REJECTED.
         */
        REJECTED(0x0800),

        /**
         * To be used only by TxnId inside of Dep collections. Can never be taken at the same time as REJECTED.
         */
        UNSTABLE(0x0800),

        /**
         * An ExclusiveSyncPoint that marks its executeAt with this flag can be used to cleanup HLCs.
         * This means it did not witness any conflicts in a future epoch, and so all transactions that
         * might take a lower HLC must have been witnessed.
         */
        HLC_BOUND(0x0400)
        ;
        public final int bit;

        Flag(int bit)
        {
            this.bit = bit;
        }
    }

    /**
     * The set of flags we want to retain as we merge timestamps (e.g. when taking mergeMax).
     * Today this is only the REJECTED_FLAG, but we may include additional flags in future (such as Committed, Applied..)
     * which we may also want to retain when merging in other contexts (such as in Deps).
     */
    static final int MERGE_FLAGS = 0x0800;
    public static final long IDENTITY_LSB = 0xFFFFFFFF_FFFF00FFL;
    public static final int IDENTITY_FLAGS = 0x00000000_000000FF;
    public static final int KIND_AND_DOMAIN_FLAGS = 0x00000000_0000000F;
    public static final long MAX_EPOCH = (1L << 48) - 1;
    private static final long HLC_INCR = 1L << 16;
    static final long MAX_FLAGS = HLC_INCR - 1;

    public static Timestamp fromBits(long msb, long lsb, Id node)
    {
        return new Timestamp(msb, lsb, node);
    }

    public static Timestamp fromValues(long epoch, long hlc, Id node)
    {
        return new Timestamp(epoch, hlc, 0, node);
    }

    public static Timestamp fromValues(long epoch, long hlc, int flags, Id node)
    {
        return new Timestamp(epoch, hlc, flags, node);
    }

    public static Timestamp maxForEpoch(long epoch)
    {
        return new Timestamp(epochMsb(epoch) | 0x7fff, Long.MAX_VALUE, Id.MAX);
    }

    public static Timestamp minForEpoch(long epoch)
    {
        return new Timestamp(epochMsb(epoch), 0, Id.NONE);
    }

    public final long msb;
    public final long lsb;
    public final Id node;

    Timestamp(long epoch, long hlc, int flags, Id node)
    {
        Invariants.requireArgument(hlc >= 0, "hlc must be positive or zero; given %d", hlc);
        Invariants.requireArgument(epoch <= MAX_EPOCH, "epoch %d > MAX_EPOCH %d", epoch, MAX_EPOCH);
        Invariants.requireArgument(flags <= MAX_FLAGS, "flags %d > MAX_FLAGS %d", flags, MAX_FLAGS);
        this.msb = epochMsb(epoch) | hlcMsb(hlc);
        this.lsb = hlcLsb(hlc) | flags;
        this.node = node;
    }

    Timestamp(long msb, long lsb, Id node)
    {
        this.msb = msb;
        this.lsb = lsb;
        this.node = node;
    }

    public Timestamp(Timestamp copy)
    {
        this.msb = copy.msb;
        this.lsb = copy.lsb;
        this.node = copy.node;
    }

    public Timestamp(Timestamp copy, Id node)
    {
        this.msb = copy.msb;
        this.lsb = copy.lsb;
        this.node = node;
    }

    Timestamp(Timestamp copy, int flags)
    {
        Invariants.requireArgument(flags <= MAX_FLAGS);
        this.msb = copy.msb;
        this.lsb = notFlags(copy.lsb) | flags;
        this.node = copy.node;
    }

    @Override
    public final long epoch()
    {
        return epoch(msb);
    }

    /**
     * A hybrid logical clock with implementation-defined resolution
     */
    public final long hlc()
    {
        return highHlc(msb) | lowHlc(lsb);
    }

    public long uniqueHlc() { return hlc(); }

    public final boolean hasDistinctHlcAndUniqueHlc()
    {
        return getClass() == TimestampWithUniqueHlc.class;
    }

    public final int flags()
    {
        return flags(lsb);
    }

    // if caller masks no need to mask before returning
    final int flagsUnmasked()
    {
        return (int)lsb;
    }

    public boolean is(Flag flag)
    {
        return 0 != (flagsUnmasked() & flag.bit);
    }

    public Timestamp asRejected()
    {
        return addFlag(REJECTED);
    }

    public Timestamp withNextHlc(long hlcAtLeast)
    {
        Invariants.requireArgument(hlcAtLeast >= 0);
        return new Timestamp(epoch(), Math.max(hlcAtLeast, hlc() + 1), flags(), node);
    }

    public Timestamp withEpochAtLeast(long minEpoch)
    {
        return minEpoch <= epoch() ? this : new Timestamp(minEpoch, hlc(), flags(), node);
    }

    public Timestamp withEpoch(long epoch)
    {
        return epoch == epoch() ? this : new Timestamp(epoch, hlc(), flags(), node);
    }

    public Timestamp withNode(Id node)
    {
        return this.node.equals(node) ? this : new Timestamp(this, node);
    }

    public Timestamp addFlags(int flags)
    {
        Invariants.requireArgument(flags <= MAX_FLAGS);
        return addFlags(this, flags, Timestamp::new);
    }

    public Timestamp addFlag(Flag flag)
    {
        return addFlags(flag.bit);
    }

    public final Timestamp addFlags(Timestamp merge)
    {
        return addFlags(this, merge, Timestamp::new);
    }

    static <T extends Timestamp> T addFlags(T to, T from, Factory<T> factory)
    {
        long newLsb = to.lsb | (from.lsb & MERGE_FLAGS);
        if (to.lsb == newLsb)
            return to;
        if (from.lsb == newLsb && to.msb == from.msb && to.node.equals(from.node))
            return from;
        return factory.create(to.msb, newLsb, to.node);
    }

    static <T extends Timestamp> T addFlags(T to, int addFlags, Factory<T> factory)
    {
        long newLsb = to.lsb | addFlags;
        if (to.lsb == newLsb)
            return to;
        return factory.create(to.msb, newLsb, to.node);
    }

    public Timestamp logicalNext(Id node)
    {
        long lsb = this.lsb + HLC_INCR;
        long msb = this.msb;
        if (lowHlc(lsb) == 0)
            ++msb; // overflow of lsb
        return new Timestamp(msb, lsb, node);
    }

    public Timestamp next()
    {
        if (node.id < Long.MAX_VALUE)
            return new Timestamp(msb, lsb, new Id(node.id + 1));
        return logicalNext(Id.NONE);
    }

    @Override
    public int compareTo(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = compareMsb(this.msb, that.msb);
        if (c == 0) c = compareLsb(this.lsb, that.lsb);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public boolean isAtLeastByEpochAndHlc(@Nonnull Timestamp that)
    {
        if (this == that) return true;
        int cmpEpoch = Long.compare(this.epoch(), that.epoch());
        int cmpHlc = Long.compare(this.hlc(), that.hlc());
        if (cmpEpoch < 0 || cmpHlc < 0) return false;
        if (cmpEpoch > 0 || cmpHlc > 0) return true;
        int c = Long.compare(this.lsb & IDENTITY_FLAGS, that.lsb & IDENTITY_FLAGS);
        if (c == 0) c = this.node.compareTo(that.node);
        return c >= 0;
    }

    public static int compareMsb(long msbA, long msbB)
    {
        return Long.compareUnsigned(msbA, msbB);
    }

    public static int compareLsb(long lsbA, long lsbB)
    {
        int c = Long.compare(lowHlc(lsbA), lowHlc(lsbB));
        return c != 0 ? c : Long.compare(lsbA & IDENTITY_FLAGS, lsbB & IDENTITY_FLAGS);
    }

    public int compareToWithoutEpoch(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compare(highHlc(this.msb), highHlc(that.msb));
        if (c == 0) c = compareLsb(this.lsb, that.lsb);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public int compareToStrict(@Nonnull Timestamp that)
    {
        if (this == that) return 0;
        int c = Long.compareUnsigned(this.msb, that.msb);
        if (c == 0) c = Long.compare(lowHlc(this.lsb), lowHlc(that.lsb));
        if (c == 0) c = Long.compare(this.uniqueHlc(), that.uniqueHlc());
        if (c == 0) c = Long.compare(this.flags(), that.flags());
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    public Timestamp flattenUniqueHlc()
    {
        if (!hasDistinctHlcAndUniqueHlc())
            return this;
        return new Timestamp(epoch(), uniqueHlc(), flags(), node);
    }

    @Override
    public int hashCode()
    {
        return (int) (((msb * 31) + lowHlc(lsb)) * 31) + node.hashCode();
    }

    public boolean equals(Timestamp that)
    {
        return that != null && this.msb == that.msb
               && ((this.lsb ^ that.lsb) & IDENTITY_LSB) == 0
               && this.node.equals(that.node);
    }

    /**
     * Include flag bits in identity and any uniqueHlc
     */
    public boolean equalsStrict(Timestamp that)
    {
        return this.msb == that.msb && this.lsb == that.lsb && this.node.equals(that.node) && uniqueHlc() == that.uniqueHlc();
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof Timestamp && equals((Timestamp) that);
    }

    public static <T extends Timestamp> T max(T a, T b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    /**
     * The resulting timestamp will have max(a.hlc,b.hlc) and max(a.epoch, b.epoch),
     * and any mergeable flag in either a or b
     */
    public static Timestamp mergeMaxAndFlags(Timestamp a, Timestamp b)
    {
        // Note: it is not safe to take the highest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        return a.compareToWithoutEpoch(b) >= 0 ? a.addFlags(b).withEpochAtLeast(b.epoch())
                                               : b.addFlags(a).withEpochAtLeast(a.epoch());
    }

    /**
     * The resulting timestamp will have max(a.hlc,b.hlc) and max(a.epoch, b.epoch)
     */
    public static Timestamp mergeMax(Timestamp a, Timestamp b)
    {
        // Note: it is not safe to take the highest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        return a.compareToWithoutEpoch(b) >= 0 ? a.withEpochAtLeast(b.epoch())
                                               : b.withEpochAtLeast(a.epoch());
    }

    public static <T extends Timestamp> T nonNullOrMax(T a, T b)
    {
        return a == null ? b : b == null ? a : max(a, b);
    }

    public static <T extends Timestamp> T nonNullOrMin(T a, T b)
    {
        return a == null ? b : b == null ? a : min(a, b);
    }

    public static <T extends Timestamp> T min(T a, T b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static long epoch(long msb)
    {
        return msb >>> 15;
    }

    static long epochMsb(long epoch)
    {
        return epoch << 15;
    }

    private static long hlcMsb(long hlc)
    {
        return hlc >>> 48;
    }

    private static long hlcLsb(long hlc)
    {
        return hlc << 16;
    }

    private static long highHlc(long msb)
    {
        return (msb & 0x7fff) << 48;
    }

    private static long lowHlc(long lsb)
    {
        return lsb >>> 16;
    }

    private static int flags(long lsb)
    {
        return (int) (lsb & MAX_FLAGS);
    }

    private static long notFlags(long lsb)
    {
        return lsb & ~MAX_FLAGS;
    }

    public Timestamp merge(Timestamp that)
    {
        return merge(this, that, Timestamp::fromBits);
    }

    interface Constructor<T>
    {
        T construct(long msb, long lsb, Id node);
    }

    static <T extends Timestamp> T merge(Timestamp a, Timestamp b, Constructor<T> constructor)
    {
        Invariants.requireArgument(a.msb == b.msb);
        Invariants.requireArgument(lowHlc(a.lsb) == lowHlc(b.lsb));
        Invariants.requireArgument(a.node.equals(b.node));
        return constructor.construct(a.msb, a.lsb | b.lsb, a.node);
    }

    @Override
    public String toString()
    {
        return toStandardString();
    }

    public String toStandardString()
    {
        long hlc = hlc();
        long uniqueHlc = uniqueHlc();
        return "[" + epoch() + ',' + hlc + (hlc == uniqueHlc ? "" : "+" + (uniqueHlc - hlc)) + ',' + flags() + ',' + node + ']';
    }

    public static Timestamp fromString(String string)
    {
        String[] split = string.replaceFirst("\\[", "").replaceFirst("\\]", "").split(",");
        assert split.length == 4;
        int indexOfUniqueHlc = split[2].indexOf('+');
        int flags = Integer.parseInt(indexOfUniqueHlc < 0 ? split[2] : split[2].substring(0, indexOfUniqueHlc));
        Timestamp result = Timestamp.fromValues(Long.parseLong(split[0]),
                                                Long.parseLong(split[1]),
                                                flags,
                                                new Id(Integer.parseInt(split[3])));
        if (indexOfUniqueHlc < 0)
            return result;
        return new TimestampWithUniqueHlc(result, Integer.parseInt(split[2].substring(indexOfUniqueHlc + 1)));
    }
}
