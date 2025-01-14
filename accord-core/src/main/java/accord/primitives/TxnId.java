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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import accord.local.Node.Id;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;

import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.illegalArgument;

public class TxnId extends Timestamp
{
    public enum FastPath
    {
        UNOPTIMISED,
        PRIVILEGED_COORDINATOR_WITHOUT_DEPS,
        PRIVILEGED_COORDINATOR_WITH_DEPS;

        private static final FastPaths IS_PRIVILEGED = new FastPaths(PRIVILEGED_COORDINATOR_WITH_DEPS, PRIVILEGED_COORDINATOR_WITHOUT_DEPS);
        private static final FastPath[] VALUES = values();

        public final int bits;

        FastPath()
        {
            this.bits = ordinal() << 4;
        }

        public static FastPath forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }

        static boolean hasPrivilegedCoordinator(int ordinal)
        {
            return IS_PRIVILEGED.contains(ordinal);
        }

        public FastPath toPermitted(FastPaths permitted)
        {
            if (permitted.contains(this))
                return this;
            if (this == UNOPTIMISED)
                return this;
            FastPath alt = this == PRIVILEGED_COORDINATOR_WITH_DEPS ? PRIVILEGED_COORDINATOR_WITHOUT_DEPS : PRIVILEGED_COORDINATOR_WITH_DEPS;
            if (permitted.contains(alt))
                return alt;
            return UNOPTIMISED;
        }
    }

    public static class FastPaths extends TinyEnumSet<FastPath>
    {
        public FastPaths(FastPath... values) { super(values); }

        public boolean hasPrivilegedCoordinator() { return bitset > 1; }
    }

    public enum MediumPath
    {
        NONE(0),
        MEDIUM_PATH_WAIT_ON_RECOVERY(1),
        MEDIUM_PATH_TRACK_STABLE(2);

        private static final MediumPath[] VALUES = values();

        public final int bits;

        MediumPath(int bits)
        {
            this.bits = bits << 6;
        }

        public static boolean any(int bits)
        {
            return bits != 0;
        }

        public static MediumPath forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    private static final int DOMAIN_AND_KIND_MASK = 0xf;
    public static final TxnId[] NO_TXNIDS = new TxnId[0];

    public static final TxnId NONE = new TxnId(0, 0, Id.NONE);
    public static final TxnId MAX = new TxnId(Long.MAX_VALUE, Long.MAX_VALUE, Id.MAX);

    public static TxnId fromBits(long msb, long lsb, Id node)
    {
        return new TxnId(msb, lsb, node);
    }

    public static TxnId fromValues(long epoch, long hlc, Id node)
    {
        return new TxnId(epoch, hlc, 0, node);
    }

    public static TxnId fromValues(long epoch, long hlc, int flags, Id node)
    {
        return new TxnId(epoch, hlc, flags, node);
    }

    public static TxnId fromValues(long epoch, long hlc, int flags, int node)
    {
        return new TxnId(epoch, hlc, flags, new Id(node));
    }

    public TxnId(Timestamp timestamp, Kind rw, Domain domain)
    {
        this(timestamp, 0, rw, domain);
    }

    public TxnId(Timestamp timestamp, int flags, Kind rw, Domain domain)
    {
        super(timestamp, flags(flags, rw, domain));
    }

    public TxnId(TxnId copy)
    {
        super(copy.msb, copy.lsb, copy.node);
    }

    public TxnId(long epoch, long hlc, Kind rw, Domain domain, Id node)
    {
        this(epoch, hlc, 0, rw, domain, node);
    }

    public TxnId(long epoch, long hlc, int flags, Kind rw, Domain domain, Id node)
    {
        this(epoch, hlc, flags(flags, rw, domain), node);
    }

    private TxnId(long epoch, long hlc, int flags, Id node)
    {
        super(epoch, hlc, flags, node);
    }

    private TxnId(long msb, long lsb, Id node)
    {
        super(msb, lsb, node);
    }

    public boolean isWrite()
    {
        return is(Write);
    }

    public boolean isRead()
    {
        return is(Read);
    }

    public Kind kind()
    {
        return kind(flagsUnmasked());
    }

    public boolean is(Kinds kinds)
    {
        return kinds.testOrdinal(kindOrdinal(flagsUnmasked()));
    }

    public boolean is(Kind kind)
    {
        return kindOrdinal(flagsUnmasked()) == kind.ordinal();
    }

    public boolean is(FastPath fastPath)
    {
        return 0 != (flagsUnmasked() & fastPath.bits);
    }

    public boolean is(MediumPath mediumPath)
    {
        return 0 != (flagsUnmasked() & mediumPath.bits);
    }

    public TxnId withoutNonIdentityFlags()
    {
        return (flags() & ~IDENTITY_FLAGS) == 0 ? this : new TxnId(msb, lsb & IDENTITY_LSB, node);
    }

    public boolean hasPrivilegedCoordinator()
    {
        return FastPath.hasPrivilegedCoordinator(fastPathOrdinal(flagsUnmasked()));
    }

    public boolean hasMediumPath()
    {
        return MediumPath.any(mediumPathOrdinal(flagsUnmasked()));
    }

    public boolean isVisible()
    {
        return Kind.isVisible(kindOrdinal(flagsUnmasked()));
    }

    public boolean isSyncPoint()
    {
        return Kind.isSyncPoint(kindOrdinal(flagsUnmasked()));
    }

    public boolean isSystemTxn()
    {
        return Kind.isSystemTxn(kindOrdinal(flagsUnmasked()));
    }

    public boolean awaitsOnlyDeps()
    {
        return Kind.awaitsOnlyDeps(kindOrdinal(flagsUnmasked()));
    }

    public boolean awaitsPreviouslyOwned()
    {
        return Kind.awaitsPreviouslyOwned(kindOrdinal(flagsUnmasked()));
    }

    public boolean is(Domain domain)
    {
        return domainOrdinal(flagsUnmasked()) == domain.ordinal();
    }

    public Kinds witnesses()
    {
        return kind().witnesses();
    }

    public boolean witnesses(TxnId txnId)
    {
        return Kind.witnesses(kindOrdinal(flagsUnmasked()), kindOrdinal(txnId.flagsUnmasked()));
    }

    public boolean witnessedBy(TxnId txnId)
    {
        return txnId.witnesses(this);
    }

    public Kinds witnessedBy()
    {
        return kind().witnessedBy();
    }

    public Domain domain()
    {
        return domain(flagsUnmasked());
    }

    public TxnId as(Kind kind)
    {
        return as(kind, domain());
    }

    public TxnId as(Domain domain)
    {
        return as(kind(), domain);
    }

    public TxnId as(Kind kind, Domain domain)
    {
        return new TxnId(epoch(), hlc(), flagsWithoutDomainAndKind(), kind, domain, node);
    }

    private int flagsWithoutDomainAndKind()
    {
        return flags() & ~DOMAIN_AND_KIND_MASK;
    }

    public TxnId addFlags(int flags)
    {
        checkArgument(flags <= MAX_FLAGS);
        return addFlags(this, flags, TxnId::new);
    }

    public TxnId addFlag(Flag flag)
    {
        return addFlags(flag.bit);
    }

    public final TxnId addFlags(TxnId merge)
    {
        return addFlags(this, merge, TxnId::new);
    }

    public TxnId withEpoch(long epoch)
    {
        return epoch == epoch() ? this : new TxnId(epoch, hlc(), flags(), node);
    }

    @Override
    public TxnId merge(Timestamp that)
    {
        return merge(this, that, TxnId::fromBits);
    }

    @Override
    public String toString()
    {
        return "[" + epoch() + ',' + hlc() + ',' + flags() + '(' + domain().shortName() + kind().shortName() + ')' + ',' + node + ']';
    }

    private static int flags(int flags, Kind rw, Domain domain)
    {
        int domainAndKindFlags = flags(rw) | flags(domain);
        Invariants.checkState((flags & domainAndKindFlags) == 0);
        return domainAndKindFlags | flags;
    }

    private static int flags(Kind rw)
    {
        return rw.ordinal() << 1;
    }

    private static int flags(Domain domain)
    {
        return domain.ordinal();
    }

    private static Kind kind(int flags)
    {
        return Kind.ofOrdinal(kindOrdinal(flags));
    }

    private static Domain domain(int flags)
    {
        return Domain.ofOrdinal(domainOrdinal(flags));
    }

    public static int kindOrdinal(int flags)
    {
        return (flags >> 1) & 7;
    }

    public FastPath fastPath()
    {
        return FastPath.forOrdinal(fastPathOrdinal(flagsUnmasked()));
    }

    public static int fastPathOrdinal(int flags)
    {
        return (flags >>> 4) & 3;
    }

    public static int mediumPathOrdinal(int flags)
    {
        return (flags >>> 6) & 3;
    }

    private static int domainOrdinal(int flags)
    {
        return flags & 1;
    }

    public static TxnId maxForEpoch(long epoch)
    {
        return new TxnId(epochMsb(epoch) | 0x7fff, Long.MAX_VALUE, Id.MAX);
    }

    public static TxnId minForEpoch(long epoch)
    {
        return new TxnId(epochMsb(epoch), 0, Id.NONE);
    }

    private static final Pattern PARSE = Pattern.compile("\\[(?<epoch>[0-9]+),(?<hlc>[0-9]+),(?<flags>[0-9]+)\\([KR][REWSXL]\\),(?<node>[0-9]+)]");
    public static TxnId parse(String txnIdString)
    {
        Matcher m = PARSE.matcher(txnIdString);
        if (!m.matches())
            throw illegalArgument("Invalid TxnId string: " + txnIdString);
        return fromValues(Long.parseLong(m.group("epoch")), Long.parseLong(m.group("hlc")), Integer.parseInt(m.group("flags")), new Id(Integer.parseInt(m.group("node"))));
    }
}
