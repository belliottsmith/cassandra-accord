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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.primitives.TxnId.FastPaths;
import accord.primitives.TxnId.MediumPath;
import accord.utils.Invariants;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.Chase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.DoNotChase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Unsynced;
import static accord.api.ProtocolModifiers.Toggles.DependencyElision.IF_DURABLE;
import static accord.api.ProtocolModifiers.Toggles.InformOfDurability.ALL;

/**
 * Configure various protocol behaviours. Many of these switches are correctness impacting, and should not be touched.
 */
public class ProtocolModifiers
{
    public static class QuorumEpochIntersections
    {
        public enum ChaseFixedPoint
        {
            Chase, DoNotChase;
        }
        public enum Include
        {
            Unsynced, Owned;
        }

        public static class ChaseAndInclude
        {
            public final ChaseFixedPoint chase;
            public final Include include;

            public ChaseAndInclude(ChaseFixedPoint chase, Include include)
            {
                this.chase = chase;
                this.include = include;
            }
        }

        static class Spec
        {
            private static final Pattern PARSE = Pattern.compile("(preaccept|accept|commit|stable|recover)=([+-]{0,2})");

            final ChaseAndInclude preaccept;
            final Include accept, commit, stable, recover;
            final Include preacceptOrRecover, preacceptOrCommit;

            Spec(ChaseAndInclude preaccept, Include accept, Include commit, Include stable, Include recover)
            {
                this.preaccept = preaccept;
                Invariants.require(preaccept.chase == DoNotChase, "PreAccept chasing epoch is not implemented as not needed for current formulation and it complicated some aspects. An implementation can be found in git history.");
                this.accept = accept;
                this.commit = commit;
                this.stable = stable;
                this.recover = recover;
                this.preacceptOrRecover = preaccept.include == Owned || recover == Owned ? Owned : Unsynced;
                this.preacceptOrCommit = preaccept.include == Owned || commit == Owned ? Owned : Unsynced;
            }

            static Spec parse(String description)
            {
                Matcher m = PARSE.matcher(description);
                ChaseAndInclude preaccept = null;
                Include accept = null;
                Include commit = null;
                Include stable = null;
                Include recover = null;
                while (m.find())
                {
                    ChaseFixedPoint cfp = DoNotChase;
                    Include include = Owned;
                    String str = m.group(2);
                    for (int i = 0 ; i < str.length() ; ++i)
                    {
                        switch (str.charAt(i))
                        {
                            default: throw new AssertionError("Unexpected char: '" + str.charAt(i) + "'");
                            case '+': cfp = Chase; break;
                            case '-': include = Unsynced; break;
                        }
                    }

                    switch (m.group(1))
                    {
                        default: throw new AssertionError("Unexpected phase: " + m.group(1));
                        case "preaccept": preaccept = new ChaseAndInclude(cfp, include); break;
                        case "accept": accept = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for accept"); break;
                        case "commit": commit = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for commit"); break;
                        case "stable": stable = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for stable"); break;
                        case "recover": recover = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for stable"); break;
                    }
                }

                Invariants.require(preaccept != null, "preaccept not specified for quorum epoch intersections: " + description);
                Invariants.require(accept != null, "accept not specified for quorum epoch intersections: " + description);
                Invariants.require(commit != null, "commit not specified for quorum epoch intersections: " + description);
                Invariants.require(stable != null, "stable not specified for quorum epoch intersections: " + description);
                Invariants.require(recover != null, "recover not specified for quorum epoch intersections: " + description);
                return new Spec(preaccept, accept, commit, stable, recover);
            }
        }

        static
        {
            Spec spec = Spec.parse(System.getProperty("accord.quorums.epochs", "preaccept=-,accept=-,commit=-,stable=,recover=-"));
            preaccept = spec.preaccept;
            accept = spec.accept;
            commit = spec.commit;
            stable = spec.stable;
            recover = spec.recover;
            preacceptOrRecover = spec.preacceptOrRecover;
            preacceptOrCommit = spec.preacceptOrCommit;
        }

        // TODO (desired): support fixed-point chasing for recovery
        public static final ChaseAndInclude preaccept;
        public static final Include accept, commit, stable, recover;
        public static final Include preacceptOrRecover, preacceptOrCommit;
    }

    public static class Faults
    {
        static class Spec
        {
            // note that {txn,syncPoint}DiscardPreAcceptDeps faults expect filterDuplicateDependenciesFromAcceptReply to be off
            final boolean txnInstability, txnDiscardPreAcceptDeps;
            final boolean syncPointInstability, syncPointDiscardPreAcceptDeps;

            Spec(boolean txnInstability, boolean txnDiscardPreAcceptDeps, boolean syncPointInstability, boolean syncPointDiscardPreAcceptDeps)
            {
                this.txnInstability = txnInstability;
                this.txnDiscardPreAcceptDeps = txnDiscardPreAcceptDeps;
                this.syncPointInstability = syncPointInstability;
                this.syncPointDiscardPreAcceptDeps = syncPointDiscardPreAcceptDeps;
            }
        }

        static
        {
            // TODO (expected): configurable
            Spec spec = new Spec(false, false, false, false);
            txnInstability = spec.txnInstability;
            txnDiscardPreAcceptDeps = spec.txnDiscardPreAcceptDeps;
            syncPointInstability = spec.syncPointInstability;
            syncPointDiscardPreAcceptDeps = spec.syncPointDiscardPreAcceptDeps;
        }

        public static final boolean txnInstability, txnDiscardPreAcceptDeps;
        public static final boolean syncPointInstability, syncPointDiscardPreAcceptDeps;

        public static boolean discardPreAcceptDeps(TxnId txnId)
        {
            return (txnDiscardPreAcceptDeps | syncPointDiscardPreAcceptDeps)
                   && (txnId.isSyncPoint() ? syncPointDiscardPreAcceptDeps : txnDiscardPreAcceptDeps);
        }
    }

    public static class Toggles
    {
        private static FastPaths permittedFastPaths = new FastPaths(FastPath.values());
        public static boolean usePrivilegedCoordinator() { return permittedFastPaths.hasPrivilegedCoordinator(); }
        public static void setPermittedFastPaths(FastPaths newPermittedFastPaths) { permittedFastPaths = newPermittedFastPaths; }
        public static FastPath ensurePermitted(FastPath path) { return path.toPermitted(permittedFastPaths); }

        private static MediumPath defaultMediumPath = MediumPath.MEDIUM_PATH_WAIT_ON_RECOVERY;
        public static MediumPath defaultMediumPath() { return defaultMediumPath; }
        public static void setDefaultMediumPath(MediumPath newDefaultMediumPath) { defaultMediumPath = newDefaultMediumPath; }

        private static boolean filterDuplicateDependenciesFromAcceptReply = true;
        public static boolean filterDuplicateDependenciesFromAcceptReply() { return filterDuplicateDependenciesFromAcceptReply; }
        public static void setFilterDuplicateDependenciesFromAcceptReply(boolean newFilterDuplicateDependenciesFromAcceptReply) { filterDuplicateDependenciesFromAcceptReply = newFilterDuplicateDependenciesFromAcceptReply; }

        private static boolean permitLocalExecution = true;
        public static boolean permitLocalExecution() { return permitLocalExecution; }
        public static void setPermitLocalExecution(boolean newPermitLocalExecution) { permitLocalExecution = newPermitLocalExecution; }

        private static boolean requiresUniqueHlcs = true;
        public static boolean requiresUniqueHlcs() { return requiresUniqueHlcs; }
        public static void setRequiresUniqueHlcs(boolean newRequiresUniqueHlcs) { requiresUniqueHlcs = newRequiresUniqueHlcs; }

        public enum DependencyElision { OFF, ON, IF_DURABLE }
        private static DependencyElision dependencyElision = IF_DURABLE;
        public static DependencyElision dependencyElision() { return dependencyElision; }
        public static void setDependencyElision(DependencyElision newDependencyElision) { dependencyElision = newDependencyElision; }

        public enum InformOfDurability { HOME, ALL }
        private static InformOfDurability informOfDurability = ALL;
        public static InformOfDurability informOfDurability() { return informOfDurability; }
        public static void setInformOfDurability(InformOfDurability newInformOfDurability) { informOfDurability = newInformOfDurability; }
    }
}
