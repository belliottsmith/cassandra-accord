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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import accord.local.LoadKeys;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.TxnId.FastPath;
import accord.primitives.TxnId.FastPaths;
import accord.primitives.TxnId.MediumPath;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.CleanCfkBefore.SHARD_APPLIED;
import static accord.api.ProtocolModifiers.DependencyElision.IF_DURABLY_COMMITTED;
import static accord.api.ProtocolModifiers.FastExecution.MAY_BYPASS_COMMANDSFORKEY;
import static accord.api.ProtocolModifiers.FastExecution.MAY_BYPASS_SAFESTORE;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.Chase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.DoNotChase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Unsynced;
import static accord.api.ProtocolModifiers.SendStableMessages.FOR_READS;
import static accord.api.ProtocolModifiers.SendStableMessages.FOR_READS_OR_NONE_IF_FASTEXEC;
import static accord.api.ProtocolModifiers.SendStableMessages.TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS;
import static accord.local.cfk.CommandsForKey.managesExecution;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.utils.Invariants.illegalState;

/**
 * Configure various protocol behaviours. Many of these switches are correctness impacting, and should not be touched.
 */
public class ProtocolModifiers
{
    public enum SendStableMessages
    {
        /**
         * All participating replicas will receive a Stable message once the transaction begins the execution phase.
         * This is potentially expensive and may harm throughput by increasing the number of messages and transaction
         * state updates we need to perform, but it guarantees the fastest forward progress for any particular transaction.
         */
        TO_ALL,

        /**
         * If the transaction is able to be direct executed, send Stable messages to all replicas so that they may do so.
         * Otherwise, behaves as {@link #FOR_READS}.
         */
        TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS,

        /**
         * Thrifty execution. Send a Stable message only to replicas we want to read from, so that they may execute the read.
         * The coordinator may then send the Apply message to all replicas, saving O(n) messages.
         */
        FOR_READS,

        /**
         * Even thriftier: if the read is permitted to fast execute, there is no need to send a Stable message;
         * we can simply perform the read and permit the coordinator to send the Apply message. Otherwise
         * identical to {@link #FOR_READS}
         */
        FOR_READS_OR_NONE_IF_FASTEXEC
    }

    public enum CleanCfkBefore { SHARD_APPLIED, GC }

    public enum DependencyElision { OFF, ALWAYS, IF_DURABLY_COMMITTED, IF_DURABLY_PREAPPLIED }

    public enum AbandonFastPath { IF_ANY_DELAYED_OR_REJECTED, IF_FAST_QUORUM_DELAYED_OR_REJECTED }

    public enum FastExecution { DISABLED, MAY_BYPASS_COMMANDSFORKEY, MAY_BYPASS_SAFESTORE }

    public enum CoordinatorBacklogExecution
    {
        DISABLED,
        /** Recovery coordinators continue to execute the backlog behind the transaction they recover */
        ON_RECOVERY,
        /** Coordinators that execute locally continue to execute any backlog behind their transaction */
        ALWAYS,
        /**
         * A designated replica of each transaction's home key executes the transaction on behalf of its original coordinator
         * once it becomes ready to execute, if the coordinator is not nearby (see {@link accord.local.cfk.ExecuteTxnBacklog}).
         * Unless {@link #coordinatorBacklogExecutionPerDc()}, there is one designated replica, and coordinators in other
         * DCs defer to it; otherwise there is one per DC, and coordinators do not defer.
         * Must not be enabled until every replica supports {@link accord.messages.ReadTxnDataIfReady} without
         * {@link accord.coordinate.ExecuteFlag#NO_WAIT}.
         */
        DESIGNATED
    }

    /**
     * How much information to include in messages, assuming earlier messages have been delivered.
     * Less information is more efficient, but may lead to additional traffic and latency when earlier messages
     * have not arrived, as the recipient must request (or the coordinator must resend) a more complete message.
     */
    public enum SendMinimal
    {
        /** Always include everything a recipient might need */
        MAXIMAL,
        /** Include the least information expected to be necessary, but always include deps with Apply */
        MINIMAL,
        /**
         * As MINIMAL, but also omit deps from Apply messages to replicas we expect to have been sent a Stable message
         * (see {@link accord.messages.Apply.Kind#NoDeps}). Must not be enabled until every replica understands NoDeps.
         */
        MINIMAL_NO_DEPS
    }

    public enum InformOfDurability { NONE, HOME, ALL }

    public enum ReplicaExecution { NONE, BLIND_WRITE, ALL }
    public enum UniqueTimestampOnConflict { STALE, NOW }

    public static class Configure
    {
        private static boolean isConfigured;
        private static synchronized void setConfigured() { isConfigured = true; }
        private static void pre() { if (isConfigured) throw illegalState("Cannot set ProtocolModifiers after the configuration has been first used"); }

        private static RateLimitedLogStatement.Factory rateLimitedLogStatementFactory = new RateLimitedLogStatement.NoLimitFactory();
        public static synchronized void setRateLimitedLogStatementFactory(RateLimitedLogStatement.Factory newFactory) { pre(); rateLimitedLogStatementFactory = newFactory; }

        private static boolean rangeEndInclusive = true;
        public static synchronized void setRangeEndInclusive(boolean newRangeEndInclusive) { pre(); rangeEndInclusive = newRangeEndInclusive; }

        private static FastPaths permittedFastPaths = new FastPaths(FastPath.values());
        public static synchronized void setPermittedFastPaths(FastPaths newPermittedFastPaths) { pre(); permittedFastPaths = newPermittedFastPaths; }

        private static MediumPath defaultMediumPath = MediumPath.TrackStable;
        public static synchronized void setDefaultMediumPath(MediumPath newDefaultMediumPath) { pre(); defaultMediumPath = newDefaultMediumPath; }

        private static boolean permitFastQuorumMediumPath = true;
        public static synchronized void setPermitFastQuorumMediumPath(boolean newPermitFastQuorumMediumPath) { pre(); permitFastQuorumMediumPath = newPermitFastQuorumMediumPath; }

        private static AbandonFastPath abandonFastPath = AbandonFastPath.IF_ANY_DELAYED_OR_REJECTED;
        public static synchronized void setAbandonFastPath(AbandonFastPath newAbandonFastPath) { pre(); abandonFastPath = newAbandonFastPath; }

        private static UniqueTimestampOnConflict uniqueTimestampOnConflict = UniqueTimestampOnConflict.STALE;
        public static synchronized void setUniqueTimestampOnConflict(UniqueTimestampOnConflict newUniqueTimestampOnConflict) { pre();uniqueTimestampOnConflict = newUniqueTimestampOnConflict; }

        private static boolean recoveryAwaitsSupersedingSyncPoints = true;
        public static synchronized void setRecoveryAwaitsSupersedingSyncPoints(boolean newRecoveryAwaitsSupersedingSyncPoints) { pre(); recoveryAwaitsSupersedingSyncPoints = newRecoveryAwaitsSupersedingSyncPoints; }

        private static boolean syncPointsTrackUnstableMediumPathDependencies = false;
        public static synchronized void setSyncPointsTrackUnstableMediumPathDependencies(boolean newSyncPointsTrackUnstableMediumPathDependencies) { pre(); syncPointsTrackUnstableMediumPathDependencies = newSyncPointsTrackUnstableMediumPathDependencies; }

        private static boolean recoverPartialAcceptPhaseIfNoFastPath = false;
        public static synchronized void setRecoverPartialAcceptPhaseIfNoFastPath(boolean newSyncPointsRecoverPartialAcceptPhase) { pre(); recoverPartialAcceptPhaseIfNoFastPath = newSyncPointsRecoverPartialAcceptPhase; }

        private static boolean recoverReads = false;
        public static synchronized void setRecoverReads(boolean newRecoverReads) { pre(); recoverReads = newRecoverReads; }

        private static boolean filterDuplicateDependenciesFromAcceptReply = true;
        public static synchronized void setFilterDuplicateDependenciesFromAcceptReply(boolean newFilterDuplicateDependenciesFromAcceptReply) { pre(); filterDuplicateDependenciesFromAcceptReply = newFilterDuplicateDependenciesFromAcceptReply; }

        private static SendStableMessages sendStableMessages = FOR_READS_OR_NONE_IF_FASTEXEC;
        public static synchronized void setSendStableMessages(SendStableMessages newSendStableMessages) { pre(); sendStableMessages = newSendStableMessages; }

        private static SendMinimal sendMinimal = SendMinimal.MINIMAL;
        public static synchronized void setSendMinimal(SendMinimal newSendMinimal) { pre(); sendMinimal = newSendMinimal; }

        private static boolean permitCoordinatorLocalExecution = true;
        public static synchronized void setPermitCoordinatorLocalExecution(boolean newPermitCoordinatorLocalExecution) { pre(); permitCoordinatorLocalExecution = newPermitCoordinatorLocalExecution; }

        private static boolean permitLocalDelivery = true;
        public static synchronized void setPermitLocalDelivery(boolean newPermitLocalDelivery) { pre(); permitLocalDelivery = newPermitLocalDelivery; }

        private static boolean permitAsyncTasks = true;
        public static synchronized void setPermitAsyncTasks(boolean newPermitAsyncTasks) { pre(); permitAsyncTasks = newPermitAsyncTasks; }

        private static boolean permitAtomicIncrementalTasks = true;
        public static synchronized void setPermitAtomicIncrementalTasks(boolean newPermitAtomicIncrementalTasks) { pre(); permitAtomicIncrementalTasks = newPermitAtomicIncrementalTasks; }

        private static boolean dataStoreRequiresUniqueHlcs = true;
        public static synchronized void setDataStoreRequiresUniqueHlcs(boolean newDataStoreRequiresUniqueHlcs) { pre();dataStoreRequiresUniqueHlcs = newDataStoreRequiresUniqueHlcs; }

        private static int markStaleIfCannotExecute = TinyEnumSet.encode(Txn.Kind.Write);
        public static synchronized void setMarkStaleIfCannotExecute(Txn.Kind ... kinds)
        {
            pre();
            int newMarkStaleIfCannotExecute = TinyEnumSet.encode(kinds);
            Invariants.require(TinyEnumSet.contains(newMarkStaleIfCannotExecute, Txn.Kind.Write));
            markStaleIfCannotExecute = newMarkStaleIfCannotExecute;
        }

        private static int transitiveDependenciesAreVisible = TinyEnumSet.encode(VisibilitySyncPoint);
        public static synchronized void setTransitiveDependenciesAreVisible(Txn.Kind ... kinds)
        {
            pre();
            int newTransitiveDependenciesAreVisible = TinyEnumSet.encode(kinds);
            Invariants.require(TinyEnumSet.contains(newTransitiveDependenciesAreVisible, Txn.Kind.VisibilitySyncPoint));
            transitiveDependenciesAreVisible = newTransitiveDependenciesAreVisible;
        }

        private static CleanCfkBefore cleanCfkBefore = SHARD_APPLIED;
        public static synchronized void setCleanCfkBefore(CleanCfkBefore newCleanCfkBefore) { pre(); cleanCfkBefore = newCleanCfkBefore; }

        private static DependencyElision dependencyElision = IF_DURABLY_COMMITTED;
        public static synchronized void setDependencyElision(DependencyElision newDependencyElision) { pre(); dependencyElision = newDependencyElision; }

        private static boolean visitMaybePruned = true;
        public static synchronized void setVisitMaybePruned(boolean newVisitMaybePruned) { pre(); visitMaybePruned = newVisitMaybePruned; }

        private static InformOfDurability informOfDurability = InformOfDurability.ALL;
        private static int informOfSingleKeyDurabilityIfDepsSizeAtLeast = 16;
        public static synchronized void setInformOfDurability(InformOfDurability newInformOfDurability) { pre(); informOfDurability = newInformOfDurability; }
        public static synchronized void setInformOfSingleKeyDurabilityIfDepsSizeAtLeast(int newInformOfSingleKeyDurabilityIfDepsSizeAtLeast) { pre();informOfSingleKeyDurabilityIfDepsSizeAtLeast = newInformOfSingleKeyDurabilityIfDepsSizeAtLeast; }

        private static FastExecution fastReadExecution = MAY_BYPASS_COMMANDSFORKEY;
        public static synchronized void setFastReadExecution(FastExecution newFastReads) { pre();fastReadExecution = newFastReads; }

        private static boolean fastReadExecMayResendTxn = true;
        public static synchronized void setFastReadExecMayResendTxn(boolean newFastReadExecMayResendTxn) { pre(); fastReadExecMayResendTxn = newFastReadExecMayResendTxn; }

        private static FastExecution fastWriteExecution = MAY_BYPASS_COMMANDSFORKEY;
        public static synchronized void setFastWriteExecution(FastExecution newFastWrites) { pre();fastWriteExecution = newFastWrites; }

        private static boolean dataStoreDetectsFutureReads = false;
        public static synchronized void setDataStoreDetectsFutureReads(boolean newDataStoreDetectsFutureReads) { pre(); dataStoreDetectsFutureReads = newDataStoreDetectsFutureReads; }

        private static CoordinatorBacklogExecution coordinatorBacklogExecution = CoordinatorBacklogExecution.ON_RECOVERY;
        public static synchronized void setCoordinatorBacklogExecution(CoordinatorBacklogExecution newCoordinatorExecuteBacklog) { pre(); coordinatorBacklogExecution = newCoordinatorExecuteBacklog; }

        private static boolean coordinatorBacklogExecutionMultiKey = false;
        public static synchronized void setCoordinatorBacklogExecutionMultiKey(boolean newCoordinatorBacklogExecutionMultiKey) { pre(); coordinatorBacklogExecutionMultiKey = newCoordinatorBacklogExecutionMultiKey; }

        private static boolean coordinatorBacklogExecutionClaims = false;
        public static synchronized void setCoordinatorBacklogExecutionClaims(boolean newCoordinatorBacklogExecutionClaims) { pre(); coordinatorBacklogExecutionClaims = newCoordinatorBacklogExecutionClaims; }

        private static boolean coordinatorBacklogExecutionYieldToNearby = true;
        public static synchronized void setCoordinatorBacklogExecutionYieldToNearby(boolean newCoordinatorBacklogExecutionYieldToNearby) { pre(); coordinatorBacklogExecutionYieldToNearby = newCoordinatorBacklogExecutionYieldToNearby; }

        private static boolean coordinatorBacklogExecutionPerDc = false;
        public static synchronized void setCoordinatorBacklogExecutionPerDc(boolean newCoordinatorBacklogExecutionPerDc) { pre(); coordinatorBacklogExecutionPerDc = newCoordinatorBacklogExecutionPerDc; }

        private static long coordinatorBacklogExecutionGraceMicros = 0;
        public static synchronized void setCoordinatorBacklogExecutionGrace(long grace, TimeUnit units) { pre(); coordinatorBacklogExecutionGraceMicros = units.toMicros(grace); }

        private static boolean coordinatorBacklogExecutionWait = false;
        public static synchronized void setCoordinatorBacklogExecutionWait(boolean newCoordinatorBacklogExecutionWait) { pre(); coordinatorBacklogExecutionWait = newCoordinatorBacklogExecutionWait; }

        private static long coordinatorBacklogExecutionLingerMicros = 0;
        public static synchronized void setCoordinatorBacklogExecutionLinger(long linger, TimeUnit units) { pre(); coordinatorBacklogExecutionLingerMicros = units.toMicros(linger); }

        private static boolean coordinatorBacklogExecutionPersistLocally = false;
        public static synchronized void setCoordinatorBacklogExecutionPersistLocally(boolean newCoordinatorBacklogExecutionPersistLocally) { pre(); coordinatorBacklogExecutionPersistLocally = newCoordinatorBacklogExecutionPersistLocally; }

        private static long coordinatorBacklogExecutionRemoteApplyDelayMicros = 0;
        public static synchronized void setCoordinatorBacklogExecutionRemoteApplyDelay(long delay, TimeUnit units) { pre(); coordinatorBacklogExecutionRemoteApplyDelayMicros = units.toMicros(delay); }

        private static ReplicaExecution replicaExecution = ReplicaExecution.NONE;
        public static synchronized void setReplicaExecution(ReplicaExecution newReplicaExecution) { pre(); replicaExecution = newReplicaExecution; }

        private static float replicaExecuteDistributedPersistChance = 1.0f;
        public static synchronized void setReplicaExecuteDistributedPersistChance(float newChance) { pre(); replicaExecuteDistributedPersistChance = newChance; }

        private static Predicate<? super Txn> permitReplicaExecution = ignore -> true;
        public static synchronized void setPermitReplicaExecution(Predicate<? super Txn> newPredicate) { pre(); permitReplicaExecution = newPredicate; }

        public static void validate()
        {
            Invariants.require(dataStoreDetectsFutureReads || (fastWriteExecution != MAY_BYPASS_SAFESTORE && fastReadExecution != MAY_BYPASS_SAFESTORE), "MAY_BYPASS_SAFESTORE is only permitted when dataStoreDetectsFutureReads");
            Invariants.require(permitCoordinatorLocalExecution || (!permittedFastPaths.contains(PrivilegedCoordinatorWithDeps) && !permittedFastPaths.contains(PrivilegedCoordinatorWithoutDeps)), "Privileged coordinator optimisations require coordinator local execution");
            Invariants.require(replicaExecution != ReplicaExecution.ALL || dataStoreDetectsFutureReads, "ReplicaExecution.ALL requires that the data store can detect future reads, as it does not validate the command state after a read, and so the read may race with its own write");
        }
    }

    static
    {
        Configure.validate();
        Configure.setConfigured();
    }

    public static final boolean rangeEndInclusive = Configure.rangeEndInclusive;
    public static boolean isRangeEndInclusive() { return rangeEndInclusive; }
    public static boolean isRangeEndExclusive() { return !rangeEndInclusive; }
    public static boolean isRangeStartInclusive() { return !rangeEndInclusive; }
    public static boolean isRangeStartExclusive() { return rangeEndInclusive; }

    public static final RateLimitedLogStatement.Factory rateLimitedLogStatementFactory = Configure.rateLimitedLogStatementFactory;

    private static final FastPaths permittedFastPaths = Configure.permittedFastPaths;
    public static boolean usePrivilegedCoordinator() { return permittedFastPaths.hasPrivilegedCoordinator(); }
    public static FastPath ensurePermitted(FastPath path) { return path.toPermitted(permittedFastPaths); }

    private static final MediumPath defaultMediumPath = Configure.defaultMediumPath;
    public static MediumPath defaultMediumPath() { return defaultMediumPath; }

    private static final boolean permitFastQuorumMediumPath = Configure.permitFastQuorumMediumPath;
    public static boolean permitFastQuorumMediumPath() { return permitFastQuorumMediumPath; }

    private static final AbandonFastPath abandonFastPath = Configure.abandonFastPath;
    public static AbandonFastPath abandonFastPath() { return abandonFastPath; }

    private static final UniqueTimestampOnConflict UNIQUE_TIMESTAMP_ON_CONFLICT = Configure.uniqueTimestampOnConflict;
    public static UniqueTimestampOnConflict uniqueTimestampOnConflict() { return UNIQUE_TIMESTAMP_ON_CONFLICT; }

    private static final boolean recoveryAwaitsSupersedingSyncPoints = Configure.recoveryAwaitsSupersedingSyncPoints;
    public static boolean recoveryAwaitsSupersedingSyncPoints() { return recoveryAwaitsSupersedingSyncPoints; }

    private static final boolean syncPointsTrackUnstableMediumPathDependencies = Configure.syncPointsTrackUnstableMediumPathDependencies;
    public static boolean syncPointsTrackUnstableMediumPathDependencies() { return syncPointsTrackUnstableMediumPathDependencies; }

    private static final boolean recoverPartialAcceptPhaseIfNoFastPath = Configure.recoverPartialAcceptPhaseIfNoFastPath;
    public static boolean recoverPartialAcceptPhaseIfNoFastPath() { return recoverPartialAcceptPhaseIfNoFastPath; }

    // TODO (expected): make final, but requires some solution to burn test mutating these randomly
    private static boolean recoverReads = Configure.recoverReads;
    public static boolean recoverReads() { return recoverReads; }

    private static final boolean filterDuplicateDependenciesFromAcceptReply = Configure.filterDuplicateDependenciesFromAcceptReply;
    public static boolean filterDuplicateDependenciesFromAcceptReply() { return filterDuplicateDependenciesFromAcceptReply; }

    private static SendStableMessages sendStableMessages = Configure.sendStableMessages;
    // with designated backlog execution, every replica must learn the transaction is Stable, as any may be designated to
    // execute it (or be read from by the designated replica); so we behave as TO_ALL
    public static boolean sendOnlyReadStableMessages(TxnId txnId) { return !coordinatorBacklogExecutionDesignated() && (sendStableMessages.compareTo(FOR_READS) >= 0 || (sendStableMessages == TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS && !mayExecuteAtReplica(txnId))); }
    public static boolean sendNoStableIfFastExec() { return sendStableMessages == FOR_READS_OR_NONE_IF_FASTEXEC && !coordinatorBacklogExecutionDesignated(); }

    private static final boolean sendMinimal = Configure.sendMinimal != SendMinimal.MAXIMAL;
    public static boolean sendMinimal() { return sendMinimal; }

    private static boolean sendMinimalNoDeps = Configure.sendMinimal == SendMinimal.MINIMAL_NO_DEPS;
    /**
     * Whether {@link accord.coordinate.Persist} may send {@link accord.messages.Apply.Kind#NoDeps} to replicas we
     * expect to have been sent a Stable message (see {@link accord.coordinate.ExecuteFlag.CoordinationFlags#isStableSent}).
     */
    public static boolean sendMinimalNoDeps() { return sendMinimalNoDeps; }

    private static final boolean dataStoreRequiresUniqueHlcs = Configure.dataStoreRequiresUniqueHlcs;
    public static boolean dataStoreRequiresUniqueHlcs() { return dataStoreRequiresUniqueHlcs; }

    private static int markStaleIfCannotExecute = Configure.markStaleIfCannotExecute;
    public static boolean markStaleIfCannotExecute(TxnId txnId) { return TinyEnumSet.contains(markStaleIfCannotExecute, txnId.kindOrdinal()); }
    public static boolean markStaleIfCannotExecute(Txn.Kind kind) { return TinyEnumSet.contains(markStaleIfCannotExecute, kind); }

    private static final int transitiveDependenciesAreVisible = Configure.transitiveDependenciesAreVisible;
    public static boolean isTransitiveDependencyVisible(TxnId txnId) { return TinyEnumSet.contains(transitiveDependenciesAreVisible, txnId.kindOrdinal()); }
    public static boolean isTransitiveDependencyVisible(Txn.Kind kind) { return TinyEnumSet.contains(transitiveDependenciesAreVisible, kind); }

    private static final CleanCfkBefore cleanCfkBefore = Configure.cleanCfkBefore;
    public static CleanCfkBefore cleanCfkBefore() { return cleanCfkBefore; }

    private static final DependencyElision dependencyElision = Configure.dependencyElision;
    public static DependencyElision dependencyElision() { return dependencyElision; }

    private static final boolean visitMaybePruned = Configure.visitMaybePruned;
    public static boolean shouldVisitMaybePruned() { return visitMaybePruned; }

    public static boolean isHomeDoneIfNotDurable(SaveStatus saveStatus, TxnId txnId, Timestamp executeAt, @Nullable Txn txn)
    {
        if (txnId.isSystemTxn() || saveStatus.compareTo(SaveStatus.Stable) < 0)
            return false;

        if (executeAt == null || (executeAt != txnId && executeAt.epoch() != txnId.epoch()))
            return false;

        if (saveStatus.compareTo(SaveStatus.PreApplied) >= 0 && informOfSingleKeyDurabilityIfDepsSizeAtLeast > 0 && txnId.is(SingleKey))
            return true;

        // TODO (expected): why do we check txn != null?
        return txn != null && executeAtReplica(txnId, txn);
    }

    private static final InformOfDurability informOfDurability = Configure.informOfDurability;
    // TODO (expected): improve dependency elision, so this (and replicaExecuteDistributedPersistChance) are no longer needed
    private static final int informOfSingleKeyDurabilityIfDepsSizeAtLeast = Configure.informOfSingleKeyDurabilityIfDepsSizeAtLeast;
    public static InformOfDurability informOfDurability(TxnId txnId, @Nullable Deps deps)
    {
        if (txnId.is(Any) || deps == null || informOfSingleKeyDurabilityIfDepsSizeAtLeast <= deps.txnIdCount())
            return informOfDurability;

        return InformOfDurability.NONE;
    }

    private static FastExecution fastReadExecution = Configure.fastReadExecution;
    public static boolean fastReadsMayBypassSafeStore(TxnId txnId) { return fastReadExecution == MAY_BYPASS_SAFESTORE && (dataStoreDetectsFutureReads() || txnId.is(EphemeralRead)) && txnId.is(Domain.Key); }
    public static boolean fastReadsMayBypassCommandsForKey(TxnId txnId) { return fastReadExecution != FastExecution.DISABLED && !txnId.is(Txn.Kind.Write) && txnId.is(Domain.Key); }

    private static final boolean fastReadExecutionMayResendTxn = Configure.fastReadExecMayResendTxn;
    public static boolean fastReadExecutionMayResendTxn() { return fastReadExecutionMayResendTxn; }

    private static FastExecution fastWriteExecution = Configure.fastWriteExecution;
    public static boolean fastWritesMayBypassSafeStore() { return fastWriteExecution == MAY_BYPASS_SAFESTORE; }
    public static boolean fastWritesMayBypassCommandsForKey() { return fastWriteExecution != FastExecution.DISABLED; }

    private static boolean dataStoreDetectsFutureReads = Configure.dataStoreDetectsFutureReads;
    public static boolean dataStoreDetectsFutureReads() { return dataStoreDetectsFutureReads; }

    private static CoordinatorBacklogExecution coordinatorBacklogExecution = Configure.coordinatorBacklogExecution;
    public static boolean coordinatorBacklogExecution(Ballot ballot)
    {
        switch (coordinatorBacklogExecution)
        {
            default: throw new UnhandledEnum(coordinatorBacklogExecution);
            case DISABLED:
            case DESIGNATED: return false;
            case ALWAYS: return true;
            case ON_RECOVERY: return !ballot.equals(Ballot.ZERO);
        }
    }

    /**
     * @return true iff a designated replica executes transactions on behalf of their (remote) coordinators
     * (see {@link CoordinatorBacklogExecution#DESIGNATED}); if so, no backlog sinks are installed
     */
    public static boolean coordinatorBacklogExecutionDesignated() { return coordinatorBacklogExecution == CoordinatorBacklogExecution.DESIGNATED; }

    private static boolean coordinatorBacklogExecutionPerDc = Configure.coordinatorBacklogExecutionPerDc;

    /**
     * With {@link CoordinatorBacklogExecution#DESIGNATED}, whether to designate a replica in every DC (so that every DC
     * executes contended transactions in parallel, minimising latency at the cost of duplicated work), rather than one
     * replica overall (to which coordinators in other DCs defer)
     */
    public static boolean coordinatorBacklogExecutionPerDc() { return coordinatorBacklogExecutionPerDc; }

    private static long coordinatorBacklogExecutionGraceMicros = Configure.coordinatorBacklogExecutionGraceMicros;

    /**
     * With {@link CoordinatorBacklogExecution#DESIGNATED}, how long a designated replica waits before executing a
     * transaction that was ready to execute as soon as it became Stable. Such a transaction is probably uncontended, so its
     * original coordinator will probably execute it promptly, and we expect its Apply to arrive about when its Stable did.
     */
    public static long coordinatorBacklogExecutionGrace(TimeUnit units) { return units.convert(coordinatorBacklogExecutionGraceMicros, TimeUnit.MICROSECONDS); }

    private static boolean coordinatorBacklogExecutionMultiKey = Configure.coordinatorBacklogExecutionMultiKey;

    /**
     * Whether backlog execution (see {@link #coordinatorBacklogExecution(Ballot)}) may extend to multi-key transactions.
     * If disabled, only {@link accord.primitives.TxnId.Cardinality#SingleKey} transactions are executed as part of a backlog.
     */
    public static boolean coordinatorBacklogExecutionMultiKey() { return coordinatorBacklogExecutionMultiKey; }

    private static boolean coordinatorBacklogExecutionClaims = Configure.coordinatorBacklogExecutionClaims;

    /**
     * Whether backlog executors send {@link accord.messages.BacklogClaim} to the original coordinator of a transaction
     * they intend to execute, so that it may pause its own attempt. Must not be enabled until every replica understands
     * {@link accord.messages.BacklogClaim}.
     */
    public static boolean coordinatorBacklogExecutionClaims() { return coordinatorBacklogExecutionClaims; }

    private static boolean coordinatorBacklogExecutionYieldToNearby = Configure.coordinatorBacklogExecutionYieldToNearby;

    /**
     * Whether backlog executors leave a transaction to its original coordinator if the latter is nearby
     * (see {@link accord.local.Node#isNearby}) and plausibly still coordinating it
     */
    public static boolean coordinatorBacklogExecutionYieldToNearby() { return coordinatorBacklogExecutionYieldToNearby; }

    private static boolean coordinatorBacklogExecutionWait = Configure.coordinatorBacklogExecutionWait;

    /**
     * Whether backlog executors wait for the replicas they read from to become ready to execute, rather than abandon
     * their attempt. A backlog executor is typically caught up with the backlog, so that a replica that is not ready is
     * typically about to be (e.g. it has yet to apply the predecessor we have just executed, or to receive a Stable
     * message); if we abandon, the transaction instead falls to its original coordinator, which may be in another DC.
     * We still abandon if a replica has not yet recorded the transaction as Stable, as we cannot then learn its
     * definition, and if the transaction is executed by somebody else first.
     * Must not be enabled until every replica supports {@link accord.messages.ReadTxnDataIfReady} without
     * {@link accord.coordinate.ExecuteFlag#NO_WAIT}.
     */
    public static boolean coordinatorBacklogExecutionWait() { return coordinatorBacklogExecutionWait; }

    private static long coordinatorBacklogExecutionLingerMicros = Configure.coordinatorBacklogExecutionLingerMicros;

    /**
     * How long a backlog executor continues to listen to a key once it has caught up with the backlog there, i.e.
     * since it last found a STABLE transaction queued behind the one it is executing. If zero, it stops listening as
     * soon as it catches up. A backlog executor is typically caught up under steady contention (as it executes
     * transactions faster than they arrive), so lingering avoids handing the chain to remote coordinators; but while
     * we linger we will also execute transactions that their original coordinator might execute as promptly.
     */
    public static long coordinatorBacklogExecutionLinger(TimeUnit units) { return units.convert(coordinatorBacklogExecutionLingerMicros, TimeUnit.MICROSECONDS); }

    private static boolean coordinatorBacklogExecutionPersistLocally = Configure.coordinatorBacklogExecutionPersistLocally;

    /**
     * With {@link CoordinatorBacklogExecution#DESIGNATED} and {@link #coordinatorBacklogExecutionPerDc()}, whether a
     * designated replica persists a transaction it executes only to the replicas in its own DC, reporting to the
     * original coordinator which replicas have applied it ({@link accord.messages.BacklogApplied}). The original
     * coordinator (which also executes the transaction, in its own DC) is then responsible for the transaction's
     * durability: it does not stop on {@link accord.messages.RemoteSuccess}, and counts reported replicas towards durability.
     * See also {@link #coordinatorBacklogExecutionRemoteApplyDelay}.
     * Must not be enabled until every replica understands {@link accord.messages.BacklogApplied}.
     */
    public static boolean coordinatorBacklogExecutionPersistLocally() { return coordinatorBacklogExecutionPersistLocally && coordinatorBacklogExecutionDesignated() && coordinatorBacklogExecutionPerDc(); }

    private static long coordinatorBacklogExecutionRemoteApplyDelayMicros = Configure.coordinatorBacklogExecutionRemoteApplyDelayMicros;

    /**
     * With {@link #coordinatorBacklogExecutionPersistLocally()}, how long an original coordinator waits for designated
     * replicas in other DCs to report they have applied a transaction before sending its own Apply to the replicas that
     * have not. If zero, the original coordinator sends Apply to every replica immediately (as it would otherwise).
     */
    public static long coordinatorBacklogExecutionRemoteApplyDelay(TimeUnit units) { return coordinatorBacklogExecutionPersistLocally() ? units.convert(coordinatorBacklogExecutionRemoteApplyDelayMicros, TimeUnit.MICROSECONDS) : 0; }

    private static final boolean permitCoordinatorLocalExecution = Configure.permitCoordinatorLocalExecution;
    public static boolean permitCoordinatorLocalExecution() { return permitCoordinatorLocalExecution; }
    
    private static final boolean permitLocalDelivery = Configure.permitLocalDelivery;
    public static boolean permitLocalDelivery() { return permitLocalDelivery; }

    private static final boolean permitAsyncTasks = Configure.permitAsyncTasks;
    private static final boolean permitAtomicIncrementalTasks = Configure.permitAtomicIncrementalTasks;
    public static boolean permitAtomicIncrementalTasks() { return permitAtomicIncrementalTasks; }
    public static LoadKeys loadKeysAsyncIfPermitted(TxnId txnId)
    {
        if (!permitAsyncTasks)
            return LoadKeys.SYNC;

        if (permitAtomicIncrementalTasks())
            return LoadKeys.ASYNC;
        return managesExecution(txnId) ? LoadKeys.SYNC : LoadKeys.ASYNC;
    }

    private static ReplicaExecution replicaExecution = Configure.replicaExecution;
    private static final Predicate<? super Txn> permitReplicaExecution = Configure.permitReplicaExecution;

    public static boolean mayExecuteAtReplica(TxnId txnId)
    {
        return replicaExecution != ReplicaExecution.NONE
               && txnId.is(Txn.Kind.Write)
               && txnId.is(SingleKey)
               && txnId.is(Domain.Key);
    }

    public static boolean executeAtReplica(TxnId txnId, @Nullable Txn txn)
    {
        if (!mayExecuteAtReplica(txnId))
            return false;

        switch (replicaExecution)
        {
            default:          throw new UnhandledEnum(replicaExecution);
            case NONE:        return false;
            case BLIND_WRITE: return txn != null && txn.read().keys().isEmpty() && permitReplicaExecution.test(txn);
            case ALL:         return txn != null && permitReplicaExecution.test(txn);
        }
    }

    private static final float replicaExecuteDistributedPersistChance = Configure.replicaExecuteDistributedPersistChance;
    public static boolean replicaExecuteDistributedPersist()
    {
        float chance = replicaExecuteDistributedPersistChance;
        if (chance <= 0f) return false;
        if (chance >= 1f) return true;
        return ThreadLocalRandom.current().nextFloat() < chance;
    }

    public static class QuorumEpochIntersections
    {
        public enum ChaseFixedPoint
        {
            Chase, DoNotChase;
        }

        public enum Include
        {
            // include all nodes and epochs with unsynced ranges
            Unsynced,
            // include only replicas that own the ranges, and only the ownership epochs
            Owned;

            private static Include max(Include a, Include b)
            {
                return a.compareTo(b) >= 0 ? a : b;
            }
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
                this.preacceptOrRecover = Include.max(preaccept.include, recover);
                this.preacceptOrCommit = Include.max(preaccept.include, commit);
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
                        case "recover": recover = include; Invariants.require(cfp == DoNotChase, "Invalid to specify ChaseFixedPoint.Chase for recover"); break;
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
            Spec spec = Spec.parse(System.getProperty("accord.quorums.epochs", "preaccept=-,accept=-,commit=-,stable=-,recover=-"));
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
            boolean txnInstability, txnDiscardPreAcceptDeps;
            boolean syncPointInstability, syncPointDiscardPreAcceptDeps;

            // the following reintroduce fixed bugs, to verify that a burn test detects them
            // a re-Accept at a later executeAt without a status change does not raise maxConflicts
            boolean reacceptKeepsMaxConflicts;
            // recovery infers a fast path reject from a later privileged coordinator that is not a fast path elector in our epoch
            boolean recoverRejectsForNonElector;
            // recovery only waits on a later privileged coordinator's vote if the fast quorum includes every replica, not every elector
            boolean recoverCoordVoteWaitUsesRf;
            // LatestDeps keeps the left entry on a PreAccept-phase tie, so may drop DepsFromCoordinator depending on reply order
            boolean latestDepsTieKeepsLeft;
            // Invalidate requires a promise from every shard even when a shard is final only by virtue of a truncated decision
            boolean invalidateRequiresPromises;
            // max conflicts are merged using epoch-major Timestamp.max, so the highest hlc may be dropped
            boolean maxConflictEpochMajorMerge;
            // CoordinateMaxConflict does not query the quorums of a newer epoch reported by a replica
            boolean maxConflictIgnoresNewerEpoch;
            // Persist treats an InsufficientEpochs Apply reply as final and resends untracked, so stops accounting for the replica
            boolean persistInsufficientEpochsIsFinal;
            // TopologyManager drains retired pending epochs without checking PendingEpochs is non-empty
            boolean retiredDrainIgnoresEmpty;
            // CommandsForKey.hasUniqueHlcAndIsReadyToExecute never examines the newest byId entry
            boolean readyToExecuteSkipsNewest;
            // CommandsForKey.visit(startedBefore) omits visible loadingPruned entries not followed by a byId entry
            boolean visitSkipsTrailingLoadingPruned;
            // partial journal compaction may TRUNCATE (dropping the outcome) a record that full compaction would retain
            boolean partialCleanupTruncates;
            // Propagate throws on InsufficientEpochs or Rejected commit outcomes, rather than retrying
            boolean propagateThrowsOnRetryableCommit;
            // Node.nextTxnId(Txn, FastPath, MediumPath) grants a privileged coordinator fast path across multiple keys or local shards
            boolean privilegedMultiShard;
            // LatestDeps keeps only the coordinatedDeps of the entry that wins a reduction, dropping those of a DepsFromCoordinator entry
            boolean latestDepsDropsCoordinatorDeps;
            // BeginRecovery does not wait on an earlier, proposed but not stable, dependency that may execute after us
            boolean recoverSkipsUnstableEarlierDep;

            /**
             * Enable the faults named (comma separated) in {@code accord.faults}
             */
            Spec()
            {
                for (String name : System.getProperty("accord.faults", "").split(","))
                {
                    name = name.trim();
                    if (name.isEmpty())
                        continue;

                    try { Spec.class.getDeclaredField(name).setBoolean(this, true); }
                    catch (NoSuchFieldException | IllegalAccessException e) { throw new IllegalArgumentException("Unknown fault: " + name, e); }
                }
            }
        }

        static
        {
            Spec spec = new Spec();
            txnInstability = spec.txnInstability;
            txnDiscardPreAcceptDeps = spec.txnDiscardPreAcceptDeps;
            syncPointInstability = spec.syncPointInstability;
            syncPointDiscardPreAcceptDeps = spec.syncPointDiscardPreAcceptDeps;
            reacceptKeepsMaxConflicts = spec.reacceptKeepsMaxConflicts;
            recoverRejectsForNonElector = spec.recoverRejectsForNonElector;
            recoverCoordVoteWaitUsesRf = spec.recoverCoordVoteWaitUsesRf;
            latestDepsTieKeepsLeft = spec.latestDepsTieKeepsLeft;
            invalidateRequiresPromises = spec.invalidateRequiresPromises;
            maxConflictEpochMajorMerge = spec.maxConflictEpochMajorMerge;
            maxConflictIgnoresNewerEpoch = spec.maxConflictIgnoresNewerEpoch;
            persistInsufficientEpochsIsFinal = spec.persistInsufficientEpochsIsFinal;
            retiredDrainIgnoresEmpty = spec.retiredDrainIgnoresEmpty;
            readyToExecuteSkipsNewest = spec.readyToExecuteSkipsNewest;
            visitSkipsTrailingLoadingPruned = spec.visitSkipsTrailingLoadingPruned;
            partialCleanupTruncates = spec.partialCleanupTruncates;
            propagateThrowsOnRetryableCommit = spec.propagateThrowsOnRetryableCommit;
            privilegedMultiShard = spec.privilegedMultiShard;
            latestDepsDropsCoordinatorDeps = spec.latestDepsDropsCoordinatorDeps;
            recoverSkipsUnstableEarlierDep = spec.recoverSkipsUnstableEarlierDep;
        }

        public static final boolean txnInstability, txnDiscardPreAcceptDeps;
        public static final boolean syncPointInstability, syncPointDiscardPreAcceptDeps;
        public static final boolean reacceptKeepsMaxConflicts, recoverRejectsForNonElector, recoverCoordVoteWaitUsesRf;
        public static final boolean latestDepsTieKeepsLeft, invalidateRequiresPromises;
        public static final boolean maxConflictEpochMajorMerge, maxConflictIgnoresNewerEpoch;
        public static final boolean persistInsufficientEpochsIsFinal, retiredDrainIgnoresEmpty;
        public static final boolean readyToExecuteSkipsNewest, visitSkipsTrailingLoadingPruned;
        public static final boolean partialCleanupTruncates, propagateThrowsOnRetryableCommit, privilegedMultiShard;
        public static final boolean latestDepsDropsCoordinatorDeps, recoverSkipsUnstableEarlierDep;

        public static boolean discardPreAcceptDeps(TxnId txnId)
        {
            return (txnDiscardPreAcceptDeps | syncPointDiscardPreAcceptDeps)
                   && (txnId.isSyncPoint() ? syncPointDiscardPreAcceptDeps : txnDiscardPreAcceptDeps);
        }
    }}
