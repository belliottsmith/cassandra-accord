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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.primitives.*;
import accord.primitives.Txn.Kind;
import javax.annotation.Nonnull;
import org.apache.cassandra.utils.concurrent.Future;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;

/**
 * A CommandStore with exclusive access; a reference to this should not be retained outside of the scope of the method
 * that it is passed to. For the duration of the method invocation only, the methods on this interface are safe to invoke.
 *
 * Method implementations may therefore be single threaded, without volatile access or other concurrency control
 */
public interface SafeCommandStore
{
    Command ifPresent(TxnId txnId);

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    Command ifLoaded(TxnId txnId);
    Command command(TxnId txnId);

    /**
     * Register a listener against the given TxnId, then load the associated transaction and invoke the listener
     * with its current state.
     */
    void addAndInvokeListener(TxnId txnId, CommandListener listener);

    interface CommandFunction<I, O>
    {
        // TODO should this be Status instead?
        O apply(Seekable keyOrRange, TxnId txnId, Timestamp executeAt, Status status, I in);
    }

    enum TestTimestamp
    {
        STARTED_BEFORE,
        STARTED_AFTER,
        MAY_EXECUTE_BEFORE, // started before and uncommitted, or committed and executes before
        EXECUTES_AFTER
    }
    enum TestDep { WITH, WITHOUT, ANY_DEPS }
    enum TestKind implements Predicate<Kind>
    {
        Ws, RorWs, WsOrSyncPoint, SyncPoints, Any;

        public boolean test(Kind kind)
        {
            switch (this)
            {
                default: throw new AssertionError();
                case Any: return true;
                case WsOrSyncPoint: return kind == Write || kind == Kind.SyncPoint || kind == ExclusiveSyncPoint;
                case SyncPoints: return kind == Kind.SyncPoint || kind == ExclusiveSyncPoint;
                case Ws: return kind == Write;
                case RorWs: return kind == Read || kind == Write;
            }
        }

        public static TestKind conflicts(Kind kind)
        {
            switch (kind)
            {
                default: throw new AssertionError();
                case Read:
                    return Ws;
                case Write:
                    return RorWs;
                case SyncPoint:
                case ExclusiveSyncPoint:
                    return Any;
            }
        }

        public static TestKind shouldHaveWitnessed(Kind kind)
        {
            switch (kind)
            {
                default: throw new AssertionError();
                case Read:
                    return WsOrSyncPoint;
                case Write:
                    return Any;
                case SyncPoint:
                case ExclusiveSyncPoint:
                    return SyncPoints;
            }
        }

    }

    /**
     * Visits keys first and then ranges, both in ascending order.
     * Within each key or range visits TxnId in ascending order of queried timestamp.
     */
    // TODO these two signatures don't work well together
    <T> T mapReduceWithTerminate(Seekables<?, ?> keys, Ranges slice,
                       TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                       TestDep testDep, @Nullable TxnId depId,
                       @Nullable Status minStatus, @Nullable Status maxStatus,
                       CommandFunction<T, T> map, T accumulate, Predicate<T> terminate);
    <T> T mapReduce(Seekables<?, ?> keys, Ranges slice,
                       TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                       TestDep testDep, @Nullable TxnId depId,
                       @Nullable Status minStatus, @Nullable Status maxStatus,
                       CommandFunction<T, T> map, T initialValue, T terminalValue);

    void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command);
    void register(Seekable keyOrRange, Ranges slice, Command command);

    CommandStore commandStore();
    DataStore dataStore();
    Agent agent();
    ProgressLog progressLog();
    NodeTimeService time();
    CommandStores.RangesForEpoch ranges();
    Timestamp maxConflict(Seekables<?, ?> keys, Ranges slice);

    Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function);
}
