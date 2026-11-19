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

package accord.impl.progresslog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import accord.api.Agent;
import accord.api.ProgressLog.BlockedUntil;
import accord.local.CommandStore;
import accord.local.Node;
import accord.primitives.TxnId;

import static accord.impl.progresslog.DefaultProgressLog.ModeFlag.CATCH_UP;
import static accord.impl.progresslog.Progress.Queued;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Routable.Domain.Range;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * In CATCH_UP mode the progress log skips the delay for a first attempt, but must still back off from attempts that
 * made no progress: otherwise a request that cannot succeed (e.g. recovering a sync point whose only replica is this
 * unreadable node) is re-queued after 1us forever, and starves the executor.
 */
public class CatchUpBackoffTest
{
    private static final long NOW_MICROS = 1_000_000;
    private static final long BACKOFF_MICROS = 5_000_000;

    private Agent agent;
    private DefaultProgressLog log;
    private final TxnId txnId = new TxnId(1, 1, ExclusiveSyncPoint, Range, new Node.Id(1));

    @BeforeEach
    public void setUp()
    {
        agent = mock(Agent.class);
        when(agent.slowCoordinatorDelay(any(), any(), any(), any(), anyInt())).thenReturn(BACKOFF_MICROS);
        when(agent.slowReplicaDelay(any(), any(), any(), anyInt(), any(), any())).thenReturn(BACKOFF_MICROS);
        Node node = mock(Node.class);
        when(node.recentElapsed(MICROSECONDS)).thenReturn(NOW_MICROS);
        when(node.agent()).thenReturn(agent);
        CommandStore commandStore = mock(CommandStore.class);
        when(commandStore.agent()).thenReturn(agent);
        log = new DefaultProgressLog(node, commandStore);
        log.setModeExclusive(null, CATCH_UP);
    }

    @Test
    public void homeBacksOffAfterAnUnproductiveAttempt()
    {
        TxnState state = new TxnState(txnId);
        state.set(null, log, HomePhase.Undecided, Queued);
        assertEquals(NOW_MICROS + 1, state.scheduledTimerDeadline(), "a first attempt is not delayed while catching up");
        verify(agent, never()).slowCoordinatorDelay(any(), any(), any(), any(), anyInt());

        // as runHome does before each attempt; recoverCallback then re-queues on failure
        state.incrementHomeRunCounter();
        state.set(null, log, HomePhase.Undecided, Queued);
        assertEquals(NOW_MICROS + BACKOFF_MICROS, state.scheduledTimerDeadline(), "a retry must back off, even while catching up");
        verify(agent).slowCoordinatorDelay(any(), any(), eq(txnId), eq(MICROSECONDS), eq(2));
    }

    @Test
    public void waitingBacksOffAfterAnUnproductiveAttempt()
    {
        TxnState state = new TxnState(txnId);
        state.setBlockedUntil(null, log, BlockedUntil.HasDecidedExecuteAt);
        assertEquals(NOW_MICROS + 1, state.scheduledTimerDeadline(), "a first attempt is not delayed while catching up");
        verify(agent, never()).slowReplicaDelay(any(), any(), any(), anyInt(), any(), any());

        TxnState retried = new TxnState(txnId);
        // as retry and the unproductive await callback do before re-queueing
        retried.incrementWaitingRunCounter();
        retried.setBlockedUntil(null, log, BlockedUntil.HasDecidedExecuteAt);
        assertEquals(NOW_MICROS + BACKOFF_MICROS, retried.scheduledTimerDeadline(), "a retry must back off, even while catching up");
        verify(agent).slowReplicaDelay(any(), any(), eq(txnId), eq(2), any(), eq(MICROSECONDS));
    }
}
