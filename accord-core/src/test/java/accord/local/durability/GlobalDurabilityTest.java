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


package accord.local.durability;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import accord.local.Node;
import accord.messages.Request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

// this file was authored by LLM
class GlobalDurabilityTest
{
    @Test
    void failedSelectionDoesNotLeakInflight()
    {
        GlobalDurability durability = new GlobalDurability(mock(Node.class));
        AtomicInteger attempts = new AtomicInteger();
        durability.setMaxConcurrency(2);
        durability.setSelectSendTo(topology -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("injected launch failure");
        });

        durability.startGlobalSync();
        assertEquals(0, durability.inflight());
        durability.startGlobalSync();
        assertEquals(2, attempts.get(), "second cycle must not be suppressed");
        assertEquals(0, durability.inflight());
    }

    @Test
    void failedSendRefundsInflight()
    {
        Node node = mock(Node.class);
        doThrow(new IllegalStateException("injected send failure"))
            .when(node).send(any(Node.Id.class), any(Request.class), any(), any(), any());

        GlobalDurability durability = new GlobalDurability(node);
        durability.setMaxConcurrency(2);
        durability.setSelectSendTo(topology -> new Node.Id(1));
        durability.setSelectReadFrom(topology -> new Node.Id(2));

        // the send to node 1 fails before the read from node 2 is attempted; both must be released
        durability.startGlobalSync();
        assertEquals(0, durability.inflight());
        durability.startGlobalSync();
        assertEquals(0, durability.inflight());
        verify(node, times(2)).send(any(Node.Id.class), any(Request.class), any(), any(), any());
    }
}
