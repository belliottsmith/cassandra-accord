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

package accord.messages;

import java.util.concurrent.TimeUnit;

import accord.api.MessageSink;
import accord.local.Node;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public interface ReplyContext
{
    long expiresAt(TimeUnit units);
    void reply(Node.Id to, MessageSink sink, Reply success, Throwable failure);

    class NoReplyContext implements ReplyContext
    {
        final long expiresAtMicros;

        public NoReplyContext(Node node, Request request)
        {
            this.expiresAtMicros = node.agent().selfExpiresAt(request.primaryTxnId(), request.type(), MICROSECONDS);
        }

        @Override
        public long expiresAt(TimeUnit units)
        {
            return units.convert(expiresAtMicros, MICROSECONDS);
        }

        @Override
        public void reply(Node.Id to, MessageSink sink, Reply success, Throwable failure)
        {
        }
    }
}
