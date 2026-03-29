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

import java.util.Map;

import accord.utils.Invariants;

/**
 * Meant to assist implementations with mapping accord messages to their own messaging systems.
 */
public interface MessageType
{
    enum StandardMessage implements MessageType
    {
        SIMPLE_RSP, FAILURE_RSP,
        GET_EPHEMERAL_READ_DEPS_REQ, GET_EPHEMERAL_READ_DEPS_RSP,
        PRE_ACCEPT_REQ, PRE_ACCEPT_RSP,
        ACCEPT_REQ, ACCEPT_RSP, NOT_ACCEPT_REQ,
        COMMIT_REQ, COMMIT_INVALIDATE_REQ,
        APPLY_REQ, APPLY_RSP,
        BEGIN_RECOVER_REQ, BEGIN_RECOVER_RSP,
        BEGIN_INVALIDATE_REQ, BEGIN_INVALIDATE_RSP,
        READ_REQ, STABLE_THEN_READ_REQ, READ_EPHEMERAL_REQ, WAIT_UNTIL_APPLIED_REQ, APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, READ_RSP,
        AWAIT_REQ, AWAIT_RSP, ASYNC_AWAIT_COMPLETE_REQ,
        RECOVER_AWAIT_REQ, RECOVER_AWAIT_RSP,
        CHECK_STATUS_REQ, CHECK_STATUS_RSP,
        FETCH_DATA_REQ, FETCH_DATA_RSP,
        INFORM_DURABLE_REQ,
        GET_LATEST_DEPS_REQ, GET_LATEST_DEPS_RSP,
        GET_MAX_CONFLICT_REQ, GET_MAX_CONFLICT_RSP,
        GET_DURABLE_BEFORE_REQ, GET_DURABLE_BEFORE_RSP,
        SET_SHARD_DURABLE_REQ, SET_GLOBALLY_DURABLE_REQ,
        REMOTE_SUCCESS_REQ;

        private volatile Object mapToImplementation;
        public static void initialise(Map<StandardMessage, ?> mapToImplementation)
        {
            for (Map.Entry<StandardMessage, ?> e : mapToImplementation.entrySet())
            {
                Invariants.require(e.getKey().mapToImplementation == null);
                Invariants.require(e.getValue() != null);
                e.getKey().mapToImplementation = e.getValue();
            }
            for (StandardMessage messageType : values())
                Invariants.require(messageType.mapToImplementation != null, "Implementation has not specified a mapping for %s", messageType);
        }

        public final Object mapToImplementation()
        {
            return mapToImplementation;
        }
    }
}
