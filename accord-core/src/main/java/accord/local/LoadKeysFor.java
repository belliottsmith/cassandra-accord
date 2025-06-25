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

/**
 * For operations that need information associated with keys or ranges,
 * this indicates whether the data will be queried, updated, or both.
 */
public enum LoadKeysFor
{
    /**
     * WRITE covers only updating the relevant key state for the primaryTxnId,
     * that is for key transactions this means updating key summaries, and for
     * range transactions this means updating any range summaries.
     * Importantly, this does not mean range transactions must be able to
     * synchronously (or otherwise) write to all intersecting key summaries.
     */
    WRITE,

    /**
     * READ covers all intersecting summaries of relevant key or range transactions that might be
     * witnessed by the primaryTxnId, for any commands that should be witnessed by primaryTxnId.
     *
     * This means range transactions MUST be able to consult all intersecting key summaries.
     */
    READ_WRITE,

    /**
     * RECOVERY is READ_WRITE + summary information of keys/transactions that should have witnessed
     * the primaryTxnId.
     */
    RECOVERY
}
