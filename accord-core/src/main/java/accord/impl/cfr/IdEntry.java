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

package accord.impl.cfr;

import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

public abstract class IdEntry extends TxnId
{
    static final int SAVE_STATUS_SHIFT = Status.Durability.ENCODING_BITS;
    static final int EXECUTE_AT_BIT = 1 << (SaveStatus.ENCODING_BITS + SAVE_STATUS_SHIFT);

    int encoded;

    TxnId plainTxnId()
    {
        return new TxnId(this);
    }

    Status.Durability durability()
    {
        return Status.Durability.forEncoded(encoded & Status.Durability.ENCODING_MASK);
    }

    SaveStatus saveStatus()
    {
        return SaveStatus.forOrdinal(saveStatusOrdinal());
    }

    int saveStatusOrdinal()
    {
        return (encoded >>> SAVE_STATUS_SHIFT) & SaveStatus.ENCODING_MASK;
    }

    Timestamp maybeExecuteAt()
    {
        if ((encoded & EXECUTE_AT_BIT) != 0)
            return plainTxnId();
        return null;
    }

    public IdEntry(TxnId txnId)
    {
        super(txnId);
    }

    abstract IdEntry copy();

    abstract Ranges ranges();

    public boolean update(SaveStatus saveStatus, Status.Durability durability, Timestamp executeAt)
    {
        int newEncoded = (saveStatus.ordinal() << SAVE_STATUS_SHIFT) | durability.encoded() | (super.equals(executeAt) ? EXECUTE_AT_BIT : 0);
        if (encoded == newEncoded)
            return false;
        encoded = newEncoded;
        Invariants.require(durability() == durability);
        Invariants.require(saveStatus() == saveStatus);
        return true;
    }
}
