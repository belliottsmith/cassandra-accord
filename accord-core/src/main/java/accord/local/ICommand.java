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

import accord.api.Result;
import accord.local.Command.WaitingOn;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;

import static accord.primitives.Status.Durability.NotDurable;

public interface ICommand
{
    TxnId txnId();
    Status.Durability durability();
    StoreParticipants participants();
    Ballot promised();
    PartialTxn partialTxn();
    PartialDeps partialDeps();
    Timestamp executeAt();
    Ballot acceptedOrCommitted();
    WaitingOn waitingOn();
    Writes writes();
    Result result();

    default Builder deconstruct()
    {
        return new Builder(this);
    }

    class Builder implements ICommand
    {
        private TxnId txnId;
        private Status.Durability durability;
        private StoreParticipants participants;
        private Ballot promised;
        private PartialTxn partialTxn;
        private PartialDeps partialDeps;
        private Timestamp executeAt;
        private Ballot acceptedOrCommitted;
        private WaitingOn waitingOn;
        private Writes writes;
        private Result result;

        public Builder(TxnId txnId)
        {
            this.txnId = txnId;
            this.participants = StoreParticipants.empty(txnId);
            this.durability = NotDurable;
        }

        public Builder(ICommand copy)
        {
            this.txnId = copy.txnId();
            this.durability = copy.durability();
            this.participants = Invariants.nonNull(copy.participants());
            this.promised = copy.promised();
            this.partialTxn = copy.partialTxn();
            this.partialDeps = copy.partialDeps();
            this.executeAt = copy.executeAt();
            this.acceptedOrCommitted = copy.acceptedOrCommitted();
            this.waitingOn = copy.waitingOn();
            this.writes = copy.writes();
            this.result = copy.result();
        }

        @Override
        public Builder deconstruct()
        {
            return this;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        public Builder txnId(TxnId txnId)
        {
            this.txnId = txnId;
            return this;
        }

        @Override
        public Status.Durability durability()
        {
            return durability;
        }

        public Builder durability(Status.Durability durability)
        {
            this.durability = durability;
            return this;
        }

        @Override
        public StoreParticipants participants()
        {
            return participants;
        }

        @Override
        public Ballot promised()
        {
            return promised;
        }

        public Builder promised(Ballot promised)
        {
            this.promised = promised;
            return this;
        }

        public Builder setParticipants(StoreParticipants participants)
        {
            this.participants = participants;
            return this;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        public Builder partialTxn(PartialTxn partialTxn)
        {
            this.partialTxn = partialTxn;
            return this;
        }

        @Override
        public PartialDeps partialDeps()
        {
            return partialDeps;
        }

        @Override
        public Timestamp executeAt()
        {
            return executeAt;
        }

        public Builder executeAt(Timestamp executeAt)
        {
            this.executeAt = executeAt;
            return this;
        }

        @Override
        public Ballot acceptedOrCommitted()
        {
            return acceptedOrCommitted;
        }

        @Override
        public WaitingOn waitingOn()
        {
            return waitingOn;
        }

        public Builder waitingOn(WaitingOn waitingOn)
        {
            this.waitingOn = waitingOn;
            return this;
        }

        @Override
        public Writes writes()
        {
            return writes;
        }

        public Builder writes(Writes writes)
        {
            this.writes = writes;
            return this;
        }

        @Override
        public Result result()
        {
            return result;
        }

        public Builder result(Result result)
        {
            this.result = result;
            return this;
        }

        public Builder acceptedOrCommitted(Ballot acceptedOrCommitted)
        {
            this.acceptedOrCommitted = acceptedOrCommitted;
            return this;
        }

        public Builder partialDeps(PartialDeps partialDeps)
        {
            this.partialDeps = partialDeps;
            return this;
        }
    }

    class EmptyCommand implements ICommand
    {
        @Override public TxnId txnId() { return null; }
        @Override public Status.Durability durability() { return null; }
        @Override public StoreParticipants participants() { return null; }
        @Override public Ballot promised() { return null; }
        @Override public PartialTxn partialTxn() { return null; }
        @Override public PartialDeps partialDeps() { return null; }
        @Override public Timestamp executeAt() { return null; }
        @Override public Ballot acceptedOrCommitted() { return null; }
        @Override public WaitingOn waitingOn() { return null; }
        @Override public Writes writes() { return null; }
        @Override public Result result() { return null; }
    }
}
