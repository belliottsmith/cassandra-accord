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

package accord.primitives;

import accord.coordinate.Outcome;
import accord.local.Command;
import accord.primitives.Status.Durability.HasOutcomeOrInvalidated;

import javax.annotation.Nonnull;

import static accord.primitives.Status.Durability.HasOutcomeOrInvalidated.None;
import static accord.primitives.Status.Durability.HasOutcomeOrInvalidated.QuorumOrInvalidated;
import static accord.primitives.Status.Durability.HasOutcomeOrInvalidated.UniversalOrInvalidated;
import static accord.primitives.Status.Phase.Accept;
import static accord.primitives.Status.Phase.Commit;

/**
 * A representation of activity on a command, so that peers may monitor a command to ensure it is making progress
 */
public class ProgressToken implements Comparable<ProgressToken>, Outcome
{
    public static final ProgressToken NONE = new ProgressToken(None, Status.NotDefined, Ballot.ZERO, false);
    public static final ProgressToken INVALIDATED = new ProgressToken(UniversalOrInvalidated, Status.Invalidated, Ballot.ZERO, false);
    public static final ProgressToken APPLIED = new ProgressToken(None, Status.PreApplied, Ballot.ZERO, false);
    public static final ProgressToken TRUNCATED_DURABLE_OR_INVALIDATED = new ProgressToken(QuorumOrInvalidated, Status.Truncated, Ballot.ZERO, false);

    public final HasOutcomeOrInvalidated outcome;
    public final Status status;
    public final Ballot promised;
    public final boolean isAccepted; // is the *promised ballot* accepted

    public ProgressToken(HasOutcomeOrInvalidated outcome, Status status, Ballot promised, boolean isAccepted)
    {
        this.outcome = outcome;
        this.status = status;
        this.promised = promised;
        this.isAccepted = isAccepted;
    }

    public ProgressToken(HasOutcomeOrInvalidated outcome, Status status, Ballot promised, Ballot accepted)
    {
        this.outcome = outcome;
        this.status = status;
        this.promised = promised;
        this.isAccepted = isAccepted(status, promised, accepted);
    }

    @Override public int compareTo(@Nonnull ProgressToken that)
    {
        int c = this.outcome.compareTo(that.outcome);
        if (c == 0) c = this.status.phase.compareTo(that.status.phase);
        if (c == 0) c = this.promised.compareTo(that.promised);
        if (c == 0 && this.isAccepted != that.isAccepted) c = this.isAccepted ? 1 : -1;
        return c;
    }
    
    public int compareTo(@Nonnull Command that)
    {
        int c = this.outcome.compareTo(that.durability().allShardsOrInvalidated());
        if (c == 0) c = this.status.phase.compareTo(that.status().phase);
        if (c == 0) c = this.promised.compareTo(that.promised());
        if (c == 0 && this.isAccepted != isAccepted(that.status(), that.promised(), that.acceptedOrCommitted())) c = this.isAccepted ? 1 : -1;
        return c;
    }

    public ProgressToken merge(ProgressToken that)
    {
        HasOutcomeOrInvalidated durability = this.outcome.mergeMax(that.outcome);
        Status status = this.status.compareTo(that.status) >= 0 ? this.status : that.status;
        Ballot promised = this.promised.compareTo(that.promised) >= 0 ? this.promised : that.promised;
        boolean isAccepted = (this.isAccepted && this.promised.equals(promised)) || (that.isAccepted && that.promised.equals(promised));
        if (isSame(durability, status, promised, isAccepted))
            return this;
        if (that.isSame(durability, status, promised, isAccepted))
            return that;
        return new ProgressToken(durability, status, promised, isAccepted);
    }

    public ProgressToken merge(Command command)
    {
        HasOutcomeOrInvalidated durability = this.outcome.mergeMax(command.durability().allShardsOrInvalidated());
        Status status = command.status();
        if (this.status.compareTo(status) > 0)
            status = this.status;

        Ballot promised = command.promised();
        boolean isAccepted = isAccepted(command.status(), command.promised(), command.acceptedOrCommitted());
        if (this.promised.compareTo(promised) >= 0)
        {
            promised = this.promised;
            isAccepted = this.isAccepted || (isAccepted && this.promised.equals(promised));
        }

        if (isSame(durability, status, promised, isAccepted))
            return this;
        return new ProgressToken(durability, status, promised, isAccepted);
    }

    private boolean isSame(HasOutcomeOrInvalidated durability, Status status, Ballot promised, boolean isAccepted)
    {
        return durability == this.outcome && status == this.status && promised.equals(this.promised) && isAccepted == this.isAccepted;
    }

    @Override
    public ProgressToken asProgressToken()
    {
        return this;
    }

    private static boolean isAccepted(Status status, Ballot promised, Ballot acceptedOrCommitted)
    {
        return (status.phase == Accept || status.phase == Commit) && promised.equals(acceptedOrCommitted);
    }

    @Override
    public String toString()
    {
        return "ProgressToken{" +
               "outcome=" + outcome +
               ", status=" + status +
               ", promised=" + promised +
               ", isAccepted=" + isAccepted +
               '}';
    }
}
