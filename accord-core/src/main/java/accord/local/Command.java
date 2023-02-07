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

import accord.api.Data;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.Utils;
import accord.utils.async.AsyncChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import javax.annotation.Nullable;
import java.util.*;

import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Known.DefinitionOnly;
import static accord.utils.Utils.*;
import static java.lang.String.format;

public abstract class Command extends ImmutableState
{
    // sentinel value to indicate a command requested in a preexecute context was not found
    // should not escape the safe command store
    public static final Command EMPTY = new Command()
    {
        @Override public Route<?> route() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public RoutingKey progressKey() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public RoutingKey homeKey() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public TxnId txnId() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Ballot promised() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Command withPromised(Ballot promised) { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Status.Durability durability() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public ImmutableSet<CommandListener> listeners() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public SaveStatus saveStatus() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp executeAt() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Ballot accepted() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public PartialTxn partialTxn() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Nullable
        @Override public PartialDeps partialDeps() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }

        @Override
        public String toString()
        {
            return "Command(EMPTY)";
        }
    };

    static
    {
        EMPTY.markInvalidated();
    }

    static PreLoadContext contextForCommand(Command command)
    {
        Invariants.checkState(command.hasBeen(Status.PreAccepted) && command.partialTxn() != null);
        return command instanceof PreLoadContext ? (PreLoadContext) command : PreLoadContext.contextFor(command.txnId(), command.partialTxn().keys());
    }

    private static Status.Durability durability(Status.Durability durability, SaveStatus status)
    {
        if (status.compareTo(SaveStatus.PreApplied) >= 0 && durability == NotDurable)
            return Local; // not necessary anywhere, but helps for logical consistency
        return durability;
    }
    
    public interface CommonAttributes
    {
        TxnId txnId();
        Status.Durability durability();
        RoutingKey homeKey();
        RoutingKey progressKey();
        Route<?> route();
        PartialTxn partialTxn();
        PartialDeps partialDeps();
        ImmutableSet<CommandListener> listeners();
    }

    public static class SerializerSupport
    {
        public static Command.Listener listener(TxnId txnId)
        {
            return new Command.Listener(txnId);
        }

        public static NotWitnessed notWitnessed(CommonAttributes attributes, Ballot promised)
        {
            return NotWitnessed.Factory.create(attributes, promised);
        }

        public static PreAccepted preaccepted(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return new PreAccepted(common, executeAt, promised);
        }

        public static Accepted accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return Accepted.Factory.create(common, status, executeAt, promised, accepted);
        }

        public static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            return Committed.Factory.create(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            return Executed.Factory.create(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
        }
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> expected, Class<?> actual)
    {
        if (actual != expected)
        {
            throw new IllegalStateException(format("Cannot instantiate %s for status %s. %s expected",
                                                   actual.getSimpleName(), status, expected.getSimpleName()));
        }
        return status;
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> klass)
    {
        switch (status)
        {
            case NotWitnessed:
                return validateCommandClass(status, NotWitnessed.class, klass);
            case PreAccepted:
                return validateCommandClass(status, PreAccepted.class, klass);
            case AcceptedInvalidate:
            case AcceptedInvalidateWithDefinition:
            case Accepted:
            case AcceptedWithDefinition:
                return validateCommandClass(status, Accepted.class, klass);
            case Committed:
            case ReadyToExecute:
                return validateCommandClass(status, Committed.class, klass);
            case PreApplied:
            case Applied:
            case Invalidated:
                return validateCommandClass(status, Executed.class, klass);
            default:
                throw new IllegalStateException("Unhandled status " + status);
        }
    }

    public static Command addListener(SafeCommandStore safeStore, Command command, CommandListener listener)
    {
        return safeStore.update(command, command.withListener(listener));
    }

    public static Command removeListener(SafeCommandStore safeStore, Command command, CommandListener listener)
    {
        if (!command.listeners().contains(listener))
            return command;

        return safeStore.update(command, command.withoutListener(listener));
    }

    public static Committed updateWaitingOn(SafeCommandStore safeStore, Committed command, WaitingOn.Update waitingOn)
    {
        if (!waitingOn.hasChanges())
            return command;

        return safeStore.update(command, command.withWaitingOn(waitingOn.build()));
    }

    public static class Listener implements CommandListener
    {
        protected final TxnId listenerId;

        private Listener(TxnId listenerId)
        {
            this.listenerId = listenerId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerId.equals(that.listenerId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerId);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerId + '}';
        }

        public TxnId txnId()
        {
            return listenerId;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, SaveStatus prev, Command updated)
        {
            Commands.listenerUpdate(safeStore, safeStore.command(listenerId), updated);
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(Utils.listOf(listenerId, caller), Keys.EMPTY);
        }
    }

    public static CommandListener listener(TxnId txnId)
    {
        return new Listener(txnId);
    }

    private abstract static class AbstractCommand extends Command
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final Status.Durability durability;
        private final RoutingKey homeKey;
        private final RoutingKey progressKey;
        private final Route<?> route;
        private final Ballot promised;
        private final ImmutableSet<CommandListener> listeners;

        private AbstractCommand(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Route<?> route, Ballot promised, ImmutableSet<CommandListener> listeners)
        {
            this.txnId = txnId;
            this.status = validateCommandClass(status, getClass());
            this.durability = durability;
            this.homeKey = homeKey;
            this.progressKey = progressKey;
            this.route = route;
            this.promised = promised;
            this.listeners = listeners;
        }

        private AbstractCommand(Command prev, SaveStatus status, Route<?> route, RoutingKey progressKey, Ballot promised)
        {
            this.txnId = prev.txnId();
            this.status = validateCommandClass(status, getClass());
            this.durability = prev.durability();
            this.homeKey = prev.homeKey();
            this.progressKey = progressKey;
            this.route = route;
            this.promised = Command.checkNewBallot(prev.promised(), promised, "promised");
            this.listeners = prev.listeners();
        }

        private AbstractCommand(Command prev, SaveStatus status)
        {
            this.txnId = prev.txnId();
            this.status = validateCommandClass(status, getClass());
            this.durability = prev.durability();
            this.homeKey = prev.homeKey();
            this.progressKey = prev.progressKey();
            this.route = prev.route();
            this.promised = prev.promised();
            this.listeners = prev.listeners();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Command command = (Command) o;
            return txnId.equals(command.txnId())
                    && status == command.saveStatus()
                    && durability == command.durability()
                    && Objects.equals(homeKey, command.homeKey())
                    && Objects.equals(progressKey, command.progressKey())
                    && Objects.equals(route, command.route())
                    && Objects.equals(promised, command.promised())
                    && listeners.equals(command.listeners());
        }

        @Override
        public String toString()
        {
            return "Command@" + System.identityHashCode(this) + '{' + txnId + ':' + status + '}';
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public final RoutingKey homeKey()
        {
            checkCanReadFrom();
            return homeKey;
        }

        @Override
        public final RoutingKey progressKey()
        {
            checkCanReadFrom();
            return progressKey;
        }

        @Override
        public final Route<?> route()
        {
            checkCanReadFrom();
            return route;
        }

        @Override
        public Ballot promised()
        {
            checkCanReadFrom();
            return promised;
        }

        @Override
        public Status.Durability durability()
        {
            checkCanReadFrom();
            return Command.durability(durability, saveStatus());
        }

        @Override
        public ImmutableSet<CommandListener> listeners()
        {
            checkCanReadFrom();
            if (listeners == null)
                return ImmutableSet.of();
            return listeners;
        }

        @Override
        public final SaveStatus saveStatus()
        {
            checkCanReadFrom();
            return status;
        }
    }

    /**
     * If this is the home shard, we require that this is a Route for all states &gt; NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     */
    public abstract Route<?> route();
    public abstract RoutingKey progressKey();

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     *
     * TODO (expected, efficiency): we probably do not want to save this on its own, as we probably want to
     *  minimize IO interactions and discrete registers, so will likely reference commit log entries directly
     *  At which point we may impose a requirement that only a Route can be saved, not a homeKey on its own.
     *  Once this restriction is imposed, we no longer need to pass around Routable.Domain with TxnId.
     */
    public abstract RoutingKey homeKey();
    public abstract Command withHomeAndProgressKey(RoutingKey homeKey, RoutingKey progressKey);

    public abstract TxnId txnId();
    public abstract Ballot promised();
    public abstract Command withPromised(Ballot promised);
    public abstract Status.Durability durability();
    public abstract Command withDurability(Status.Durability durability);
    public abstract ImmutableSet<CommandListener> listeners();
    public abstract SaveStatus saveStatus();

    private static boolean isSameClass(Command command, Class<? extends Command> klass)
    {
        return command.getClass() == klass;
    }

    private static Ballot checkNewBallot(Ballot current, Ballot next, String name)
    {
        if (next.compareTo(current) < 0)
            throw new IllegalArgumentException(String.format("Cannot update %s ballot from %s to %s. New ballot is less than current", name, current, next));
        return next;
    }

    private static void checkPromised(Command command, Ballot ballot)
    {
        checkNewBallot(command.promised(), ballot, "promised");
    }

    private static void checkAccepted(Command command, Ballot ballot)
    {
        checkNewBallot(command.accepted(), ballot, "accepted");
    }

    private static void checkSameClass(Command command, Class<? extends Command> klass, String errorMsg)
    {
        if (!isSameClass(command, klass))
            throw new IllegalArgumentException(errorMsg + format(" expected %s got %s", klass.getSimpleName(), command.getClass().getSimpleName()));
    }

    // TODO (low priority, progress): callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public final Route<?> someRoute()
    {
        checkCanReadFrom();
        if (route() != null)
            return route();

        if (homeKey() != null)
            return PartialRoute.empty(txnId().domain(), homeKey());

        return null;
    }

    public Unseekables<?, ?> maxUnseekables()
    {
        Route<?> route = someRoute();
        if (route == null)
            return null;

        return route.toMaximalUnseekables();
    }

    public PreLoadContext contextForSelf()
    {
        checkCanReadFrom();
        return contextForCommand(this);
    }

    public abstract Timestamp executeAt();
    public abstract Ballot accepted();
    public abstract PartialTxn partialTxn();
    public abstract @Nullable PartialDeps partialDeps();

    public final Status status()
    {
        checkCanReadFrom();
        return saveStatus().status;
    }

    public final Status.Known known()
    {
        checkCanReadFrom();
        return saveStatus().known;
    }

    public boolean hasBeenWitnessed()
    {
        checkCanReadFrom();
        return partialTxn() != null;
    }

    public final boolean hasBeen(Status status)
    {
        return status().compareTo(status) >= 0;
    }

    public boolean has(Status.Known known)
    {
        return known.isSatisfiedBy(saveStatus().known);
    }

    public boolean has(Status.Definition definition)
    {
        return known().definition.compareTo(definition) >= 0;
    }

    public boolean has(Status.Outcome outcome)
    {
        return known().outcome.compareTo(outcome) >= 0;
    }

    public boolean is(Status status)
    {
        return status() == status;
    }

    public final CommandListener asListener()
    {
        return listener(txnId());
    }

    public final boolean isWitnessed()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.PreAccepted);
        Invariants.checkState(result == (this instanceof PreAccepted));
        return result;
    }

    public final PreAccepted asWitnessed()
    {
        checkCanReadFrom();
        return (PreAccepted) this;
    }

    public final boolean isAccepted()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.AcceptedInvalidate);
        Invariants.checkState(result == (this instanceof Accepted));
        return result;
    }

    public final Accepted asAccepted()
    {
        checkCanReadFrom();
        return (Accepted) this;
    }

    public final boolean isCommitted()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.Committed);
        Invariants.checkState(result == (this instanceof Committed));
        return result;
    }

    public final Committed asCommitted()
    {
        checkCanReadFrom();
        return (Committed) this;
    }

    public final boolean isExecuted()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.PreApplied);
        Invariants.checkState(result == (this instanceof Executed));
        return result;
    }

    public final Executed asExecuted()
    {
        checkCanReadFrom();
        return (Executed) this;
    }

    static PreAccepted preaccept(Command prev, Ballot promised, Route<?> route, RoutingKey progressKey, Timestamp executeAt, PartialTxn partialTxn, @Nullable PartialDeps partialDeps)
    {
        return new PreAccepted(prev, SaveStatus.PreAccepted, route, progressKey, promised, partialTxn, executeAt, partialDeps);
    }

    static Accepted withDefinition(Accepted prev, Route<?> route, RoutingKey progressKey, PartialTxn partialTxn)
    {
        switch (prev.saveStatus())
        {
            default:
            case NotWitnessed:
                throw new AssertionError();

            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case PreCommittedWithAcceptedDeps:
                return new Accepted(prev, SaveStatus.enrich(prev.saveStatus(), DefinitionOnly), route, progressKey, prev.promised(), prev.accepted(), partialTxn, prev.executeAt(), prev.partialDeps());

            case PreAccepted:
            case ReadyToExecute:
            case Committed:
            case AcceptedWithDefinition:
            case PreCommittedWithDefinition:
            case AcceptedInvalidateWithDefinition:
            case PreCommittedWithDefinitionAndAcceptedDeps:
            case Applied:
            case Invalidated:
            case PreApplied:
                return prev;
        }
    }

    static Accepted accept(Command prev, Route<?> route, RoutingKey progressKey, Ballot promised, Ballot accepted, PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        return new Accepted(prev, SaveStatus.enrich(SaveStatus.Accepted, prev.saveStatus().known), route, progressKey, promised, accepted, partialTxn, executeAt, partialDeps);
    }

    static Accepted acceptInvalidation(Command prev, Ballot ballot)
    {
        Timestamp executeAt = prev.isWitnessed() ? prev.asWitnessed().executeAt() : null;
        return new Accepted(prev, SaveStatus.AcceptedInvalidate, ballot, ballot, executeAt);
    }

    static Accepted precommit(Command prev, Timestamp executeAt)
    {
        return new Accepted(prev, SaveStatus.enrich(SaveStatus.PreCommitted, prev.known()), executeAt);
    }

    static Committed commit(Command prev, Route<?> route, RoutingKey progressKey, PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps, WaitingOn waitingOn)
    {
        return new Committed(prev, SaveStatus.Committed, route, progressKey, partialTxn, executeAt, partialDeps, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
    }

    static Executed commitInvalidation(Command prev)
    {
        return new Executed(prev, SaveStatus.Invalidated, Timestamp.NONE, PartialDeps.NONE, WaitingOn.EMPTY, null, null);
    }

    static Executed preapply(Command prev, Route<?> route, RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps, WaitingOn waitingOn, Writes writes, Result result)
    {
        return new Executed(prev, SaveStatus.PreApplied, route, progressKey, executeAt, partialDeps, waitingOn, writes, result);
    }

    public static final class NotWitnessed extends AbstractCommand
    {
        NotWitnessed(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Route<?> route, Ballot promised, ImmutableSet<CommandListener> listeners)
        {
            super(txnId, status, durability, homeKey, progressKey, route, promised, listeners);
        }

        NotWitnessed(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            super(common, status, promised);
        }

        public static NotWitnessed create(TxnId txnId)
        {
            return new NotWitnessed(txnId, SaveStatus.NotWitnessed, NotDurable, null, null, null, Ballot.ZERO, null);
        }

        private static class Factory
        {
            public static NotWitnessed create(CommonAttributes common, Ballot promised)
            {
                return new NotWitnessed(common, SaveStatus.NotWitnessed, promised);
            }

            public static NotWitnessed update(NotWitnessed command, CommonAttributes common, Ballot promised)
            {
                checkSameClass(command, NotWitnessed.class, "Cannot update");
                command.checkCanReadFrom();
                Invariants.checkArgument(command.txnId().equals(common.txnId()));
                return new NotWitnessed(common, command.saveStatus(), promised);
            }
        }

        @Override
        public Timestamp executeAt()
        {
            checkCanReadFrom();
            return null;
        }

        @Override
        public Ballot promised()
        {
            checkCanReadFrom();
            return Ballot.ZERO;
        }

        @Override
        public Ballot accepted()
        {
            checkCanReadFrom();
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkCanReadFrom();
            return null;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            checkCanReadFrom();
            return null;
        }
    }

    public static class PreAccepted extends AbstractCommand
    {
        private final Timestamp executeAt;
        private final PartialTxn partialTxn;
        private final @Nullable PartialDeps partialDeps;

        private PreAccepted(PreAccepted prev, SaveStatus saveStatus)
        {
            super(prev, saveStatus);
            this.executeAt = prev.executeAt;
            this.partialTxn = prev.partialTxn;
            this.partialDeps = prev.partialDeps;
        }

        private PreAccepted(Command prev, SaveStatus saveStatus, Route<?> route, RoutingKey progressKey, Ballot promised, Timestamp executeAt, @Nullable PartialDeps partialDeps)
        {
            this(prev, saveStatus, route, progressKey, promised, prev.partialTxn(), executeAt, partialDeps);
        }

        private PreAccepted(Command prev, SaveStatus saveStatus, Route<?> route, RoutingKey progressKey, Ballot promised, PartialTxn partialTxn, Timestamp executeAt, @Nullable PartialDeps partialDeps)
        {
            super(prev, saveStatus, route, progressKey, promised);
            this.executeAt = executeAt;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            PreAccepted that = (PreAccepted) o;
            return executeAt.equals(that.executeAt)
                    && Objects.equals(partialTxn, that.partialTxn)
                    && Objects.equals(partialDeps, that.partialDeps);
        }

        @Override
        public Timestamp executeAt()
        {
            checkCanReadFrom();
            return executeAt;
        }

        @Override
        public Ballot accepted()
        {
            checkCanReadFrom();
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkCanReadFrom();
            return partialTxn;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            checkCanReadFrom();
            return partialDeps;
        }
    }

    public static class Accepted extends PreAccepted
    {
        private final Ballot accepted;

        Accepted(Command prev, SaveStatus status, Route<?> route, RoutingKey progressKey, Ballot promised, Ballot accepted, PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
        {
            super(prev, status, route, progressKey, promised, partialTxn, executeAt, partialDeps);
            this.accepted = Command.checkNewBallot(prev.accepted(), accepted, "accepted");;
        }

        private Accepted(Command prev, SaveStatus status, Timestamp executeAt)
        {
            this(prev, status, prev.promised(), prev.accepted(), executeAt);
        }

        private Accepted(Accepted prev, SaveStatus status)
        {
            super(prev, status);
            this.accepted = prev.accepted;
        }

        private Accepted(Command prev, SaveStatus status, Ballot promised, Ballot accepted, Timestamp executeAt)
        {
            super(prev, status, prev.route(), prev.progressKey(), promised, executeAt, prev.partialDeps());
            this.accepted = accepted;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Accepted that = (Accepted) o;
            return Objects.equals(accepted, that.accepted);
        }

        @Override
        public Ballot accepted()
        {
            checkCanReadFrom();
            return accepted;
        }
    }

    public static class Committed extends Accepted
    {
        private final ImmutableSortedSet<TxnId> waitingOnCommit;
        private final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        Committed(Command command, SaveStatus status, Route<?> route, RoutingKey progressKey, PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            super(command, status, route, progressKey, command.promised(), command.accepted(), partialTxn, executeAt, partialDeps);
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        Committed(Committed command, SaveStatus status)
        {
            super(command, status);
            this.waitingOnCommit = command.waitingOnCommit;
            this.waitingOnApply = command.waitingOnApply;
        }

        Committed(Command command, SaveStatus status, Route<?> route, RoutingKey progressKey, PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps, WaitingOn waitingOn)
        {
            this(command, status, route, progressKey, partialTxn, executeAt, partialDeps, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Committed committed = (Committed) o;
            return Objects.equals(waitingOnCommit, committed.waitingOnCommit)
                    && Objects.equals(waitingOnApply, committed.waitingOnApply);
        }

        public Committed readyToExecute()
        {
            return new Committed(this, SaveStatus.ReadyToExecute);
        }

        public AsyncChain<Data> read(SafeCommandStore safeStore)
        {
            checkCanReadFrom();
            return partialTxn().read(safeStore, this);
        }

        public WaitingOn waitingOn()
        {
            return new WaitingOn(waitingOnCommit, waitingOnApply);
        }

        public ImmutableSortedSet<TxnId> waitingOnCommit()
        {
            checkCanReadFrom();
            return waitingOnCommit;
        }

        public boolean isWaitingOnCommit()
        {
            checkCanReadFrom();
            return waitingOnCommit != null && !waitingOnCommit.isEmpty();
        }

        public TxnId firstWaitingOnCommit()
        {
            checkCanReadFrom();
            return isWaitingOnCommit() ? waitingOnCommit.first() : null;
        }

        public ImmutableSortedMap<Timestamp, TxnId> waitingOnApply()
        {
            checkCanReadFrom();
            return waitingOnApply;
        }

        public boolean isWaitingOnApply()
        {
            checkCanReadFrom();
            return waitingOnApply != null && !waitingOnApply.isEmpty();
        }

        public TxnId firstWaitingOnApply()
        {
            checkCanReadFrom();
            return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
        }

        public boolean hasBeenWitnessed()
        {
            checkCanReadFrom();
            return partialTxn() != null;
        }

        public boolean isWaitingOnDependency()
        {
            checkCanReadFrom();
            return isWaitingOnCommit() || isWaitingOnApply();
        }
    }

    public static class Executed extends Committed
    {
        private final Writes writes;
        private final Result result;

        private Executed(Executed command, SaveStatus status)
        {
            super(command, status);
            this.writes = command.writes();
            this.result = command.result();
        }

        Executed(Command command, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            super(command, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
            this.writes = writes;
            this.result = result;
        }

        Executed(Command command, SaveStatus status, Timestamp executeAt, PartialDeps partialDeps, WaitingOn waitingOn, @Nullable Writes writes, @Nullable Result result)
        {
            super(command, status, command.route(), command.progressKey(), command.partialTxn(), executeAt, partialDeps, waitingOn);
            this.writes = writes;
            this.result = result;
        }

        Executed(Command command, SaveStatus status, Timestamp executeAt, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(command, status, executeAt, waitingOn);
            this.writes = writes;
            this.result = result;
        }

        Executed(Command prev, SaveStatus status, Route<?> route, RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps, WaitingOn waitingOn, Writes writes, Result result)
        {
            this(prev, status, route, progressKey, prev.partialDeps(), executeAt, partialDeps, waitingOn.waitingOnCommit, waitingOn.waitingOnApply, writes, result);
        }

        public Executed applied()
        {
            return new Executed(asExecuted(), SaveStatus.Applied);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Executed executed = (Executed) o;
            return Objects.equals(writes, executed.writes)
                    && Objects.equals(result, executed.result);
        }

        public Writes writes()
        {
            checkCanReadFrom();
            return writes;
        }

        public Result result()
        {
            checkCanReadFrom();
            return result;
        }
    }

    public static class WaitingOn
    {
        public static final WaitingOn EMPTY = new WaitingOn(ImmutableSortedSet.of(), ImmutableSortedMap.of());
        public final ImmutableSortedSet<TxnId> waitingOnCommit;
        public final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        public WaitingOn(ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        public static class Update
        {
            private boolean hasChanges = false;
            private NavigableSet<TxnId> waitingOnCommit;
            private NavigableMap<Timestamp, TxnId> waitingOnApply;

            public Update()
            {

            }

            public Update(WaitingOn waitingOn)
            {
                this.waitingOnCommit = waitingOn.waitingOnCommit;
                this.waitingOnApply = waitingOn.waitingOnApply;
            }

            public Update(Committed committed)
            {
                this.waitingOnCommit = committed.waitingOnCommit();
                this.waitingOnApply = committed.waitingOnApply();
            }

            public boolean hasChanges()
            {
                return hasChanges;
            }

            public void addWaitingOnCommit(TxnId txnId)
            {
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.add(txnId);
                hasChanges = true;
            }

            public void removeWaitingOnCommit(TxnId txnId)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.remove(txnId);
                hasChanges = true;
            }

            public void addWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.put(executeAt, txnId);
                hasChanges = true;
            }

            public void removeWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.remove(executeAt);
                hasChanges = true;
            }

            public void removeWaitingOn(TxnId txnId, Timestamp executeAt)
            {
                removeWaitingOnCommit(txnId);
                removeWaitingOnApply(txnId, executeAt);
                hasChanges = true;
            }

            public WaitingOn build()
            {
                if ((waitingOnCommit == null || waitingOnCommit.isEmpty()) && (waitingOnApply == null || waitingOnApply.isEmpty()))
                    return EMPTY;
                return new WaitingOn(ensureSortedImmutable(waitingOnCommit), ensureSortedImmutable(waitingOnApply));
            }
        }
    }
}
