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

package accord.impl.basic;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import accord.api.Result;
import accord.impl.MessageListener;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.Listeners;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.LocalRequest;
import accord.messages.Message;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.*;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;

import static accord.local.Status.Durability.Local;
import static accord.utils.Invariants.checkState;
import static accord.utils.Invariants.illegalState;


public class Journal implements LocalRequest.Handler, Runnable
{
    private final Queue<RequestContext> unframedRequests = new ArrayDeque<>();
    private final LongArrayList waitForEpochs = new LongArrayList();
    private final Long2ObjectHashMap<ArrayList<RequestContext>> delayedRequests = new Long2ObjectHashMap<>();
    private final MessageListener messageListener;
    private final Map<Key, List<Diff>> diffs = new HashMap<>();
    private Node node;

    public Journal(MessageListener messageListener)
    {
        this.messageListener = messageListener;
    }

    public void start(Node node)
    {
        this.node = node;
        node.scheduler().recurring(this, 1, TimeUnit.MILLISECONDS);
    }

    public void shutdown()
    {
        this.node = null;
    }

    @Override
    public <R> void handle(LocalRequest<R> message, BiConsumer<? super R, Throwable> callback, Node node)
    {
        messageListener.onMessage(NodeSink.Action.DELIVER, node.id(), node.id(), -1, message);
        if (message.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(message, message.waitForEpoch(), () -> node.scheduler().now(() -> message.process(node, callback))));
            return;
        }
        message.process(node, callback);
    }

    public void handle(Request request, Node.Id from, ReplyContext replyContext)
    {
        if (request.type() != null && request.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(request, request.waitForEpoch(), () -> node.receive(request, from, replyContext)));
            return;
        }
        node.receive(request, from, replyContext);
    }

    @Override
    public void run()
    {
        if (this.node == null)
            return;
        try
        {
            doRun();
        }
        catch (Throwable t)
        {
            node.agent().onUncaughtException(t);
        }
    }

    private void doRun()
    {
        ArrayList<RequestContext> requests = null;
        // check to see if any pending epochs are in
        waitForEpochs.sort(null);
        for (int i = 0; i < waitForEpochs.size(); i++)
        {
            long waitForEpoch = waitForEpochs.getLong(i);
            if (!node.topology().hasEpoch(waitForEpoch))
                break;
            List<RequestContext> delayed = delayedRequests.remove(waitForEpoch);
            if (null == requests) requests = new ArrayList<>(delayed.size());
            requests.addAll(delayed);
        }
        waitForEpochs.removeIfLong(epoch -> !delayedRequests.containsKey(epoch));

        // for anything queued, put into the pending epochs or schedule
        RequestContext request;
        while (null != (request = unframedRequests.poll()))
        {
            long waitForEpoch = request.waitForEpoch;
            if (waitForEpoch != 0 && !node.topology().hasEpoch(waitForEpoch))
            {
                delayedRequests.computeIfAbsent(waitForEpoch, ignore -> new ArrayList<>()).add(request);
                if (!waitForEpochs.containsLong(waitForEpoch))
                    waitForEpochs.addLong(waitForEpoch);
            }
            else
            {
                if (null == requests) requests = new ArrayList<>();
                requests.add(request);
            }
        }

        // schedule
        if (requests != null)
        {
            requests.forEach(Runnable::run);
        }
    }

    public Command reconstruct(int commandStoreId, TxnId txnId)
    {
        Key key = new Key(txnId, commandStoreId);
        List<Diff> diffs = this.diffs.get(key);
        if (diffs == null || diffs.isEmpty())
            return null;

        Timestamp executeAt = null;
        Timestamp executesAtLeast = null;
        SaveStatus saveStatus = null;
        Status.Durability durability = null;

        Ballot acceptedOrCommitted = Ballot.ZERO;
        Ballot promised = null;

        Route<?> route = null;
        PartialTxn partialTxn = null;
        PartialDeps partialDeps = null;

        Command.WaitingOn waitingOn = null;

        Writes writes = null;
        Seekables<?, ?> additionalKeysOrRanges = null;
        Listeners.Immutable<Command.DurableAndIdempotentListener> listeners = null;
        for (Diff diff : diffs)
        {
            if (diff.txnId != null)
                txnId = diff.txnId;

            if (diff.executeAt != null)
                executeAt = diff.executeAt;

            if (diff.executesAtLeast != null)
                executesAtLeast = diff.executesAtLeast;

            if (diff.saveStatus != null)
                saveStatus = diff.saveStatus;

            if (diff.durability != null)
                durability = diff.durability;

            if (diff.acceptedOrCommitted != null)
                acceptedOrCommitted = diff.acceptedOrCommitted;

            if (diff.promised != null)
                promised = diff.promised;

            if (diff.route != null)
                route = diff.route;

            if (diff.partialTxn != null)
                partialTxn = diff.partialTxn;

            if (diff.partialDeps != null)
                partialDeps = diff.partialDeps;

            if (diff.waitingOn != null)
                waitingOn = diff.waitingOn;

            if (diff.writes != null)
                writes = diff.writes;

            if (diff.additionalKeysOrRanges != null)
                additionalKeysOrRanges = diff.additionalKeysOrRanges;

            if (diff.listeners != null)
                listeners = diff.listeners;
        }

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        if (partialTxn != null)
            attrs.partialTxn(partialTxn);
        if (durability != null)
            attrs.durability(durability);
        if (route != null)
            attrs.route(route);
        if (partialDeps != null &&
            (saveStatus.known.deps != Status.KnownDeps.NoDeps &&
             saveStatus.known.deps != Status.KnownDeps.DepsErased &&
             saveStatus.known.deps != Status.KnownDeps.DepsUnknown))
            attrs.partialDeps(partialDeps);
        if (additionalKeysOrRanges != null)
            attrs.additionalKeysOrRanges(additionalKeysOrRanges);
        if (listeners != null)
            attrs.setListeners(listeners);
        Invariants.checkState(saveStatus != null,
                              "Save status is null after applying %s", diffs);
        switch (saveStatus.status)
        {
            case NotDefined:
                return saveStatus == SaveStatus.Uninitialised ? Command.NotDefined.uninitialised(attrs.txnId())
                                                              : Command.NotDefined.notDefined(attrs, promised);
            case PreAccepted:
                return Command.PreAccepted.preAccepted(attrs, executeAt, promised);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return Command.Accepted.accepted(attrs, saveStatus, executeAt, promised, acceptedOrCommitted);
            case Committed:
            case Stable:
                return Command.Committed.committed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn);
            case PreApplied:
            case Applied:
                Result result = CommandSerializers.APPLIED;
                return Command.Executed.executed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn, writes, result);
            case Invalidated:
            case Truncated:
                return truncated(attrs, saveStatus, executeAt, executesAtLeast, writes, CommandSerializers.APPLIED);
            default:
                throw new IllegalStateException("Do not know " + saveStatus.status + " " + saveStatus);
        }
    }

    private static Command.Truncated truncated(CommonAttributes.Mutable attrs, SaveStatus status, Timestamp executeAt, Timestamp executesAtLeast, Writes writes, Result result)
    {
        switch (status)
        {
            default:
                throw illegalState("Unhandled SaveStatus: " + status);
            case TruncatedApplyWithOutcome:
            case TruncatedApplyWithDeps:
            case TruncatedApply:
                if (!attrs.txnId().kind().awaitsOnlyDeps())
                    executesAtLeast = null;
                switch (status.known.outcome)
                {
                    case Erased:
                    case WasApply:
                        writes = null;
                        result = null; // TODO
                        break;
                }
                return Command.Truncated.truncatedApply(attrs, status, executeAt, writes, result, executesAtLeast);
            case ErasedOrInvalidated:
                return Command.Truncated.erasedOrInvalidated(attrs.txnId(), attrs.durability(), attrs.route());
            case Erased:
                return Command.Truncated.erased(attrs.txnId(), attrs.durability(), attrs.route());
            case Invalidated:
                return Command.Truncated.invalidated(attrs.txnId(), attrs.durableListeners());
        }
    }

    public void onExecute(int commandStoreId, Command before, Command after)
    {
        if (before == null && after == null)
            return;
        Diff diff = diff(before, after);
        if (diff != null)
        {
            Key key = new Key(after.txnId(), commandStoreId);
            diffs.computeIfAbsent(key, (k_) -> new ArrayList<>()).add(diff);
        }
    }

    private static class RequestContext implements Runnable
    {
        final long waitForEpoch;
        final Message message;
        final Runnable fn;

        protected RequestContext(Message request, long waitForEpoch, Runnable fn)
        {
            this.waitForEpoch = waitForEpoch;
            this.message = request;
            this.fn = fn;
        }

        @Override
        public void run()
        {
            fn.run();
        }
    }

    private static class Diff
    {
        public final TxnId txnId;

        public final Timestamp executeAt;
        public final Timestamp executesAtLeast;
        public final SaveStatus saveStatus;
        public final Status.Durability durability;

        public final Ballot acceptedOrCommitted;
        public final Ballot promised;

        public final Route<?> route;
        public final PartialTxn partialTxn;
        public final PartialDeps partialDeps;

        public final Writes writes;
        public final Command.WaitingOn waitingOn;
        public final Seekables<?, ?> additionalKeysOrRanges;
        public final Listeners.Immutable<Command.DurableAndIdempotentListener> listeners;

        public Diff(TxnId txnId,
                    Timestamp executeAt,
                    Timestamp executesAtLeast,
                    SaveStatus saveStatus,
                    Status.Durability durability,

                    Ballot acceptedOrCommitted,
                    Ballot promised,

                    Route<?> route,
                    PartialTxn partialTxn,
                    PartialDeps partialDeps,
                    Command.WaitingOn waitingOn,

                    Writes writes,
                    Seekables<?, ?> additionalKeysOrRanges,
                    Listeners.Immutable<Command.DurableAndIdempotentListener> listeners)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.executesAtLeast = executesAtLeast;
            this.saveStatus = saveStatus;
            this.durability = durability;

            this.acceptedOrCommitted = acceptedOrCommitted;
            this.promised = promised;

            this.route = route;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;

            this.writes = writes;
            this.waitingOn = waitingOn;
            this.additionalKeysOrRanges = additionalKeysOrRanges;
            this.listeners = listeners;
        }

        @Override
        public String toString()
        {
            return "SavedDiff{" +
                   " txnId=" + txnId +
                   ", executeAt=" + executeAt +
                   ", saveStatus=" + saveStatus +
                   ", durability=" + durability +
                   ", acceptedOrCommitted=" + acceptedOrCommitted +
                   ", promised=" + promised +
                   ", route=" + route +
                   ", partialTxn=" + partialTxn +
                   ", partialDeps=" + partialDeps +
                   ", writes=" + writes +
                   ", waitingOn=" + waitingOn +
                   ", additionalKeysOrRanges=" + additionalKeysOrRanges +
                   '}';
        }
    }

    static Diff diff(Command before, Command after)
    {
        if (Objects.equals(before, after))
            return null;

        // TODO: we do not need to save `waitingOn` _every_ time.
        Command.WaitingOn waitingOn = getWaitingOn(after);
        return new Diff(after.txnId(),
                        ifNotEqual(before, after, Command::executeAt, true),
                        ifNotEqual(before, after, Command::executesAtLeast, true),
                        ifNotEqual(before, after, Command::saveStatus, false),

                        ifNotEqual(before, after, Command::durability, false),
                        ifNotEqual(before, after, Command::acceptedOrCommitted, false),
                        ifNotEqual(before, after, Command::promised, false),

                        ifNotEqual(before, after, Command::route, true),
                        ifNotEqual(before, after, Command::partialTxn, false),
                        ifNotEqual(before, after, Command::partialDeps, false),
                        waitingOn,
                        ifNotEqual(before, after, Command::writes, false),
                        // TODO (required): reflect in Cassandra
                        ifNotEqual(before, after, Command::additionalKeysOrRanges, false),
                        // TODO (required)^; reflect in Cassandra
                        ifNotEqual(before, after, Command::durableListeners, false));
    }

    static Command.WaitingOn getWaitingOn(Command command)
    {
        if (command instanceof Command.Committed)
            return command.asCommitted().waitingOn();

        return null;
    }

    private static <OBJ, VAL> VAL ifNotEqual(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r)
            return null;
        if (l == null || r == null)
            return r;
        assert allowClassMismatch || l.getClass() == r.getClass() : String.format("%s != %s", l.getClass(), r.getClass());

        if (l.equals(r))
            return null;

        return r;
    }

    public static class Key
    {
        final TxnId timestamp;
        final int commandStoreId;

        public Key(TxnId timestamp, int commandStoreId)
        {
            this.timestamp = timestamp;
            this.commandStoreId = commandStoreId;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return commandStoreId == key.commandStoreId && Objects.equals(timestamp, key.timestamp);
        }

        public int hashCode()
        {
            return Objects.hash(timestamp, commandStoreId);
        }
    }
}