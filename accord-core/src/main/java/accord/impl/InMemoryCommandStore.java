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

package accord.impl;

import accord.api.*;
import accord.local.*;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.Committed;
import static accord.primitives.Routables.Slice.Minimal;

public abstract class InMemoryCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);

    public static <State extends ImmutableState, V> V withActiveState(State state, Callable<V> callable)
    {
        state.checkIsDormant();
        state.markActive();
        try
        {
            return callable.call();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            state.checkIsActive();
            state.markDormant();
        }
    }

    public static <State extends ImmutableState, V> V ensureActiveState(State state, Callable<V> callable)
    {
        boolean wasDormant = state.isDormant();
        try
        {
            if (wasDormant)
                state.markActive();
            else
                state.checkIsActive();
            return callable.call();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (wasDormant)
                state.markDormant();
        }
    }

    public static <State extends ImmutableState, V> void ensureActiveState(State state, Runnable runnable)
    {
        ensureActiveState(state, () -> {
            runnable.run();
            return null;
        });
    }

    public static <State extends ImmutableState> void withActiveState(State state, Runnable runnable)
    {
        withActiveState(state, () -> {
            runnable.run();
            return null;
        });
    }

    static class RangeCommand
    {
        final Command command;
        Ranges ranges;

        RangeCommand(Command command)
        {
            this.command = command;
        }

        void update(Ranges add)
        {
            if (ranges == null) ranges = add;
            else ranges = ranges.with(add);
        }
    }

    public static class InMemoryState
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog progressLog;
        private final RangesForEpochHolder rangesForEpochHolder;
        private RangesForEpoch rangesForEpoch;

        private final InMemoryCommandStore commandStore;
        private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
        private final NavigableMap<RoutableKey, CommandsForKey> commandsForKey = new TreeMap<>();
        private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();

        public InMemoryState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, InMemoryCommandStore commandStore)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLog = progressLog;
            this.rangesForEpochHolder = rangesForEpoch;
            this.commandStore = commandStore;
        }

        void refreshRanges()
        {
            rangesForEpoch = rangesForEpochHolder.get();
        }

        public Command command(TxnId txnId)
        {
            return commands.get(txnId);
        }

        public boolean hasCommand(TxnId txnId)
        {
            return commands.containsKey(txnId);
        }

        public CommandsForKey commandsForKey(Key key)
        {
            return commandsForKey.get(key);
        }

        public boolean hasCommandsForKey(Key key)
        {
            return commandsForKey.containsKey(key);
        }

        public void forEpochCommands(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
            Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
            for (Range range : ranges)
            {
                Iterable<CommandsForKey> rangeCommands = commandsForKey.subMap(
                        range.start(), range.startInclusive(),
                        range.end(), range.endInclusive()
                ).values();

                for (CommandsForKey commands : rangeCommands)
                {
                    commands.forWitnessed(minTimestamp, maxTimestamp, txnId -> consumer.accept(command(txnId)));
                }
            }
        }

        public void forCommittedInEpoch(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
            Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
            for (Range range : ranges)
            {
                Iterable<CommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                                                                                       range.startInclusive(),
                                                                                       range.end(),
                                                                                       range.endInclusive()).values();
                for (CommandsForKey commands : rangeCommands)
                {
                    commands.byExecuteAt()
                            .between(minTimestamp, maxTimestamp, status -> status.hasBeen(Committed))
                            .forEach(txnId -> consumer.accept(command(txnId)));
                }
            }
        }

        public void register(SafeCommandStore safeStore, Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
        {
            switch (keysOrRanges.domain())
            {
                default: throw new AssertionError();
                case Key:
                    forEach(keysOrRanges, slice, key -> CommandsForKeys.register(safeStore, command, key, slice));
                    break;
                case Range:
                    rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(command))
                            .update((Ranges)keysOrRanges);
            }
        }

        public void register(SafeCommandStore safeStore, Seekable keyOrRange, Ranges slice, Command command)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    forEach(keyOrRange, slice, key -> CommandsForKeys.register(safeStore, command, key, slice));
                    break;
                case Range:
                    rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(command))
                            .update(Ranges.of((Range)keyOrRange));
            }
        }

        private <O> O mapReduceForKey(SafeCommandStore safeStore, Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandsForKey, O, O> map, O accumulate, O terminalValue)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    for (Key key : keys)
                    {
                        if (!slice.contains(key)) continue;
                        CommandsForKey forKey = ((AbstractSafeCommandStore)safeStore).ifLoaded(key);
                        accumulate = map.apply(forKey, accumulate);
                        if (accumulate.equals(terminalValue))
                            return accumulate;
                    }
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    Ranges sliced = ranges.slice(slice, Minimal);
                    for (Range range : sliced)
                    {
                        for (CommandsForKey forKey : commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).values())
                        {
                            O initial = accumulate;
                            accumulate = withActiveState(forKey, () -> map.apply(forKey, initial));
                            if (accumulate.equals(terminalValue))
                                return accumulate;
                        }
                    }
            }
            return accumulate;
        }

        private void forEach(Seekables<?, ?> keysOrRanges, Ranges slice, Consumer<RoutableKey> forEach)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    keys.forEach(slice, key -> forEach.accept(key));
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    ranges.slice(slice).forEach(range -> {
                        commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                                .keySet().forEach(forEach);
                    });
            }
        }

        private void forEach(Routable keyOrRange, Ranges slice, Consumer<RoutableKey> forEach)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    Key key = (Key) keyOrRange;
                    if (slice.contains(key))
                        forEach.accept(key);
                    break;
                case Range:
                    Range range = (Range) keyOrRange;
                    Ranges.of(range).slice(slice).forEach(r -> {
                        commandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive())
                                .keySet().forEach(forEach);
                    });
            }
        }
    }

    private class CFKLoader implements CommandsForKey.CommandLoader<TxnId>
    {
        private Command loadForCFK(TxnId data)
        {
            InMemorySafeStore safeStore = current;
            Command result;
            // simplifies tests
            if (safeStore != null)
            {
                result = safeStore.ifPresent(data);
                if (result != null)
                    return result;
            }
            result = state.command(data);
            if (result != null)
                return result;
            throw new IllegalStateException("Could not find command for CFK for " + data);
        }

        @Override
        public TxnId txnId(TxnId txnId)
        {
            return loadForCFK(txnId).txnId();
        }

        @Override
        public Timestamp executeAt(TxnId txnId)
        {
            return loadForCFK(txnId).executeAt();
        }

        @Override
        public Txn.Kind txnKind(TxnId txnId)
        {
            return loadForCFK(txnId).partialTxn().kind();
        }

        @Override
        public SaveStatus saveStatus(TxnId txnId)
        {
            return loadForCFK(txnId).saveStatus();
        }

        @Override
        public PartialDeps partialDeps(TxnId txnId)
        {
            return loadForCFK(txnId).partialDeps();
        }

        @Override
        public TxnId saveForCFK(Command command)
        {
            return command.txnId();
        }
    }

    protected final CFKLoader cfkLoader = new CFKLoader();

    private static <K, V> Function<K, V> getOrCreate(Function<K, V> get, Function<K, V> init)
    {
        return key -> {
            V value = get.apply(key);
            if (value != null)
                return value;
            return init.apply(key);
        };
    }

    private static class InMemorySafeStore extends AbstractSafeCommandStore
    {
        private final InMemoryState state;
        private final CFKLoader cfkLoader;

        public InMemorySafeStore(InMemoryState state, CFKLoader cfkLoader, PreExecuteContext context)
        {
            super(context);
            this.cfkLoader = cfkLoader;
            this.state = state;
        }

        public AbstractSafeCommandStore.ContextState<RoutableKey, CommandsForKey, CommandsForKey.Update> commandsForKey()
        {
            return commandsForKey;
        }

        public CommandsForKey ifLoaded(RoutableKey key)
        {
            CommandsForKey cfk = getIfLoaded(key, commandsForKey, this::getIfLoaded, CommandsForKey.EMPTY);
            if (cfk == null)
                return null;
            cfk = maybeConvertEmpty(key, cfk, commandsForKey, k -> new CommandsForKey((Key) k, cfkLoader()), CommandsForKey.EMPTY);
            cfk.checkIsActive();
            return cfk;
        }

        public CommandsForKey commandsForKey(RoutableKey key)
        {
            CommandsForKey cfk = commandsForKey.get(key);
            if (cfk == null)
                throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", key));
            cfk = maybeConvertEmpty(key, cfk, commandsForKey, k -> new CommandsForKey((Key) k, cfkLoader()), CommandsForKey.EMPTY);
            cfk.checkIsActive();
            return cfk;
        }

        @Override
        protected Command getIfLoaded(TxnId txnId)
        {
            return state.command(txnId);
        }

        @Override
        protected CommandsForKey getIfLoaded(RoutableKey key)
        {
            return state.commandsForKey((Key) key);
        }

        @Override
        public InMemoryCommandStore commandStore()
        {
            return state.commandStore;
        }

        @Override
        public DataStore dataStore()
        {
            return state.store;
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        @Override
        public ProgressLog progressLog()
        {
            return state.progressLog;
        }

        @Override
        public RangesForEpoch ranges()
        {
            Invariants.checkState(state.rangesForEpoch != null);
            return state.rangesForEpoch;
        }

        @Override
        public long latestEpoch()
        {
            return state.time.epoch();

        }

        @Override
        public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            Timestamp timestamp = state.mapReduceForKey(this, keysOrRanges, slice, (forKey, prev) -> Timestamp.max(forKey.max(), prev), Timestamp.NONE, null);
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            for (RangeCommand command : state.rangeCommands.values())
            {
                if (command.ranges.intersects(sliced))
                    timestamp = Timestamp.max(timestamp, ensureActiveState(command.command, () -> command.command.executeAt()));
            }
            return timestamp;
        }

        @Override
        public NodeTimeService time()
        {
            return state.time;
        }

        @Override
        public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
        {
            accumulate = state.mapReduceForKey(this, keysOrRanges, slice, (forKey, prev) -> {
                CommandsForKey.CommandTimeseries<?> timeseries;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case STARTED_BEFORE:
                        timeseries = forKey.byId();
                        break;
                    case EXECUTES_AFTER:
                    case MAY_EXECUTE_BEFORE:
                        timeseries = forKey.byExecuteAt();
                }
                CommandsForKey.CommandTimeseries.TestTimestamp remapTestTimestamp;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case EXECUTES_AFTER:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.AFTER;
                        break;
                    case STARTED_BEFORE:
                    case MAY_EXECUTE_BEFORE:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.BEFORE;
                }
                return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
            }, accumulate, terminalValue);

            if (accumulate.equals(terminalValue))
                return accumulate;

            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            Map<Range, List<Command>> collect = new TreeMap<>(Range::compare);
            for (RangeCommand rangeCommand : state.rangeCommands.values())
            {
                Command command = rangeCommand.command;
                ensureActiveState(command, () -> {

                    switch (testTimestamp)
                    {
                        default: throw new AssertionError();
                        case STARTED_AFTER:
                            if (command.txnId().compareTo(timestamp) < 0) return;
                            else break;
                        case STARTED_BEFORE:
                            if (command.txnId().compareTo(timestamp) > 0) return;
                            else break;
                        case EXECUTES_AFTER:
                            if (command.executeAt().compareTo(timestamp) < 0) return;
                            else break;
                        case MAY_EXECUTE_BEFORE:
                            Timestamp compareTo = command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : command.txnId();
                            if (compareTo.compareTo(timestamp) > 0) return;
                            else break;
                    }

                    if (minStatus != null && command.status().compareTo(minStatus) < 0)
                        return;

                    if (maxStatus != null && command.status().compareTo(maxStatus) > 0)
                        return;

                    if (testKind == Ws && command.txnId().rw().isRead())
                        return;

                    if (testDep != ANY_DEPS)
                    {
                        if (!command.known().deps.hasProposedOrDecidedDeps())
                            return;

                        if ((testDep == WITH) == !command.partialDeps().contains(depId))
                            return;
                    }

                    if (!rangeCommand.ranges.intersects(sliced))
                        return;

                    Routables.foldl(rangeCommand.ranges, sliced, (r, in, i) -> {
                        // TODO (easy, efficiency): pass command as a parameter to Fold
                        List<Command> list = in.computeIfAbsent(r, ignore -> new ArrayList<>());
                        if (list.isEmpty() || list.get(list.size() - 1) != command)
                            list.add(command);
                        return in;
                    }, collect);
                });
            }

            for (Map.Entry<Range, List<Command>> e : collect.entrySet())
            {
                for (Command command : e.getValue())
                {
                    T initial = accumulate;
                    accumulate = ensureActiveState(command, () -> map.apply(e.getKey(), command.txnId(), command.executeAt(), initial));
                }
            }

            return accumulate;
        }

        @Override
        public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
        {
            state.register(this, keysOrRanges, slice, command);
        }

        @Override
        public void register(Seekable keyOrRange, Ranges slice, Command command)
        {
            state.register(this, keyOrRange, slice, command);
        }

        public void addListener(TxnId command, TxnId listener)
        {
            Command.addListener(this, command(command), Command.listener(listener));
        }

        @Override
        public CommandsForKey.CommandLoader<?> cfkLoader()
        {
            return cfkLoader;
        }
    }

    final InMemoryState state;
    private InMemorySafeStore current;

    public InMemoryCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
    {
        super(id);
        this.state = new InMemoryState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
    }

    public abstract boolean containsCommand(TxnId txnId);

    public abstract Command command(TxnId txnId);

    @Override
    public Agent agent()
    {
        return state.agent;
    }

    @Override
    public final AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return submit(context, i -> { consumer.accept(i); return null; });
    }

    protected InMemorySafeStore createCommandStore(InMemoryState state, PreExecuteContext context)
    {
        return new InMemorySafeStore(state, cfkLoader, context);
    }

    @Override
    public SafeCommandStore beginOperation(PreLoadContext loadContext)
    {
        PreExecuteContext executeContext = createPreExecuteCtx(loadContext);
        state.refreshRanges();
        if (current != null)
            throw new IllegalStateException("Another operation is in progress or it's store was not cleared");
        current = createCommandStore(state, executeContext);
        current.commands().checkActive();
        current.commandsForKey().checkActive();
        return current;
    }

    @Override
    public void completeOperation(SafeCommandStore store)
    {
        if (store != current)
            throw new IllegalStateException("This operation has already been cleared");
        PostExecuteContext result = current.complete();
        current.commands().markDormant();
        current.commandsForKey().markDormant();
        current = null;
        applyPostExecuteCtx(result);
    }

    private PreExecuteContext createPreExecuteCtx(PreLoadContext preLoadContext)
    {
        Map<TxnId, Command> commands = new HashMap<>();
        Map<RoutableKey, CommandsForKey> commandsForKeys = new HashMap<>();
        for (TxnId txnId : preLoadContext.txnIds())
        {
            Command command = state.command(txnId);
            commands.put(txnId, command != null ? command : Command.EMPTY);
        }
        for (Seekable seekable : preLoadContext.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    RoutableKey key = (RoutableKey) seekable;
                    CommandsForKey cfk = state.commandsForKey((Key) key);
                    commandsForKeys.put(key, cfk != null ? cfk : CommandsForKey.EMPTY);
                    break;
                case Range:
                    // load range cfks here
            }
        }
        return PreExecuteContext.of(preLoadContext, commands, commandsForKeys);
    }

    private void applyPostExecuteCtx(PostExecuteContext context)
    {
        context.commands.forEach(((txnId, update) -> {
            switch (txnId.domain())
            {
                case Key:
                    state.commands.put(txnId, update.current());
                    return;
                case Range:
                    RangeCommand existing = state.rangeCommands.get(txnId);
                    RangeCommand updated = new RangeCommand(update.current());
                    updated.update(existing.ranges);;
                    state.rangeCommands.put(txnId, updated);
                    return;
                default:
                    throw new IllegalStateException();
            }
        }));
        context.commandsForKey.forEach((key, update) -> state.commandsForKey.put(key, update.current()));
    }

    private <T> T executeInContext(CommandStore commandStore, PreLoadContext preLoadContext, Function<? super SafeCommandStore, T> function, boolean isDirectCall)
    {
        SafeCommandStore safeStore = ((InMemoryCommandStore)commandStore).beginOperation(preLoadContext);
        try
        {
            return function.apply(safeStore);
        }
        catch (Throwable t)
        {
            if (isDirectCall) logger.error("Uncaught exception", t);
            throw t;
        }
        finally
        {
            ((InMemoryCommandStore)commandStore).completeOperation(safeStore);
        }
    }

    protected <T> T executeInContext(CommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function)
    {
        return executeInContext(commandStore, context, function, true);

    }

    protected <T> void executeInContext(CommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
    {
        try
        {
            T result = executeInContext(commandStore, context, function, false);
            callback.accept(result, null);
        }
        catch (Throwable t)
        {
            logger.error("Uncaught exception", t);
            callback.accept(null, t);
        }
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        Runnable active = null;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        public Synchronized(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        public synchronized boolean containsCommand(TxnId txnId)
        {
            return state.commands.containsKey(txnId);
        }

        @Override
        public synchronized Command command(TxnId txnId)
        {
            return state.commands.get(txnId);
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        private synchronized void maybeRun()
        {
            if (active != null)
                return;

            active = queue.poll();
            while (active != null)
            {
                try
                {
                    active.run();
                }
                catch (Throwable t)
                {
                    logger.error("Uncaught exception", t);
                }
                active = queue.poll();
            }
        }

        private void enqueueAndRun(Runnable runnable)
        {
            boolean result = queue.add(runnable);
            if (!result)
                throw new IllegalStateException("could not add item to queue");
            maybeRun();
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                public void begin(BiConsumer<? super T, Throwable> callback)
                {
                    enqueueAndRun(() -> executeSync(context, function, callback));
                }
            };
        }


        private synchronized <T> void executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
        {
            executeInContext(this, context, function, callback);
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends InMemoryCommandStore
    {
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();
        private final ExecutorService executor;

        public SingleThread(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpoch);
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ']');
                return thread;
            });
        }

        void assertThread()
        {
            Thread current = Thread.currentThread();
            Thread expected;
            while (true)
            {
                expected = expectedThread.get();
                if (expected != null)
                    break;
                expectedThread.compareAndSet(null, Thread.currentThread());
            }
            if (expected != current)
                throw new IllegalStateException(String.format("Command store called from the wrong thread. Expected %s, got %s", expected, current));
        }

        @Override
        public boolean containsCommand(TxnId txnId)
        {
            assertThread();
            return state.commands.containsKey(txnId);
        }

        @Override
        public Command command(TxnId txnId)
        {
            assertThread();
            return state.commands.get(txnId);
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return AsyncChains.ofCallable(executor, () -> executeInContext(this, context, function));
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class Debug extends SingleThread
    {
        private class DebugInMemorySafeStore extends InMemorySafeStore
        {
            public DebugInMemorySafeStore(InMemoryState state, CFKLoader cfkLoader, PreExecuteContext context)
            {
                super(state, cfkLoader, context);
            }

            @Override
            public Command ifPresent(TxnId txnId)
            {
                assertThread();
                return super.ifPresent(txnId);
            }

            @Override
            public Command ifLoaded(TxnId txnId)
            {
                assertThread();
                return super.ifLoaded(txnId);
            }

            @Override
            public Command command(TxnId txnId)
            {
                assertThread();
                return super.command(txnId);
            }

            public CommandsForKey commandsForKey(RoutableKey key)
            {
                assertThread();
                return super.commandsForKey(key);
            }

            @Override
            public void addAndInvokeListener(TxnId txnId, TxnId listenerId)
            {
                assertThread();
                super.addAndInvokeListener(txnId, listenerId);
            }

            @Override
            public InMemoryCommandStore commandStore()
            {
                assertThread();
                return super.commandStore();
            }

            @Override
            public DataStore dataStore()
            {
                assertThread();
                return super.dataStore();
            }

            @Override
            public Agent agent()
            {
                assertThread();
                return super.agent();
            }

            @Override
            public ProgressLog progressLog()
            {
                assertThread();
                return super.progressLog();
            }

            @Override
            public RangesForEpoch ranges()
            {
                assertThread();
                return super.ranges();
            }

            @Override
            public long latestEpoch()
            {
                assertThread();
                return super.latestEpoch();
            }

            @Override
            public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
            {
                assertThread();
                return super.maxConflict(keysOrRanges, slice);
            }

            @Override
            public void addListener(TxnId command, TxnId listener)
            {
                assertThread();
                super.addListener(command, listener);
            }
        }

        public Debug(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        protected InMemorySafeStore createCommandStore(InMemoryState state, PreExecuteContext context)
        {
            return new DebugInMemorySafeStore(state, cfkLoader, context);
        }
    }

    public static InMemoryState inMemory(CommandStore unsafeStore)
    {
        return (unsafeStore instanceof Synchronized) ? ((Synchronized) unsafeStore).state : ((SingleThread) unsafeStore).state;
    }

    public static InMemoryState inMemory(SafeCommandStore safeStore)
    {
        return inMemory(safeStore.commandStore());
    }
}
