package accord.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import accord.api.Key;
import accord.local.Command;
import accord.local.ContextValue;
import accord.local.ImmutableState;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

public abstract class AbstractSafeCommandStore implements SafeCommandStore
{

    public static class ContextState<Key, Value extends ImmutableState, Update>
    {
        private final Value emptySentinel;
        private final Map<Key, ContextValue.WithUpdate<Value, Update>> values;
        private final Set<Key> pendingUpdates = new HashSet<>();
        private boolean completed = false;

        public ContextState(Value emptySentinel, Map<Key, Value> originals)
        {
            this.emptySentinel = emptySentinel;
            this.values = new HashMap<>();
            originals.forEach((k, v) -> {
                if (v != emptySentinel)
                {
                    v.checkIsDormant();
                    v.markActive();
                }
                values.put(k, new ContextValue.WithUpdate<>(v));
            });
        }

        private void checkState()
        {
            if (completed)
                throw new IllegalStateException("Context has been closed");
        }

        private Value getOriginal(Key key)
        {
            Value val = ContextValue.original(values.get(key));
            return val != emptySentinel ? val : null;
        }

        private Value getCurrent(Key key)
        {
            Value val = ContextValue.current(values.get(key));
            return val != emptySentinel ? val : null;
        }

        public Value get(Key key)
        {
            checkState();
            return ContextValue.current(values.get(key));
        }

        public void addOriginal(Key key, Value item)
        {
            Invariants.checkArgument(!values.containsKey(key));
            values.put(key, new ContextValue.WithUpdate<>(item));
        }

        public void setCurrent(Key key, Value current)
        {
            ContextValue.WithUpdate<Value, Update> value = values.get(key);
            Invariants.checkState(value != null && value.current() == emptySentinel);
            value.current(current);
        }

        public Update beginUpdate(Key key, Value item, Function<Value, Update> factory)
        {
            checkState();
            ContextValue.WithUpdate<Value, Update> value = values.get(key);
            if (value == null)
                throw new IllegalStateException("No state in context for " + key);
            if (value.current() != item)
                throw new IllegalStateException(String.format("Attempting to update invalid state for %s", key));

            if (value.update() != null)
                throw new IllegalStateException(String.format("Update already in progress for %s", key));

            Update update = factory.apply(item);
            value.update(update);
            pendingUpdates.add(key);
            return update;
        }

        public void completeUpdate(Key key, Update update, Value prev, Value next)
        {
            checkState();
            ContextValue.WithUpdate<Value, Update> value = values.get(key);
            if (value == null)
                throw new IllegalStateException("No state in context for " + key);
            if (prev != null) prev.checkIsSuperseded();
            if (next != null) next.checkIsCurrent();
            Value currentVal = value.current();
            if (currentVal == emptySentinel)
                currentVal = null;
            Update pending = value.update();
            if (pending == null)
                throw new IllegalStateException(String.format("Update doesn't exist for %s", key));
            if (pending != update)
                throw new IllegalStateException(String.format("Update was already completed for %s", key));

            value.clearUpdate();
            pendingUpdates.remove(key);
            if (currentVal != prev)
                throw new IllegalStateException(String.format("Current value for %s differs from expected value (%s != %s)", key, value.current(), prev));
            value.current(next);
        }

        public ImmutableMap<Key, ContextValue<Value>> complete()
        {
            checkState();
            if (!pendingUpdates.isEmpty())
                throw new IllegalStateException("Pending updates left uncompleted: " + pendingUpdates);

            ImmutableMap.Builder<Key, ContextValue<Value>> result = ImmutableMap.builder();
            values.forEach((key, v) -> {
                if (v.update() != null)
                    throw new IllegalStateException("Pending update left uncompleted for " + key);

                Value original = v.original();
                if (original == emptySentinel)
                    original = null;

                Value current = v.current();
                if (current == emptySentinel)
                    current = null;

                result.put(key, new ContextValue<>(original, current));
            });
            completed = true;
            return result.build();
        }

        public void checkActive()
        {
            values.forEach((k, v) -> {
                if (v.current() != emptySentinel)
                    v.current().checkIsActive();
            });
        }

        public void markDormant()
        {
            values.forEach((k, v) -> {
                if (v.current() != emptySentinel)
                    v.current().markDormant();
            });
        }
    }

    protected final PreExecuteContext context;
    protected final ContextState<TxnId, Command, Command> commands;
    protected final ContextState<RoutableKey, CommandsForKey, CommandsForKey.Update> commandsForKey;

    public AbstractSafeCommandStore(PreExecuteContext context)
    {
        this.context = context;
        this.commands = new ContextState<>(Command.EMPTY, context.commands());
        this.commandsForKey = new ContextState<>(CommandsForKey.EMPTY, context.commandsForKey());
    }

    public ContextState<TxnId, Command, Command> commands()
    {
        return commands;
    }

    public ContextState<RoutableKey, CommandsForKey, CommandsForKey.Update> commandsForKey()
    {
        return commandsForKey;
    }

    @Override
    public Command ifPresent(TxnId txnId)
    {
        Command command = getIfLoaded(txnId, commands, this::getIfLoaded, Command.EMPTY);
        if (command == Command.EMPTY)
            return null;
        return command;
    }

    protected abstract Command getIfLoaded(TxnId txnId);

    static <K, V extends ImmutableState> V maybeConvertEmpty(K key, V value, ContextState<K, V, ?> context, Function<K, V> factory, V emptySentinel)
    {
        if (value == emptySentinel)
        {
            value = factory.apply(key);
            value.checkIsDormant();
            value.markActive();
            context.setCurrent(key, value);
        }
        return value;
    }

    static <K, V extends ImmutableState> V getIfLoaded(K key, ContextState<K, V, ?> context, Function<K, V> getIfLoaded, V emptySentinel)
    {
        V value = context.get(key);
        if (value != null)
            return value;

        value = getIfLoaded.apply(key);
        if (value == null)
            return null;

        if (value != emptySentinel)
        {
            value.checkIsDormant();
            value.markActive();
        }
        context.addOriginal(key, value);
        return value;
    }

    @Override
    public Command ifLoaded(TxnId txnId)
    {
        Command command = getIfLoaded(txnId, commands, this::getIfLoaded, Command.EMPTY);
        if (command == null)
            return null;
        command = maybeConvertEmpty(txnId, command, commands, Command.NotWitnessed::create, Command.EMPTY);
        command.checkIsActive();
        return command;
    }

    @Override
    public Command command(TxnId txnId)
    {
        Command command = commands.get(txnId);
        if (command == null)
            throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", txnId));
        command = maybeConvertEmpty(txnId, command, commands, Command.NotWitnessed::create, Command.EMPTY);
        command.checkIsActive();
        return command;
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

    protected abstract CommandsForKey getIfLoaded(RoutableKey key);

    public CommandsForKey maybeCommandsForKey(RoutableKey key)
    {
        CommandsForKey cfk = getIfLoaded(key, commandsForKey, this::getIfLoaded, CommandsForKey.EMPTY);
        if (cfk == CommandsForKey.EMPTY)
            return null;
        return cfk;
    }

    @Override
    public boolean canExecuteWith(PreLoadContext context)
    {
        return context.isSubsetOf(this.context);
    }

    protected abstract Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice);

    @Override
    public Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys)
    {
        Timestamp max = maxConflict(keys, ranges().at(txnId.epoch()));
        long epoch = latestEpoch();
        long now = time().now();
        if (txnId.compareTo(max) > 0 && txnId.epoch() >= epoch && !agent().isExpired(txnId, now))
            return txnId;

        return time().uniqueNow(max);
    }

    @Override
    public <C extends Command> C update(Command prev, C updated)
    {
        commands.completeUpdate(updated.txnId(), updated, prev, updated);
        return updated;
    }

    protected CommandsForKey.Update createCommandsForKeyUpdate(CommandsForKey cfk)
    {
        return new CommandsForKey.Update(this, cfk);
    }

    public CommandsForKey.Update beginUpdate(CommandsForKey cfk)
    {
        return commandsForKey.beginUpdate(cfk.key(), cfk, this::createCommandsForKeyUpdate);
    }

    public void completeUpdate(CommandsForKey.Update update, CommandsForKey current, CommandsForKey updated)
    {
        commandsForKey.completeUpdate(update.key(), update, current, updated);
    }

    public PostExecuteContext complete()
    {
        return new PostExecuteContext(commands.complete(), commandsForKey.complete());
    }

    public abstract CommandsForKey.CommandLoader<?> cfkLoader();
}
