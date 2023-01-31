package accord.messages;

import java.util.function.Function;

import accord.local.*;
import accord.local.Status.Known;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;

import static accord.messages.Defer.Ready.Expired;
import static accord.messages.Defer.Ready.No;
import static accord.messages.Defer.Ready.Yes;

class Defer implements CommandListener
{
    public enum Ready { No, Yes, Expired }

    final Function<Command, Ready> waitUntil;
    final TxnRequest<?> request;
    final IntHashSet waitingOn = new IntHashSet();
    int waitingOnCount;
    boolean isDone;

    Defer(Known waitUntil, Known expireAt, TxnRequest<?> request)
    {
        this(command -> {
            if (!waitUntil.isSatisfiedBy(command.known()))
                return No;
            if (expireAt.isSatisfiedBy(command.known()))
                return Expired;
            return Yes;
        }, request);
    }

    Defer(Function<Command, Ready> waitUntil, TxnRequest<?> request)
    {
        this.waitUntil = waitUntil;
        this.request = request;
    }

    void add(Command command, CommandStore commandStore)
    {
        if (isDone)
            throw new IllegalStateException("Recurrent retry of " + request);

        waitingOn.add(commandStore.id());
        ++waitingOnCount;
        command.addListener(this);
    }

    @Override
    public void onChange(SafeCommandStore safeStore, Command command)
    {
        Ready ready = waitUntil.apply(command);
        if (ready == No) return;
        command.removeListener(this);
        if (ready == Expired) return;

        int id = safeStore.commandStore().id();
        if (!waitingOn.contains(id))
            throw new IllegalStateException();
        waitingOn.remove(id);

        if (0 == --waitingOnCount)
        {
            isDone = true;
            request.process();
        }
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        Invariants.checkState(caller.equals(request.txnId));
        return request;
    }
}

