package accord.messages;

import java.util.IdentityHashMap;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Listener;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;

// TODO: use something more efficient? could probably assign each CommandStore a unique ascending integer and use an int[]
class Defer extends IdentityHashMap<CommandStore, Boolean> implements Listener
{
    final Status waitUntil;
    final Request request;
    final Node node;
    final Node.Id replyToNode;
    final ReplyContext replyContext;
    boolean isDone;

    Defer(Status waitUntil, Request request, Node node, Id replyToNode, ReplyContext replyContext)
    {
        this.waitUntil = waitUntil;
        this.request = request;
        this.node = node;
        this.replyToNode = replyToNode;
        this.replyContext = replyContext;
    }

    void add(Command command, CommandStore commandStore)
    {
        if (isDone)
            throw new IllegalStateException("Recurrent retry of " + request);

        put(commandStore, Boolean.TRUE);
        command.addListener(this);
    }

    @Override
    public void onChange(Command command)
    {
        int c = command.status().logicalCompareTo(waitUntil);
        if (c < 0) return;
        command.removeListener(this);
        if (c > 0) return;

        remove(command.commandStore);
        if (isEmpty())
        {
            isDone = true;
            request.process(node, replyToNode, replyContext);
        }
    }
}

