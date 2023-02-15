package accord.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.local.Command;
import accord.local.Command.ApplyOutcome;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.primitives.Writes;

/*
 * Used by local and global inclusive sync points to effect the sync point at each node
 * Combines commit, execute (with nothing really to execute), and apply into one request/response
 */
public class WaitForDependenciesThenApply extends WhenReadyToExecute
{
    private static final Logger logger = LoggerFactory.getLogger(WhenReadyToExecute.class);

    public static class SerializerSupport
    {
        public static WaitForDependenciesThenApply create(TxnId txnId, PartialRoute<?> route, PartialDeps deps, Seekables<?, ?> partialTxnKeys, Writes writes, Result result)
        {
            return new WaitForDependenciesThenApply(txnId, route, deps, partialTxnKeys, writes, result);
        }
    }

    public final PartialRoute<?> route;
    public final PartialDeps deps;
    public final Writes writes;
    public final Result result;

    WaitForDependenciesThenApply(TxnId txnId, PartialRoute<?> route, PartialDeps deps, Seekables<?, ?> partialTxnKeys, Writes writes, Result result)
    {
        super(txnId, partialTxnKeys, txnId.epoch(), txnId.epoch());
        this.route = route;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
    }

    @Override
    protected void readyToExecute(SafeCommandStore safeStore, Command command)
    {
        logger.trace("{}: executing WaitForDependenciesThenApply", command.txnId());
        CommandStore unsafeStore = safeStore.commandStore();
        node.agent().onLocalBarrier(scope, txnId);
        // Send a response immediately once deps have been applied
        onExecuteComplete(unsafeStore);
        // Apply after, we aren't waiting for the side effects of this txn since it is an inclusive sync
        ApplyOutcome outcome = command.apply(safeStore, txnId.epoch(), route, txnId, deps, writes, result);
        switch (outcome)
        {
            default:
                throw new IllegalStateException("Unexpected outcome " + outcome);
            case Success:
                // TODO fine to ignore the other outcomes?
                break;
        }
    }

    @Override
    public ExecuteType kind()
    {
        return ExecuteType.waitAndApply;
    }

    @Override
    protected void failed()
    {
    }

    @Override
    protected void sendSuccessReply()
    {
        node.reply(replyTo, replyContext, new ExecuteOk(null));
    }
}
