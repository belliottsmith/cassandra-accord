package accord.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;

public class ReadData extends WhenReadyToExecute
{
    private static final Logger logger = LoggerFactory.getLogger(WhenReadyToExecute.class);

    public static class SerializerSupport
    {
        public static ReadData create(TxnId txnId, Seekables<?, ?> scope, long executeAtEpoch, long waitForEpoch)
        {
            return new ReadData(txnId, scope, executeAtEpoch, waitForEpoch);
        }
    }

    private Data data;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Seekables<?, ?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope, executeAt);
    }

    ReadData(TxnId txnId, Seekables<?, ?> readScope, long executeAtEpoch, long waitForEpoch)
    {
        super(txnId, readScope, executeAtEpoch, waitForEpoch);
    }

    @Override
    public ExecuteType kind()
    {
        return ExecuteType.readData;
    }

    @Override
    protected void readyToExecute(SafeCommandStore safeStore, Command command)
    {
        logger.trace("{}: executing read", command.txnId());
        CommandStore unsafeStore = safeStore.commandStore();
        command.read(safeStore).addCallback((next, throwable) -> {
            if (throwable != null)
            {
                // TODO (expected, exceptions): should send exception to client, and consistency handle/propagate locally
                logger.trace("{}: read failed for {}: {}", txnId, unsafeStore, throwable);
                node.reply(replyTo, replyContext, ExecuteNack.Error);
            }
            else
            {
                synchronized (ReadData.this)
                {
                    if (next != null)
                        data = data == null ? next : data.merge(next);
                    onExecuteComplete(unsafeStore);
                }
            }
        });
    }

    @Override
    protected void failed()
    {
        data = null;
    }

    @Override
    protected void sendSuccessReply()
    {
        node.reply(replyTo, replyContext, new ExecuteOk(data));
    }
}
