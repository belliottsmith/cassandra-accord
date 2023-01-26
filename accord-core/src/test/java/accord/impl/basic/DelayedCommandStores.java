package accord.impl.basic;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.impl.InMemoryCommandStore;
import accord.local.AsyncCommandStores;
import accord.local.CommandStores;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;

import java.util.Random;

public class DelayedCommandStores extends AsyncCommandStores
{
    private DelayedCommandStores(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, DelayedExecutorService executorService)
    {
        super(time, agent, store, shardDistributor, progressLogFactory, InMemoryCommandStore.SingleThread.factory(executorService));
    }

    public static CommandStores.Factory factory(PendingQueue pending, Random random)
    {
        DelayedExecutorService executorService = new DelayedExecutorService(pending, random);
        return (time, agent, store, shardDistributor, progressLogFactory) -> new DelayedCommandStores(time, agent, store, shardDistributor, progressLogFactory, executorService);
    }
}
