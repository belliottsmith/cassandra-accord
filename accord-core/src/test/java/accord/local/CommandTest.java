package accord.local;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.local.CommandStore.Synchronized;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Route;
import accord.topology.Topology;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.Utils.id;
import static accord.Utils.writeTxn;

public class CommandTest
{
    private static final Node.Id ID1 = id(1);
    private static final Node.Id ID2 = id(2);
    private static final Node.Id ID3 = id(3);
    private static final List<Node.Id> IDS = List.of(ID1, ID2, ID3);
    private static final KeyRange FULL_RANGE = IntKey.range(0, 100);
    private static final KeyRanges FULL_RANGES = KeyRanges.single(FULL_RANGE);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, FULL_RANGE);
    private static final IntKey KEY = IntKey.key(10);
    private static final RoutingKey HOME_KEY = KEY.toRoutingKey();
    private static final Route ROUTE = new Route(KEY, new RoutingKey[] { KEY });

    private static class CommandStoreSupport
    {
        final AtomicReference<Topology> local = new AtomicReference<>(TOPOLOGY);
        final MockStore data = new MockStore();
        final AtomicReference<Timestamp> nextTimestamp = new AtomicReference<>(Timestamp.NONE);
        final Function<Timestamp, Timestamp> uniqueNow = atleast -> {
            Timestamp next = nextTimestamp.get();
            Assertions.assertTrue(next.compareTo(atleast) >= 0);
            return next;
        };
    }

    private static void setTopologyEpoch(AtomicReference<Topology> topology, long epoch)
    {
        topology.set(topology.get().withEpoch(epoch));
    }

    private static CommandStore createStore(CommandStoreSupport storeSupport)
    {
        return null;
        // TODO (now): !
//        return new CommandStore.Synchronized(0,
//                                             0,
//                                             1,
//                                             ID1,
//                                             storeSupport.uniqueNow,
//                                             new TestAgent(),
//                                             storeSupport.data,
//                                             storeSupport.local.get().ranges(),
//                                             storeSupport.local::get);
    }

    @Test
    void noConflictWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        CommandStore commands = createStore(support);
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = clock.idForNode(1, 1);
        Txn txn = writeTxn(Keys.of(KEY));

        Command command = new Command(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        command.preaccept(txn.slice(FULL_RANGES, true), ROUTE, HOME_KEY);
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(txnId, command.executeAt());
    }

    @Test
    void supersedingEpochWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        CommandStore commands = createStore(support);
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = clock.idForNode(1, 1);
        Txn txn = writeTxn(Keys.of(KEY));


        Command command = new Command(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        setTopologyEpoch(support.local, 2);
        Timestamp expectedTimestamp = new Timestamp(2, 110, 0, ID1);
        support.nextTimestamp.set(expectedTimestamp);
        commands.process((Consumer<? super CommandStore>) cstore -> command.preaccept(txn.slice(FULL_RANGES, true), ROUTE, HOME_KEY));
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(expectedTimestamp, command.executeAt());
    }
}
