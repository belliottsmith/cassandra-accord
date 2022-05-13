package accord.burn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import accord.impl.IntHashKey;
import accord.impl.basic.Cluster;
import accord.impl.basic.PropagatingPendingQueue;
import accord.impl.basic.RandomDelayQueue.Factory;
import accord.impl.TopologyFactory;
import accord.impl.basic.Packet;
import accord.impl.basic.PendingQueue;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListRequest;
import accord.impl.list.ListResult;
import accord.impl.list.ListUpdate;
import accord.local.Node.Id;
import accord.api.Key;
import accord.txn.Txn;
import accord.primitives.Keys;
import accord.verify.StrictSerializabilityVerifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTest.class);

    static List<Packet> generate(Random random, List<Id> clients, List<Id> nodes, int keyCount, int operations)
    {
        List<Key> keys = new ArrayList<>();
        for (int i = 0 ; i < keyCount ; ++i)
            keys.add(IntHashKey.key(i));

        List<Packet> packets = new ArrayList<>();
        int[] next = new int[keyCount];

        for (int count = 0 ; count < operations ; ++count)
        {
            Id client = clients.get(random.nextInt(clients.size()));
            Id node = nodes.get(random.nextInt(clients.size()));

            int readCount = 1 + random.nextInt(2);
            int writeCount = random.nextInt(3);

            TreeSet<Key> requestKeys = new TreeSet<>();
            while (readCount-- > 0)
                requestKeys.add(randomKey(random, keys, requestKeys));

            ListUpdate update = new ListUpdate();
            while (writeCount-- > 0)
            {
                int i = randomKeyIndex(random, keys, update.keySet());
                update.put(keys.get(i), ++next[i]);
            }

            requestKeys.addAll(update.keySet());
            ListRead read = new ListRead(new Keys(requestKeys));
            ListQuery query = new ListQuery(client, count, read.keys, update);
            ListRequest request = new ListRequest(new Txn(new Keys(requestKeys), read, query, update));
            packets.add(new Packet(client, node, count, request));
        }

        return packets;
    }

    private static Key randomKey(Random random, List<Key> keys, Set<Key> notIn)
    {
        return keys.get(randomKeyIndex(random, keys, notIn));
    }

    private static int randomKeyIndex(Random random, List<Key> keys, Set<Key> notIn)
    {
        int i;
        while (notIn.contains(keys.get(i = random.nextInt(keys.size()))));
        return i;
    }

    static void burn(TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException
    {
        Random random = new Random();
        long seed = random.nextLong();
        System.out.println(seed);
        random.setSeed(seed);
        burn(random, topologyFactory, clients, nodes, keyCount, operations, concurrency);
    }

    static void burn(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException
    {
        Random random = new Random();
        System.out.println(seed);
        random.setSeed(seed);
        burn(random, topologyFactory, clients, nodes, keyCount, operations, concurrency);
    }

    static void reconcile(long seed, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency) throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        ReconcilingLogger logReconciler = new ReconcilingLogger(logger);

        Random random1 = new Random(), random2 = new Random();
        random1.setSeed(seed);
        random2.setSeed(seed);
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<?> f1;
        try (ReconcilingLogger.Session session = logReconciler.nextSession())
        {
            f1 = exec.submit(() -> burn(random1, topologyFactory, clients, nodes, keyCount, operations, concurrency));
        }

        Future<?> f2;
        try (ReconcilingLogger.Session session = logReconciler.nextSession())
        {
            f2 = exec.submit(() -> burn(random2, topologyFactory, clients, nodes, keyCount, operations, concurrency));
        }
        exec.shutdown();
        f1.get();
        f2.get();

        assert logReconciler.reconcile();
    }

    static void burn(Random random, TopologyFactory topologyFactory, List<Id> clients, List<Id> nodes, int keyCount, int operations, int concurrency)
    {
        // TODO (now): error handling is bad and inconsistent, some definitely failing to be reported
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        PendingQueue queue = new PropagatingPendingQueue(failures, new Factory(random).get());

        StrictSerializabilityVerifier strictSerializable = new StrictSerializabilityVerifier(keyCount);

        Packet[] requests = generate(random, clients, nodes, keyCount, operations).toArray(Packet[]::new);
        int[] starts = new int[requests.length];
        Packet[] replies = new Packet[requests.length];

        AtomicInteger acks = new AtomicInteger();
        AtomicInteger nacks = new AtomicInteger();
        AtomicInteger clock = new AtomicInteger();
        AtomicInteger requestIndex = new AtomicInteger();
        for (int max = Math.min(concurrency, requests.length) ; requestIndex.get() < max ; )
        {
            int i = requestIndex.getAndIncrement();
            starts[i] = clock.incrementAndGet();
            queue.add(requests[i]);
        }

        // not used for atomicity, just for encapsulation
        Consumer<Packet> responseSink = packet -> {
            ListResult reply = (ListResult) packet.message;
            if (replies[(int)packet.replyId] != null)
                return;

            if (requestIndex.get() < requests.length)
            {
                int i = requestIndex.getAndIncrement();
                starts[i] = clock.incrementAndGet();
                queue.add(requests[i]);
            }

            try
            {

                int start = starts[(int)packet.replyId];
                int end = clock.incrementAndGet();

                logger.debug("{} at [{}, {}]", reply, start, end);

                replies[(int)packet.replyId] = packet;
                if (reply.keys == null)
                {
                    nacks.incrementAndGet();
                    return;
                }

                acks.incrementAndGet();
                strictSerializable.begin();

                for (int i = 0 ; i < reply.read.length ; ++i)
                {
                    Key key = reply.keys.get(i);
                    int k = key(key);

                    int[] read = reply.read[i];
                    int write = reply.update.getOrDefault(key, -1);

                    if (read != null)
                        strictSerializable.witnessRead(k, read);
                    if (write >= 0)
                        strictSerializable.witnessWrite(k, write);
                }

                strictSerializable.apply(start, end);
            }
            catch (Throwable t)
            {
                failures.add(t);
            }
        };

        Cluster.run(nodes.toArray(Id[]::new), () -> queue,
                    responseSink, failures::add,
                    () -> new Random(random.nextLong()),
                    () -> new AtomicLong()::incrementAndGet,
                    topologyFactory, () -> null);

        logger.info("Received {} acks and {} nacks ({} total) to {} operations\n", acks.get(), nacks.get(), acks.get() + nacks.get(), operations);
        if (clock.get() != operations * 2)
        {
            for (int i = 0 ; i < requests.length ; ++i)
            {
                logger.info("{}", requests[i]);
                logger.info("\t\t" + replies[i]);
            }
            throw new AssertionError("Incomplete set of responses");
        }
    }

    public static void main(String[] args) throws Exception
    {
//        long[] seeds = new long[] { 626863990011600453L,8583529384508063193L,-3444853481299271324L,6676617323471878597L,-4804379332709488479L,3479579412490552547L,6327303334037664345L,-8192613545211335195L,6323758985787667747L,-2302688428151789221L,-2141044851388513534L,3188432455892855862L,-6993845414394751388L,-4317637005144238616L,7829246061241610932L,-918921449809481674L,-7283295771782669701L,-4603787488231841671L,-3148931565724257431L,179837616132753636L,-5904756342597852669L,6419234615560855423L,465902875550204568L,-3462938150020321971L,-8442714202001747286L,6619562996549875393L,-809978648866547405L,6938719384277400135L,-5055009743263384608L,-1144743865490314487L,-5477742876188026995L,3390224924192947049L,-2709865146278118268L,-7126525170706402431L,5780011715045659831L,5999993711801064813L,8246638115985938481L,-8290570786339293922L,2188882367516443429L,-3884324262675383520L,3901201894874521865L,5721108360906620286L,-7305702243679368177L,-7286671479381706655L,4560368775318371928L,-436386150803657542L,3338592446754728290L,3489068868575180596L,8853731434787698267L,3353295539620966161L,-6164450591610510615L,1077576344069910622L,-4313027874654550166L,-4705245436284140915L,-1355642656942945178L,-3464514852966375659L,6385383399320183853L,-7221060467902231984L,-5103778446133573671L,-3131000437811812917L,8644771407931552819L,-2839775619279133991L,-3434335975609444999L,4534128117289840375L,-7718863450233787888L,9103764271940626264L,-1021158602138076250L,-3100492231010909496L,3362504493461170518L,-4292546544865651029L,-6110313408439435352L,6511554459503259265L,-7165756082552888143L,-738812080289401823L,-3712434777663269935L,7938565649294540047L,-1587475227173628599L,-1087191311381024183L,-6479699542385973894L,-1062363414259091681L,5342860228430669996L,2758676562116436982L,-2852525019368562417L,-5025249735701151631L,-3234433307849026045L,6850157294823652318L,2822255962993117041L,1937098658237932180L,6547163268148247390L,-6029622782521680166L,179580956951964868L,3732700046007835920L,-920226215090471642L,-3334371736113131383L,-8122029444894410664L,-1073538930424785015L,-8938225956572199868L,4703393306112737688L,8656934002825454755L,3074421550098668860L,-6540929384965007881L,9176759424047094313L,3377112787909805837L,-3617095772509226796L,3130787055403027054L,2692579591825428958L,7800053561192467851L,3771379007766301369L,-938110732773103790L,-5895130961057603930L,2393950531168581742L,1661694043955975394L,-1603659730016856656L,1136844776895055889L,511489498167943444L,4813206408749019951L,-8532832977841301739L,1893017325482302418L,-49832066826587920L,-552869017296538782L,-8398565991707695502L,-7352038252332511590L,8258956835311531147L,-3365016730933863000L,-5607313488937251373L,5877223761512654109L,-1569321822949015791L,1368774142700300823L,692637339525701837L,-5572176107668413628L,-6501555849303912584L,-5624873271301949498L,6101666211142734330L,-1698256708737981972L,-2786797194260634394L,-244217159526616607L,-6570175578447516213L,5790185004478020438L,4442925498405946982L,-1614043980996292010L,3648588173820722218L,1755175575745833543L,5864761362982129933L,4189064531191589661L,4985105127500804483L,3160978830294052170L,1675577962431449109L,3238806276217088524L,3746443145339833432L,-742977779341704850L,7586311559264263675L,5196513926845056216L,4467979141426638602L,-9916598016923627L,-7690992356895713110L,8603016195634269605L,-6737886295034221900L,-3594511789154987227L,8863353301521559607L,1287775192703083440L,3198639127623049913L,-6828877095885759743L,4301501762243970477L,-646264608135188996L,2327782366483051538L,-7625521362058421983L,6017891694859072015L,3951811033540802993L,2103708834929878988L,-8847235394386041249L,7117547568737147642L,4734366996644823234L,-3397081935575880299L,689557462618432631L,8569492915832760255L,7731868016483337395L,9106827836109292836L,5100377254707899509L,772087688540857788L,1624177548293496432L,-5376353781674158277L,-7008472491340613973L,2283488814225975721L,-1304665920487058124L,-3193219875060007145L,2895138679616474338L,-182230644140462111L,2146841180922491038L,-4832219817846524098L,8894542040592892155L,-3427163590834115937L,3450730853845605317L,6985151132473629753L,-2242814501375103893L,5523888917317517977L,8229031348033426000L,8499068812241148332L,-7266813829292685984L,2488431381569337887L,446141189265120782L,-5289238097239435951L,-4144246073844624077L,7657545060883933272L,-6489514652261346156L,-2355470519933822545L,-2951984289136223607L,-571777887285165779L,3394009697507381088L,-3376860395555921365L,6615343589041429084L,658586493318619440L,-5733179071405403220L,1945396714405174901L,-830479116531127809L,3007251791062392778L,-6716328363971034167L,4000596777697622038L,1148222429176800979L,1727200716555611734L,-5887282412247092453L,-3106277513221502545L,3729322506242334704L,-8018293103883443739L,5393285871950499935L,8654363800196553635L,-2073431323425721423L,-5372438019923065825L,-6703437597133293809L,2683670701121337820L,8719866217850765775L,-350699469932944805L,-8828314515535513394L,-2486475073840329037L,879785808919150654L,2973595266113571128L,-1511729246886304556L,-9001023715460694629L,-1992702297407851180L,8157075095585464092L,-2724303775215308251L,4316853530489920940L,-129772835268492350L,7572483916959019455L,-7281908315961613516L,-3971253338685375807L,-7303634530928180532L,5736141898752737750L,-745182931032206219L,3927584004420019873L,-1883218505267987232L,-950977876817181122L,3507972497682548601L,-7922902762823524156L,5537785953573201014L,-8438192571860942838L,-2406107424887483672L,405581664357432395L,-4904483318120034697L,-1947172217316900549L,-5835230526740480246L,-6779727077520753939L,-708597760236696478L,9152943141104483551L,7448652220096987873L,-5669978989335258386L,2788306722348697673L,-2187412369495316906L,-8903174333372474168L,1657726016037360294L,-2536639792031516292L,-4982899409274744900L,4697596134373929946L,4465576610198827516L,8516568823312179673L,-8251597861336731105L,-2137409450928481715L,-4960899187808439371L,-6654396861134688188L,991508970392471484L,422868985115236764L,-6592708395533199852L,-5644398576899847792L,-7895783691848222324L,-7749068457093029742L,-7970130950797714104L,1235441663761771372L,-7200114386927234336L,-1856993318527913970L,4676635441799737059L,6480687981724560105L,-7144439988027193492L,-5312685340660221127L,-5890278975493637062L,-2012770740885779118L,7139244219922949748L,-1546909207404966464L,-8148995728007372984L,-6764525529088555214L,404593216260480282L,3183060637206272485L,7803377045062753340L,4391328284131243586L,224642781194926969L,5030582524211455844L,8248290893201054451L,6323097057751339571L,-2225084639955136380L,-5240391238570649481L,-349146567577083609L,-4017310906945294057L,-1690304905974384308L,-3353486745143582570L,1121648797009552240L,8391161145482847322L,4991890354796943938L,-3710242744689698287L,-950129984646927096L,-8461879564763899725L,-2955490786456417491L,-2563740878803408261L,8346820901130073875L,-6179716182627137185L,-515941261205851498L,-3021373727797102328L,2345423453664928474L,2563451308451462022L,4737365669075977691L,-7990899706091676011L,6544584481791768305L,4183948025102945212L,-3825457670201020633L,6818884602437413225L,7010066264017852211L,1083549421284338692L,-8866883227988189118L,7928257743004902944L,9165474962424216856L,7682523230584585466L,3096010670988212302L,-7357939662008698680L,-7476306460534231915L,-5128698689840443470L,-6711525856693077214L,-5847144037246972900L,-1743253335817399L,8848398169422350998L,-790880296644445128L,-1086585856860993594L,-2222578038542783818L,-5513513449543647391L,8665949882054267636L,-347368559995247303L,-8843332400220203046L,290945265508020228L,-9011452510499720771L,-6599579254807576849L,3092715152546115938L,2784346131784541085L,-3545357801715111806L,-1333128458467877140L,4582115704649474455L,-9150795015014030372L,5957918870264100962L,-578126679318468670L,-2045722679034605396L,5937783010302080583L,-707511093785326573L,4968350461339941237L,229598964788930859L,-6455616781031104975L,8946394293812720291L,-8646695361368598869L,2876990540773707769L,-1507889474339450408L,5629249202898842431L,-4652436981930925598L,6378783536372116785L,-4519559202879789738L,-8200942677245337823L,2760880685670360289L,2458217616770426669L,4863051684402232404L,-6479543096611397915L,-1240300833890338863L,6120137479463949698L,3363102428639288557L,5669033144580666764L,-7149503475887733223L,-7571437603847242131L,-4726243383732000844L,-7261631692448261738L,-3437288313047460490L,2263899128860512458L,2117561965284765052L,-7641636754107529410L,-7006347296273148701L,785872931820776045L,9053850769647367864L,-7266524295484328314L,8781371962881042414L,7382115749183415653L,8748604047949957682L,5091646529717369505L,-5406273938091699375L,2116085921823539745L,-737291131019119962L,5493149452422251761L,6030872158182909309L,-6270845193615019980L,2876539655024301686L,5242086412481881063L,-5406359735301272722L,2557962337581654577L,-2010235046017619706L,310837439336726894L,-4015948458223316873L,-8792972904620205319L,-8755770673741885365L,8653794043075437706L,-556420369691056757L,-8029595883267010620L,-5942069198436463124L,-245988348517678653L,-8849909025311160269L,-3090055333658784549L,-1922192219024870097L,7223094097605750540L,-446385557437298433L,2357800716117881782L,-4844039550159025771L,3671033916939712977L,-8925463534943555332L,-4538457228981151058L,6859858024783427086L,-8800506516084835904L,-2841610836589486586L,-1399614579962358921L,-6028608249234090996L,-2030523177775081986L,-4229472096296130996L,1827212113066463835L,6739147498460994047L,-5654329345789305569L,7708157457178891920L,-7519070356471697037L,7396610793354721237L,-4968620265979731311L,-5109925894737798607L,8638280035970934169L,-5188694717118797042L,-3213253521967318505L,-3197442226000661768L,-4484462278527224923L,1281975599685900509L,8387907026686721537L,-3251175600117434587L,-7370588848530945737L,-167720566935007089L,-5311110026377106011L,5654675516249899551L,6240045399460511410L,-8370135698769382619L,-5971973336220103649L,5361626254039536632L,5815089446986266787L,-8965006410239464853L,5963918726236785069L,-1056913637309526198L,6693866719624736839L,-3554859562178966121L,2153974875584784366L,-7658289008305992188L,5505874265811410901L,-3684779152013358473L,-2163950963725207625L,-6082545645404875510L,-7457221140576449749L,-5239860607379821724L,-7926641197723071624L,-2031698184367508214L,-8000194431527524443L,3169895010102412748L,-9212375792274258187L,-3943266035288758194L,8701371880526060974L,3993007578132958911L,6571712975695241870L,837105448793109562L,429532422280654215L,7528415190305215784L,-1476058954617382002L,-1664194934542307509L,4165822777616170433L,-7061296171833356749L,5111799465246888034L,6288901195154792471L };
//        Long overrideSeed = null;
        Long overrideSeed = 2153330720604734761L;
//        for (int i = 0 ; i < seeds.length ; ++i)
//        {
//            long seed = seeds[i];
        do
        {
            long seed = overrideSeed != null ? overrideSeed : ThreadLocalRandom.current().nextLong();
            logger.info("Seed: {}", seed);
            Cluster.trace.trace("Seed: {}", seed);
            Random random = new Random(seed);
            try
            {
                List<Id> clients = generateIds(true, 1 + random.nextInt(4));
                List<Id> nodes = generateIds(false, 5 + random.nextInt(5));
                burn(random, new TopologyFactory<>(nodes.size() == 5 ? 3 : (2 + random.nextInt(3)), IntHashKey.ranges(4 + random.nextInt(12))),
                     clients,
                     nodes,
                     5 + random.nextInt(15),
                     200,
                     10 + random.nextInt(30));
            }
            catch (Throwable t)
            {
                logger.error("Exception running burn test for seed {}:", seed, t);
                throw t;
            }
        } while (overrideSeed == null);
//            }
//        }
    }

    private static List<Id> generateIds(boolean clients, int count)
    {
        List<Id> ids = new ArrayList<>();
        for (int i = 1; i <= count ; ++i)
            ids.add(new Id(clients ? -i : i));
        return ids;
    }

    private static int key(Key key)
    {
        return ((IntHashKey) key).key;
    }

    private static int[] append(int[] to, int append)
    {
        to = Arrays.copyOf(to, to.length + 1);
        to[to.length - 1] = append;
        return to;
    }
}
