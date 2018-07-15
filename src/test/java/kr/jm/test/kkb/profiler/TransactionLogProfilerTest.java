package kr.jm.test.kkb.profiler;

import kr.jm.test.kkb.profiler.repository.ProfileRepository;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMResources;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.kafka.JMKafkaBroker;
import kr.jm.utils.kafka.client.JMKafkaConsumer;
import kr.jm.utils.kafka.client.JMKafkaProducer;
import kr.jm.utils.zookeeper.JMZookeeperServer;
import org.apache.http.client.fluent.Request;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class TransactionLogProfilerTest {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
    }

    private String inputTopic = "testInputTopic";
    private String outputTopic = "testOutputTopic";
    private String bootstrapServer = "localhost:9300";
    private JMZookeeperServer zooKeeper;
    private JMKafkaBroker kafkaBroker;
    private JMKafkaProducer jmKafkaProducer;
    private JMKafkaConsumer jmKafkaConsumer;

    @Before
    public void setUp() throws Exception {
        Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        this.zooKeeper = new JMZookeeperServer("localhost", 12181);
        this.zooKeeper.start();
        JMThread.sleep(3000);
        this.kafkaBroker =
                new JMKafkaBroker(zooKeeper.getZookeeperConnect(), "localhost",
                        9300);
        this.kafkaBroker.startup();
        JMThread.sleep(3000);
        this.jmKafkaProducer =
                new JMKafkaProducer(bootstrapServer, inputTopic);

    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        jmKafkaProducer.close();
        jmKafkaConsumer.close();
        kafkaBroker.stop();
        zooKeeper.stop();
        Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath("kafka-broker-log"))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
    }


    @Test
    public void testMain() throws InterruptedException, IOException {
        JMResources.readLines("testArchive.log").forEach
                (jmKafkaProducer::send);
        TransactionLogProfiler.main(bootstrapServer, inputTopic, outputTopic);
        LongAdder accountHistoryCount = new LongAdder();
        this.jmKafkaConsumer = new JMKafkaConsumer(false, bootstrapServer,
                "testGroup", consumerRecords -> {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
                accountHistoryCount.increment();
            }
        }, outputTopic);
        this.jmKafkaConsumer.start();
        Thread.sleep(5000);

        String uri = "http://localhost:4567/api/customer/";
        String customerProfile =
                Request.Get(uri + "100").execute().returnContent().asString();
        System.out.println(customerProfile);
        Assert.assertEquals("{\"customer_number\":100,\"name\":\"제민\"," +
                "\"join_dt\":\"2018-07-14 01:24:06\"," +
                "\"largest_deposit_amount\":96700," +
                "\"largest_withdrawal_amount\":94900," +
                "\"largest_transfer_amount\":98500}", customerProfile);
        String accountProfile =
                Request.Get(uri + "101/account/12345685").execute()
                        .returnContent().asString();
        System.out.println(accountProfile);
        Assert.assertEquals(
                "{\"customer_number\":101,\"account_number\":12345685,\"create_dt\":\"2018-07-14 01:24:11\",\"balance\":-297000,\"deposits\":[{\"amount\":38100,\"datetime\":\"2018-07-14 01:24:11\"},{\"amount\":44200,\"datetime\":\"2018-07-14 01:24:15\"}],\"withdrawals\":[{\"amount\":37700,\"datetime\":\"2018-07-14 01:24:12\"},{\"amount\":56600,\"datetime\":\"2018-07-14 01:24:12\"},{\"amount\":69600,\"datetime\":\"2018-07-14 01:24:14\"},{\"amount\":34900,\"datetime\":\"2018-07-14 01:24:15\"}],\"transfers\":[{\"amount\":69500,\"datetime\":\"2018-07-14 01:24:13\"},{\"amount\":89700,\"datetime\":\"2018-07-14 01:24:15\"},{\"amount\":21300,\"datetime\":\"2018-07-14 01:24:18\"}]}",
                accountProfile);

        Assert.assertEquals(208, accountHistoryCount.longValue());
    }


    @Test
    public void testStart() throws InterruptedException {
        JMResources.readLines("testArchive.log").forEach
                (jmKafkaProducer::send);

        TransactionLogProfiler transactionLogProfiler =
                new TransactionLogProfiler(false, bootstrapServer, inputTopic,
                        outputTopic);
        transactionLogProfiler.start();
        LongAdder accountHistoryCount = new LongAdder();
        ProfileRepository profileRepository =
                transactionLogProfiler.getProfileRepository();
        this.jmKafkaConsumer = new JMKafkaConsumer(false, bootstrapServer,
                "testGroup", consumerRecords -> {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
                accountHistoryCount.increment();
            }
        }, outputTopic);
        this.jmKafkaConsumer.start();
        Thread.sleep(3000);
        transactionLogProfiler.close();
        System.out.println(profileRepository.getUserProfile(100));
        System.out.println(profileRepository.getAccountProfileMap(100));
        Assert.assertEquals(208, accountHistoryCount.longValue());
    }
}