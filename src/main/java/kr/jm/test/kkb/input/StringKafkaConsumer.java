package kr.jm.test.kkb.input;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class StringKafkaConsumer extends KafkaConsumer<String, String> {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory
                    .getLogger(StringKafkaConsumer.class);

    private final AtomicBoolean isRunning = new AtomicBoolean();
    private final AtomicBoolean isPaused = new AtomicBoolean();
    private int pollIntervalMs = 100;
    private String[] topics;

    private String groupId;

    private ExecutorService kafkaConsumerThreadPool;

    private RecordsConsumer consumer;

    public StringKafkaConsumer(Properties properties,
            RecordsConsumer consumer,
            String... topics) {
        super(properties, Serdes.String().deserializer(),
                Serdes.String().deserializer());
        this.kafkaConsumerThreadPool = Executors.newSingleThreadExecutor();
        this.consumer = consumer;
        this.topics = topics;
        this.groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public StringKafkaConsumer(String bootstrapServers, String groupId,
            RecordsConsumer consumer, String... topics) {
        this(true, bootstrapServers, groupId, consumer, topics);
    }

    public StringKafkaConsumer(Boolean isLatest, String bootstrapServers,
            String groupId, RecordsConsumer consumer, String... topics) {
        this(buildProperties(isLatest, bootstrapServers, groupId, 1000,
                30000), consumer, topics);
    }

    public static Properties buildProperties(Boolean isLatest,
            String bootstrapServers, String groupId, int autoCommitIntervalMs,
            int sessionTimeoutMs) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                autoCommitIntervalMs);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                sessionTimeoutMs);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                Optional.ofNullable(isLatest)
                        .map(b -> b ? "latest" : "earliest").orElse("none"));
        return properties;
    }

    /**
     * Subscribe.
     *
     * @param topics the topics
     */
    public void subscribe(String... topics) {
        super.subscribe(Arrays.asList(topics));
    }

    /**
     * Start.
     */
    public void start() {
        log.info("start - {}, {}, {}", groupId, topics, pollIntervalMs);
        kafkaConsumerThreadPool.execute(this::consume);
        isRunning.set(true);
    }

    private void consume() {
        try {
            subscribe(topics);
            while (isRunning()) {
                handleConsumerRecords(poll(pollIntervalMs));
                checkPauseStatus();
            }
        } catch (Exception e) {
            if (isRunning())
                log.error("consume()", e);
        } finally {
            super.close();
        }
    }

    private void handleConsumerRecords(
            ConsumerRecords<String, String> consumerRecords) {
        log.debug("Consume Timestamp = {}, Record Count = {}",
                System.currentTimeMillis(), consumerRecords.count());
        consumer.accept(consumerRecords);
    }

    private void checkPauseStatus() throws InterruptedException {
        while (isPaused())
            Thread.sleep(100);
    }

    public boolean isRunning() {
        return this.isRunning.get();
    }

    public void setPaused(boolean isPaused) {
        log.info("setPaused({})", isPaused);
        this.isPaused.set(isPaused);
    }

    public boolean isPaused() {
        return this.isPaused.get();
    }

    @Override
    public void close() {
        isRunning.set(false);
        wakeup();
        try {
            kafkaConsumerThreadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(int pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public List<String> getTopicList() {
        return Arrays.asList(topics);
    }

    public String getGroupId() {
        return groupId;
    }

    @FunctionalInterface
    public interface RecordsConsumer
            extends Consumer<ConsumerRecords<String, String>> {
    }

}

