package kr.jm.test.kkb.profiler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.test.kkb.input.StringKafkaConsumer;
import kr.jm.test.kkb.output.StringKafkaProducer;
import kr.jm.test.kkb.profiler.repository.ProfileRepository;
import kr.jm.test.kkb.transaction.log.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import spark.Spark;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TransactionLogProfiler implements AutoCloseable {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(TransactionLogProfiler.class);

    private ObjectMapper objectMapper;
    private Map<TransactionType, Class<? extends TransactionLogInterface>>
            transactionTypeClassMap;
    private ProfileRepository profileRepository;
    private StringKafkaConsumer stringKafkaConsumer;
    private StringKafkaProducer stringKafkaProducer;

    public TransactionLogProfiler(boolean isLatest, String bootstrapServers,
            String inputTopic, String outputTopic) {
        this.objectMapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
        this.transactionTypeClassMap = new EnumMap<>(TransactionType.class);
        this.transactionTypeClassMap
                .put(TransactionType.NEW_USER, NewUser.class);
        this.transactionTypeClassMap
                .put(TransactionType.OPENING_ACCOUNT, OpeningAccount.class);
        this.transactionTypeClassMap
                .put(TransactionType.DEPOSIT, Deposit.class);
        this.transactionTypeClassMap
                .put(TransactionType.WITHDRAW, Withdraw.class);
        this.transactionTypeClassMap
                .put(TransactionType.TRANSFER, Transfer.class);
        this.stringKafkaConsumer =
                new StringKafkaConsumer(isLatest, bootstrapServers,
                        "TransactionLogProfiler",
                        consumerRecords -> consumerRecords.forEach(
                                consumerRecord -> consumeTransactionLog(
                                        consumerRecord.value())),
                        inputTopic);
        this.stringKafkaProducer =
                new StringKafkaProducer(bootstrapServers, outputTopic);
        this.profileRepository = new ProfileRepository();
        this.profileRepository
                .addCreateCustomerProfileConsumer(
                        customerProfile -> produceProfile(
                                customerProfile.getCustomer_number(),
                                customerProfile));
        this.profileRepository
                .addCreateAccountProfileConsumer(
                        accountProfile -> produceProfile(
                                accountProfile.getCustomer_number(),
                                accountProfile));
        this.profileRepository
                .addCreateAccountHistoryConsumer(
                        accountHistory -> produceProfile(
                                accountHistory.getCustomerNumber(),
                                accountHistory));
    }

    private Future<RecordMetadata> produceProfile(int customerNumber,
            Object profile) {
        return stringKafkaProducer
                .sendStringData(String.valueOf(customerNumber),
                        buildJsonString(profile));
    }

    ProfileRepository consumeTransactionLog(String transactionLogJsonString) {
        Optional.ofNullable(extractTransactionType(transactionLogJsonString))
                .map(transactionTypeClassMap::get)
                .map(tClass -> convertToTransactionLog(transactionLogJsonString,
                        tClass))
                .ifPresent(profileRepository::createTransactionLogProfile);
        return profileRepository;
    }

    private <T extends TransactionLogInterface> T convertToTransactionLog(
            String transactionLogJsonString, Class<T> tClass) {
        try {
            return objectMapper.readValue(transactionLogJsonString, tClass);
        } catch (IOException e) {
            log.error("convertToTransactionLog({}, {})",
                    transactionLogJsonString, tClass, e);
            return null;
        }
    }

    private TransactionType extractTransactionType(
            String transactionLogJsonString) {
        try {
            return TransactionType
                    .valueOf(objectMapper.readTree(transactionLogJsonString)
                            .get("transactionType").asText());
        } catch (Exception e) {
            log.error("extractTransactionType({})", transactionLogJsonString,
                    e);
            return null;
        }
    }

    private String buildJsonString(Object dataObject) {
        try {
            return dataObject != null ? objectMapper.writeValueAsString
                    (dataObject) : null;
        } catch (Exception e) {
            log.error("buildJSonString({})", dataObject, e);
            return null;
        }
    }


    public ProfileRepository getProfileRepository() {
        return profileRepository;
    }

    @Override
    public void close() {
        log.info("close");
        this.stringKafkaProducer.close();
        this.stringKafkaConsumer.close();
    }

    public void start() {
        log.info("start - {}, {}", stringKafkaConsumer.getTopicList(),
                stringKafkaConsumer.getGroupId());
        this.stringKafkaConsumer.start();
    }

    public static void main(String... args) {
        if (args.length < 3) {
            String message =
                    "Wrong Args !!! - Args: <kafkaConnect> <inputTopic> " +
                            "<outputTopic>";
            System.err.println(message);
        }

        String bootstrapServers = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];
        TransactionLogProfiler transactionLogProfiler =
                new TransactionLogProfiler(false, bootstrapServers,
                        inputTopic, outputTopic);
        transactionLogProfiler.start();
        Executors.newSingleThreadExecutor()
                .execute(() -> initSpark(transactionLogProfiler));
        Runtime.getRuntime()
                .addShutdownHook(new Thread(transactionLogProfiler::stop));
    }

    private static void initSpark(
            TransactionLogProfiler transactionLogProfiler) {
        Spark.initExceptionHandler(
                e -> log.error("Rest API Error Occur !!!", e));
        Spark.notFound((req, res) -> {
            res.type("application/json");
            return "{\"message\":\"Custom 404\"}";
        });
        Spark.get("/hello", (req, res) -> "Hello World");
        Spark.get("/api/customer/:customerNumber", (req, res) ->
                transactionLogProfiler
                        .getCustomerProfileAsJson(
                                req.params(":customerNumber")));
        Spark.get("/api/customer/:customerNumber/account/:accountNumber",
                (req, res) -> transactionLogProfiler
                        .getAccountProfileAsJson(req.params(":customerNumber"),
                                req.params(":accountNumber")));
    }

    private String getAccountProfileAsJson(String customerNumber,
            String accountNumber) {
        return buildJsonString(profileRepository
                .getAccountProfile(Integer.valueOf(customerNumber),
                        Integer.valueOf(accountNumber)));
    }

    private String getCustomerProfileAsJson(String customerNumber) {
        return buildJsonString(profileRepository
                .getUserProfile(Integer.valueOf(customerNumber)));
    }

    public void stop() {
        close();
        Spark.stop();
    }

}
