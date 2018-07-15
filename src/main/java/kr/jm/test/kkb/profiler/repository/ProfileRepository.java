package kr.jm.test.kkb.profiler.repository;

import kr.jm.test.kkb.profiler.repository.profile.AccountHistory;
import kr.jm.test.kkb.profiler.repository.profile.AccountProfile;
import kr.jm.test.kkb.profiler.repository.profile.CustomerProfile;
import kr.jm.test.kkb.transaction.log.AbstractAmountTransactionLog;
import kr.jm.test.kkb.transaction.log.NewUser;
import kr.jm.test.kkb.transaction.log.OpeningAccount;
import kr.jm.test.kkb.transaction.log.TransactionLogInterface;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class ProfileRepository {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
    private static final ZoneId ZONE_ID = ZoneId.systemDefault();
    private Map<Integer, CustomerProfile> customerProfileRepository;
    private Map<Integer, Map<Integer, AccountProfile>> accountProfileRepository;
    private List<Consumer<CustomerProfile>> createCustomerProfileConsumerList;
    private List<Consumer<AccountProfile>> createAccountProfileConsumerList;
    private List<Consumer<AccountHistory>> createAccountHistoryConsumerList;
    private ExecutorService singleExecutorService;


    public ProfileRepository() {
        this.customerProfileRepository = new ConcurrentHashMap<>();
        this.accountProfileRepository = new ConcurrentHashMap<>();
        this.createCustomerProfileConsumerList = new ArrayList<>();
        this.createAccountProfileConsumerList = new ArrayList<>();
        this.createAccountHistoryConsumerList = new ArrayList<>();
        this.singleExecutorService = Executors.newSingleThreadExecutor();
    }

    public void addCreateCustomerProfileConsumer(Consumer<CustomerProfile>
            customerProfileConsumer) {
        this.createCustomerProfileConsumerList.add(customerProfileConsumer);
    }

    public void addCreateAccountProfileConsumer(
            Consumer<AccountProfile> accountProfileConsumer) {
        this.createAccountProfileConsumerList.add(accountProfileConsumer);
    }

    public void addCreateAccountHistoryConsumer(
            Consumer<AccountHistory> accountHistoryConsumer) {
        this.createAccountHistoryConsumerList.add(accountHistoryConsumer);
    }

    private <T> void consumeProfileAndHistory(List<Consumer<T>> consumerList,
            T profileAndHistory) {
        singleExecutorService.execute(() -> consumerList.forEach(consumer ->
                consumer.accept(profileAndHistory)));
    }


    public CustomerProfile getUserProfile(int customerNumber) {
        return customerProfileRepository.get(customerNumber);
    }

    public Map<Integer, AccountProfile> getAccountProfileMap(
            int customerNumber) {
        return Optional.ofNullable(accountProfileRepository.get(customerNumber))
                .orElseGet(Collections::emptyMap);
    }


    public AccountProfile getAccountProfile(int customerNumber,
            int accountNumber) {
        return getAccountProfileMap(customerNumber).get(accountNumber);
    }

    public ProfileRepository createTransactionLogProfile(
            TransactionLogInterface transactionLog) {
        switch (transactionLog.getTransactionType()) {
            case NEW_USER:
                return createUserProfile((NewUser) transactionLog);
            case OPENING_ACCOUNT:
                return createAccountProfile((OpeningAccount) transactionLog);
            default:
                return createAmountTransaction((AbstractAmountTransactionLog)
                        transactionLog);
        }
    }


    public ProfileRepository createUserProfile(NewUser newUser) {
        consumeProfileAndHistory(createCustomerProfileConsumerList,
                customerProfileRepository.computeIfAbsent
                        (newUser.getUserNumber(),
                                userNumber -> new CustomerProfile(
                                        userNumber,
                                        newUser.getUserName(),
                                        buildDataTime(
                                                newUser.getLogTimestamp()))));
        return this;
    }

    public ProfileRepository createAccountProfile(
            OpeningAccount openingAccount) {
        if (customerProfileRepository
                .containsKey(openingAccount.getUserNumber()))
            consumeProfileAndHistory(createAccountProfileConsumerList,
                    accountProfileRepository
                            .computeIfAbsent(openingAccount.getUserNumber(),
                                    accountNumber -> new ConcurrentHashMap<>())
                            .computeIfAbsent(
                                    openingAccount.getAccountNumber(),
                                    accountNumber -> new AccountProfile(
                                            openingAccount.getUserNumber(),
                                            accountNumber, buildDataTime(
                                            openingAccount
                                                    .getLogTimestamp()))));
        else
            log.error("No CustomerProfile Occur !!! - customerId = {}, " +
                            "OpeningAccount = {}", openingAccount.getUserNumber(),
                    openingAccount);
        return this;
    }

    public static String buildDataTime(long logTimestamp) {
        return LocalDateTime
                .ofInstant(Instant.ofEpochMilli(logTimestamp), ZONE_ID)
                .format(DATE_TIME_FORMATTER);
    }

    public ProfileRepository createAmountTransaction(
            AbstractAmountTransactionLog amountTransactionLog) {
        Optional<AccountHistory> accountHistoryAsOpt =
                Optional.ofNullable(
                        getUserProfile(amountTransactionLog.getUserNumber()))
                        .map(customerProfile -> setLargestAmount(
                                amountTransactionLog, customerProfile))
                        .map(this.accountProfileRepository::get)
                        .map(accountProfileMap -> accountProfileMap.get
                                (amountTransactionLog.getAccountNumber()))
                        .map(accountProfile -> accountProfile
                                .addHistoryTransactionLog(
                                        amountTransactionLog));
        if (!accountHistoryAsOpt.isPresent()) {
            log.error("Wrong Transaction Log Occur !!! - {}",
                    amountTransactionLog);
            return null;
        }
        accountHistoryAsOpt.ifPresent(accountHistory -> consumeProfileAndHistory(
                createAccountHistoryConsumerList, accountHistory));
        return this;
    }

    private int setLargestAmount(
            AbstractAmountTransactionLog amountTransactionLog,
            CustomerProfile customerProfile) {
        customerProfile
                .setLargestAccount(
                        amountTransactionLog.getTransactionType(),
                        amountTransactionLog.getAmount());
        return amountTransactionLog.getUserNumber();
    }

}
