package kr.jm.test.kkb.profiler.repository;

import kr.jm.test.kkb.transaction.log.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProfileRepositoryTest {

    private ProfileRepository profileRepository;

    @Before
    public void setUp() throws Exception {
        this.profileRepository = new ProfileRepository();
    }

    @Test
    public void testProfileRepository() throws InterruptedException {
        AtomicInteger customerProfileCount = new AtomicInteger();
        profileRepository.addCreateCustomerProfileConsumer(customerProfile ->
                customerProfileCount.incrementAndGet());
        AtomicInteger accountProfileCount = new AtomicInteger();
        profileRepository.addCreateAccountProfileConsumer(accountProfile ->
                accountProfileCount.incrementAndGet());
        AtomicLong accountHistoryAmount = new AtomicLong();
        profileRepository.addCreateAccountHistoryConsumer(accountHistory ->
        {
            switch (accountHistory.getTransactionType()) {
                case DEPOSIT:
                    accountHistoryAmount
                            .updateAndGet(amount -> amount +
                                    accountHistory.getAmount());
                    break;
                case WITHDRAW:
                case TRANSFER:
                    accountHistoryAmount.updateAndGet(
                            amount -> amount - accountHistory.getAmount());
                    break;
            }
        });
        int userNumber = 10;
        int accountNumber = 100;
        Long finalAmount = -9000L;
        Assert.assertEquals(finalAmount, profileRepository
                .createUserProfile(
                        new NewUser(userNumber, System.currentTimeMillis(),
                                "testName"))
                .createAccountProfile(new OpeningAccount(userNumber, System
                        .currentTimeMillis(), accountNumber))
                .createAmountTransaction(new Deposit(userNumber, System
                        .currentTimeMillis(), accountNumber, 10000))
                .createAmountTransaction(new Withdraw(userNumber, System
                        .currentTimeMillis(), accountNumber, 1000))
                .createAmountTransaction(new Transfer(userNumber, System
                        .currentTimeMillis(), accountNumber, 9000, "KKB",
                        122939333, "받는사람"))
                .createAmountTransaction(new Deposit(userNumber, System
                        .currentTimeMillis(), accountNumber, 1000))
                .createAmountTransaction(new Transfer(userNumber, System
                        .currentTimeMillis(), accountNumber, 1000, "KKB",
                        122939333, "받는사람"))
                .createAmountTransaction(new Withdraw(userNumber, System
                        .currentTimeMillis(), accountNumber, 10000))
                .createAmountTransaction(new Deposit(userNumber, System
                        .currentTimeMillis(), accountNumber, 11000))
                .createAmountTransaction(new Withdraw(userNumber, System
                        .currentTimeMillis(), accountNumber, 1000))
                .createAmountTransaction(new Transfer(userNumber, System
                        .currentTimeMillis(), accountNumber, 9000, "KKB",
                        122939333,
                        "받는사람")).getAccountProfile(userNumber, accountNumber)
                .getBalance());

        System.out.println(profileRepository.getUserProfile(userNumber));
        System.out.println(profileRepository.getAccountProfile(userNumber,
                accountNumber));

        Assert.assertEquals(11000, profileRepository.getUserProfile
                (userNumber).getLargest_deposit_amount());
        Assert.assertEquals(10000, profileRepository.getUserProfile
                (userNumber).getLargest_withdrawal_amount());
        Assert.assertEquals(9000, profileRepository.getUserProfile
                (userNumber).getLargest_transfer_amount());

        profileRepository.createAccountProfile(
                new OpeningAccount(1, System.currentTimeMillis(), 1));
        Assert.assertNull(profileRepository.getAccountProfile(1, 1));

        Thread.sleep(1000);
        Assert.assertEquals(finalAmount.longValue(),
                accountHistoryAmount.longValue());
    }

}