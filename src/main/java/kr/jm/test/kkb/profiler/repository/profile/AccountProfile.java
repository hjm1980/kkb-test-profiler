package kr.jm.test.kkb.profiler.repository.profile;

import kr.jm.test.kkb.profiler.repository.ProfileRepository;
import kr.jm.test.kkb.transaction.log.AbstractAmountTransactionLog;
import kr.jm.test.kkb.transaction.log.TransactionType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@ToString(callSuper = true)
public class AccountProfile extends AbstractProfile {
    private int account_number;
    private String create_dt;
    private Long balance;
    private List<SimpleAccountHistory> deposits;
    private List<SimpleAccountHistory> withdrawals;
    private List<SimpleAccountHistory> transfers;

    public AccountProfile(int customer_number, int account_number,
            String create_dt) {
        super(customer_number);
        this.account_number = account_number;
        this.create_dt = create_dt;
        this.balance = 0L;
        this.deposits = Collections.synchronizedList(new ArrayList<>());
        this.withdrawals = Collections.synchronizedList(new ArrayList<>());
        this.transfers = Collections.synchronizedList(new ArrayList<>());
    }

    public AccountHistory addHistoryTransactionLog(
            AbstractAmountTransactionLog amountTransactionLog) {
        return addHistoryTransactionLog(
                amountTransactionLog.getTransactionType(),
                amountTransactionLog.getAmount(), amountTransactionLog);
    }

    private AccountHistory addHistoryTransactionLog(
            TransactionType transactionType, long amount,
            AbstractAmountTransactionLog amountTransactionLog) {
        calBalance(transactionType, amount);
        AccountHistory accountHistory =
                new AccountHistory(amount, ProfileRepository
                        .buildDataTime(amountTransactionLog.getLogTimestamp()),
                        amountTransactionLog.getAccountNumber(),
                        amountTransactionLog.getAccountNumber(),
                        transactionType);
        Objects.requireNonNull(getAccountHistoryList(transactionType))
                .add(new SimpleAccountHistory(accountHistory.getAmount(),
                        accountHistory.getDatetime()));
        return accountHistory;
    }

    private void calBalance(TransactionType transactionType,
            long amount) {
        synchronized (balance) {
            switch (transactionType) {
                case DEPOSIT:
                    balance += amount;
                    break;
                case WITHDRAW:
                case TRANSFER:
                    balance -= amount;
                    break;
            }
        }
    }

    private List<SimpleAccountHistory> getAccountHistoryList(
            TransactionType transactionType) {
        switch (transactionType) {
            case DEPOSIT:
                return deposits;
            case WITHDRAW:
                return withdrawals;
            case TRANSFER:
                return transfers;
            default:
                return null;
        }
    }

}
