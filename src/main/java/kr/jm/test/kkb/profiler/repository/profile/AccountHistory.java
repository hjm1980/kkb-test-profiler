package kr.jm.test.kkb.profiler.repository.profile;

import kr.jm.test.kkb.transaction.log.TransactionType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@ToString(callSuper = true)
@Getter
public class AccountHistory extends SimpleAccountHistory {
    private int customerNumber;
    private int accountNumber;
    private TransactionType transactionType;

    public AccountHistory(long amount, String datetime, int customerNumber,
            int accountNumber, TransactionType transactionType) {
        super(amount, datetime);
        this.customerNumber = customerNumber;
        this.accountNumber = accountNumber;
        this.transactionType = transactionType;
    }
}
