package kr.jm.test.kkb.profiler.repository.profile;

import kr.jm.test.kkb.transaction.log.TransactionType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@ToString(callSuper = true)
public class CustomerProfile extends AbstractProfile {
    private String name;
    private String join_dt;
    private volatile long largest_deposit_amount;
    private volatile long largest_withdrawal_amount;
    private volatile long largest_transfer_amount;

    public CustomerProfile(int customer_number, String name,
            String join_dt) {
        super(customer_number);
        this.name = name;
        this.join_dt = join_dt;
        this.largest_deposit_amount = 0L;
        this.largest_withdrawal_amount = 0L;
        this.largest_transfer_amount = 0L;
    }

    public void setLargestAccount(TransactionType transactionType,
            long amount) {
        switch (transactionType) {
            case DEPOSIT:
                setLargestDepositAccount(amount);
                break;
            case WITHDRAW:
                setLargestWithdrawalAccount(amount);
                break;
            case TRANSFER:
                setLargestTransferAccount(amount);
                break;
        }
    }

    private void setLargestDepositAccount(long amount) {
        if (largest_deposit_amount < amount)
            largest_deposit_amount = amount;
    }

    private void setLargestWithdrawalAccount(long amount) {
        if (largest_withdrawal_amount < amount)
            largest_withdrawal_amount = amount;
    }

    private void setLargestTransferAccount(long amount) {
        if (largest_transfer_amount < amount)
            largest_transfer_amount = amount;
    }

}
