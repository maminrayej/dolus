package manager.transaction;

import manager.lock.Lock;

public class Transaction {


    private String transactionId;

    public Transaction(String transactionId)
    {
        this.transactionId = transactionId;
    }

    public String getTransactionId(){
        return this.transactionId;
    }

    public void lockIsGranted(Lock lock) {

    }
}
