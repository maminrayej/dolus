package manager.transaction;

import manager.lock.Lock;

public class Transaction {


    private int transactionId;

    public Transaction(int transactionId)
    {
        this.transactionId = transactionId;
    }

    public int getTransactionId(){
        return this.transactionId;
    }

    public void lockIsGranted(Lock lock) {

    }
}
