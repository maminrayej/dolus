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
        System.out.println(String.format("Transaction: %s granted with -> (%s,%s,%s)", this.transactionId, lock.getDatabase(), lock.getTable(), lock.getRecord()));
    }
}
