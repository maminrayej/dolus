package manager.transaction;

import manager.lock.Lock;
import manager.lock.LockManager;

import java.util.HashMap;

public class Transaction implements Runnable{

    private String transactionId;
    private String query;

    private LockManager lockManager;

    private HashMap<String,QueryExecutor> lockExecutorMap;

    private final int NOT_VALID = 0;
    private final int SELECT = 1;
    private final int UPDATE = 2;
    private final int DELETE = 3;
    private final int INSERT = 4;

    public Transaction(String query, String transactionId, LockManager lockManager)
    {
        this.transactionId = transactionId;
        this.query = query;
        this.lockManager = lockManager;
        this.lockExecutorMap = new HashMap<>();
    }

    public String getTransactionId(){
        return this.transactionId;
    }

    public void lockIsGranted(Lock lock) {
        System.out.println(String.format("Transaction: %s granted with -> (%s,%s,%s)", this.transactionId, lock.getDatabase(), lock.getTable(), lock.getRecord()));

        lockExecutorMap.get(lock.toString()).lockIsGranted(lock);

    }

    public void submitLock(Lock lock, QueryExecutor queryExecutor) {
        System.out.println("Lock: " + lock.toString() + " is submitted");
        boolean granted = lockManager.lock(this, lock);
        if (granted)
            queryExecutor.lockIsGranted(lock);
        else
            lockExecutorMap.put(lock.toString(), queryExecutor);
    }

    public void releaseLock() {
        lockManager.unlock(this);
    }

    @Override
    public void run() {
        int queryType = getQueryType(this.query);

        if (queryType == SELECT){
            new Thread(
                    new SelectExecutorRunnable(this.query, this)
                    ).start();
        }
        else if (queryType == UPDATE)
            ;
        else if (queryType == INSERT)
            new Thread(
                    new InsertExecutorRunnable(this.query, this)
            ).start();
        else if (queryType == DELETE)
            ;
        else {

        }

    }

    private int getQueryType(String query) {
        if (query.toLowerCase().contains("select"))
            return SELECT;
        else if (query.toLowerCase().contains("update"))
            return UPDATE;
        else if (query.toLowerCase().contains("insert"))
            return INSERT;
        else if (query.toLowerCase().contains("delete"))
            return DELETE;

        return NOT_VALID;
    }

}
