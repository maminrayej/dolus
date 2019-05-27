package manager.transaction;

import manager.lock.Lock;

import java.util.HashMap;

public abstract class QueryExecutor implements Runnable{

    private HashMap<String,Boolean> lockRequests;

    private Transaction transaction;

    private boolean allGranted;

    private boolean die;

    public QueryExecutor(Transaction transaction) {
        this.transaction = transaction;
        lockRequests = new HashMap<>();
    }

    public void lockIsGranted(Lock lock) {
        System.out.println("Lock: " + lock.toString() + " is granted");
        lockRequests.put(lock.toString(), true);

        allGranted = checkGranted();
        System.out.println(allGranted);
    }

    protected void submitLock(Lock lock) {
        lockRequests.put(lock.toString(), false);
        transaction.submitLock(lock, this);
    }

    protected boolean checkGranted() {
        Object[] lockNames = lockRequests.keySet().toArray();

        for (int i = 0; i < lockNames.length; i++) {
            if (!lockRequests.get(lockNames[i]))
                return false;
        }
        return true;
    }

    public boolean isAllGranted() {
        return allGranted;
    }

    public void die() {
        this.die = true;
    }

    public void releaseLock() {
        this.transaction.releaseLock();
    }

    protected void waitOrDie() {

        while (true) {

            try {
                if (allGranted || die) {
                    System.out.println(allGranted);
                    return;
                }
                else
                    Thread.sleep(500);
            }
            catch(InterruptedException e) {
                System.out.println("QueryExecutor is interrupted");
            }
        }
    }
}
