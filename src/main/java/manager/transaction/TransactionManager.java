package manager.transaction;

import config.ConfigUtilities;
import manager.lock.LockManager;

import java.util.HashMap;

public class TransactionManager {

    private int transactionId = 0;

    private HashMap<String, Transaction> transactionMap;

    private LockManager lockManager;

    public TransactionManager(LockManager lockManager) {
        transactionMap = new HashMap<>();
        this.lockManager = lockManager;
    }
    public synchronized void executeQuery(String query) {

        transactionId++;

        Transaction transaction = new Transaction(query, transactionId+"", lockManager);

        transactionMap.put(transactionId+"", transaction);

        new Thread(transaction).start();
    }

    public static void main(String[] args) {

        ConfigUtilities.loadMainConfig("/home/amin/programming/projects/dolus/dolus-config.json");
        ConfigUtilities.loadStorageConfig();

        LockManager lockManager = new LockManager();
        TransactionManager transactionManager = new TransactionManager(lockManager);

//        for (int i = 2; i < 100; i++)
//            transactionManager.executeQuery(String.format("INSERT INTO SAILORS(SID, SNAME, PHONE) VALUES(%d, \"amin%d\", \"09111063006\")",i,i));

        transactionManager.executeQuery("SELECT S.SID, S.SNAME, S.PHONE FROM SAILORS S");
    }
}

//Insert : "INSERT INTO SAILORS(SID, SNAME, PHONE) VALUES(2, \"amin2\", \"09111063006\")"
// select: "SELECT S.SID, S.SNAME, S.PHONE FROM SAILORS S"