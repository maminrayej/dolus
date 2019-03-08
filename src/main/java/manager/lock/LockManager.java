package manager.lock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import common.Log;
import manager.lock.LockConstants.LockLevels;
import manager.lock.LockConstants.LockTypes;
import manager.transaction.Transaction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class manages locks. and provides lock() and releaseLock() interface.
 * also uses a waits-for graph to detect dead locks.
 *
 * @author m.amin.rayej
 * @version 1.0
 * @since 1.0
 */
public class LockManager {

    /**
     * Component name to use in logging system
     */
    private static final String componentName = "LockManager";

    /**
     * A Map between database name -> database element.
     * this is the root pointer in lock tree
     */
    private HashMap<String, LockTreeDatabaseElement> lockTree;

    /**
     * Mapping between a transaction and its acquired locks
     */
    private HashMap<String, AcquiredLockTree> acquiredLockTreeMap;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockManager() {

        lockTree = new HashMap<>();

        acquiredLockTreeMap = new HashMap<>();
    }

    public static void main(String[] args) throws InterruptedException {

        LockManager lockManager = new LockManager();

        Transaction transaction1 = new Transaction("1");

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                lockManager.lock(transaction1, new Lock("database1", LockTypes.UPDATE));

                try{
                    Thread.sleep(1000);
                    lockManager.unlock(transaction1);
                }
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        thread1.start();
        thread1.join();

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(lockManager));

    }

    /**
     * Interface for transactions to acquire locks
     *
     * @param transaction transaction that requested the lock
     * @param lock        lock that transaction wants to acquire
     * @return true if lock request is granted, false otherwise
     * @since 1.0
     */
    public synchronized boolean lock(Transaction transaction, Lock lock) {

        //manage acquired lock tree for this transaction
        String transactionId = transaction.getTransactionId();

        //get acquired lock tree registered for this transaction
        AcquiredLockTree acquiredLockTree = acquiredLockTreeMap.get(transactionId);

        //if there is no acquired lock tree registered for this transaction -> register one!
        if (acquiredLockTree == null)
            acquiredLockTreeMap.put(transactionId, new AcquiredLockTree());

        //get name of the database to be locked
        String database = lock.getDatabase();

        //get name of the table to be lock
        String table = lock.getTable();

        //get id of the record to be locked
        Integer record = lock.getRecord();

        //determine the lock level
        int lockLevel = getLockLevel(lock);
        if (lockLevel == LockLevels.NOT_VALID_LEVEL) {
            Log.log(String.format("Lock requested by transaction: %s is not a valid request", transaction.getTransactionId()), componentName, Log.ERROR);
            return false;
        }

        //shows if requested lock is granted immediately or not
        boolean granted = false;

        //according to the lock level, call its appropriate manager
        if (lockLevel == LockLevels.DATABASE_LOCK) {

            granted = manageDatabaseLevelLock(transaction, lock, lock, database);

        } else if (lockLevel == LockLevels.TABLE_LOCK) {

            granted = manageTableLevelLock(transaction, lock, lock, database, table);

        } else if (lockLevel == LockLevels.RECORD_LOCK) {

            granted = manageRecordLevelLock(transaction, lock, database, table, record);
        }

        return granted;
    }

    /**
     * Determines the lock level
     *
     * @param lock lock object
     * @return level of the lock
     * @since 1.0
     */
    private int getLockLevel(Lock lock) {

        String database = lock.getDatabase();
        String table = lock.getTable();
        Integer record = lock.getRecord();

        //if all elements are null -> requested level is not valid
        if (record == null && table == null && database == null)
            return LockLevels.NOT_VALID_LEVEL;
        else if (record == null && table == null)//if only database element is defined
            return LockLevels.DATABASE_LOCK;
        else if (record == null)//if only table and database element is defined
            return LockLevels.TABLE_LOCK;
        else//if all elements are defined
            return LockLevels.RECORD_LOCK;

    }

    /**
     * Each element in lock tree can be locked if an appropriate lock has been acquired on its parent
     * this method return the appropriate lock type for the parent based on the type of the requested lock
     *
     * @param lock request lock
     * @return appropriate lock type for the parent
     * @since 1.0
     */
    private int getAppropriateParentLockType(Lock lock) {

        int lockType = lock.getType();

        if (lockType == LockTypes.SHARED || lockType == LockTypes.INTENT_SHARED)
            return LockTypes.INTENT_SHARED;
        else if (lockType == LockTypes.UPDATE || lockType == LockTypes.EXCLUSIVE || lockType == LockTypes.INTENT_EXCLUSIVE)
            return LockTypes.INTENT_EXCLUSIVE;

        return LockTypes.INTENT_SHARED;
    }

    /**
     * Manages a database level lock request
     *
     * @param transaction  transaction that requested the lock
     * @param originalLock original requested lock by transaction
     * @param appliedLock  lock to be applied to database element
     * @param databaseName name of the database to be locked
     * @return true if request is granted and false otherwise
     * @since 1.0
     */
    private boolean manageDatabaseLevelLock(Transaction transaction, Lock originalLock, Lock appliedLock, String databaseName) {

        //get database element with name specified by database variable
        LockTreeDatabaseElement databaseElement = this.lockTree.get(databaseName);

        //if this is a new node in tree -> no lock has been acquired on this database yet
        if (databaseElement == null) {

            //create a new database element
            databaseElement = new LockTreeDatabaseElement();

            //add this new database element to lock tree
            lockTree.put(databaseName, databaseElement);

            //add transaction to waiting queue of this element
            databaseElement.acquireLock(transaction, originalLock, appliedLock);

            //get transaction Id
            String transactionId = transaction.getTransactionId();

            //add this database element to acquired lock tree of the transaction
            this.acquiredLockTreeMap.get(transactionId).addDatabaseLock(databaseName, databaseElement);

            //lock is granted
            return true;
        }

        boolean granted = databaseElement.acquireLock(transaction, originalLock, appliedLock);

        if (granted) {
            //get transaction Id
            String transactionId = transaction.getTransactionId();

            //add this database element to acquired lock tree of the transaction
            this.acquiredLockTreeMap.get(transactionId).addDatabaseLock(databaseName, databaseElement);

            //lock is granted
            return true;
        } else
            return false;//lock is not granted
    }

    /**
     * Manages a table level lock request
     *
     * @param transaction  transaction that requested the lock
     * @param originalLock original requested lock by transaction
     * @param appliedLock  lock to be applied to table element
     * @param databaseName name of the database to be locked
     * @param tableName    name of the table to be locked
     * @return true if request is granted and false otherwise
     * @since 1.0
     */
    private boolean manageTableLevelLock(Transaction transaction, Lock originalLock, Lock appliedLock, String databaseName, String tableName) {

        //get appropriate lock type for database that contains the tableName
        int parentLockType = getAppropriateParentLockType(appliedLock);

        //create an applied lock for database element
        Lock databaseAppliedLock = new Lock(databaseName, parentLockType);

        //first try to lock the database with appropriate lock
        boolean databaseLevelGranted = manageDatabaseLevelLock(transaction, originalLock, databaseAppliedLock, databaseName);

        if (!databaseLevelGranted) {
            return false;
        }

        //get database element that contains the tableName
        LockTreeDatabaseElement databaseElement = lockTree.get(databaseName);

        //get table element
        LockTreeTableElement tableElement = databaseElement.getTableElement(tableName);

        //if there is no lock on requested table -> create an element for that and lock it
        if (tableElement == null) {

            //create a new table element
            tableElement = new LockTreeTableElement();

            //put this table element in database element that contains it
            databaseElement.putTableElement(tableName, tableElement);

            //add transaction to the queue
            tableElement.acquireLock(transaction, originalLock, appliedLock);

            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree registered for this transaction
            this.acquiredLockTreeMap.get(transactionId).addTableLock(databaseName, tableName, tableElement);

            //lock is granted
            return true;
        }

        boolean granted = tableElement.acquireLock(transaction, originalLock, appliedLock);

        if (granted) {
            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree registered for this transaction
            this.acquiredLockTreeMap.get(transactionId).addTableLock(databaseName, tableName, tableElement);

            //lock is granted
            return true;
        } else
            return false;//lock is not granted
    }

    /**
     * Manages a record level locking
     *
     * @param transaction  transaction that requested the lock
     * @param lock         requested lock by transaction
     * @param databaseName name of the database to be locked
     * @param tableName    name of the table to be locked
     * @param recordId     id of the record to be locked
     * @return true if request is granted and false otherwise
     */
    private boolean manageRecordLevelLock(Transaction transaction, Lock lock, String databaseName, String tableName, Integer recordId) {

        //get database element that contains the table which contains requested record
        LockTreeDatabaseElement databaseElement = lockTree.get(databaseName);

        //get table element that contains requested record
        LockTreeTableElement tableElement = databaseElement.getTableElement(tableName);

        //get record element with requested recordId
        LockTreeElement recordElement = tableElement.getRecordElement(recordId);

        //if there is no lock on this record
        if (recordElement == null) {

            //create a new record element
            recordElement = new LockTreeElement();

            //add created record element to its table
            tableElement.putRecordElement(recordId, recordElement);

            //add transaction to queue
            recordElement.acquireLock(transaction, lock, lock);

            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree for this transaction and add this record to its tree
            acquiredLockTreeMap.get(transactionId).addRecordLock(tableName, recordElement);

            //lock is granted
            return true;
        }

        boolean granted = recordElement.acquireLock(transaction, lock, lock);

        if (granted) {
            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree for this transaction and add this record to its tree
            acquiredLockTreeMap.get(transactionId).addRecordLock(tableName, recordElement);

            return true;
        } else
            return false;
    }

    /**
     * An interface for transactions to release their lock
     *
     * @param transaction transaction object that wants to release its locks
     * @since 1.0
     */
    public synchronized void unlock(Transaction transaction) {

        //create shared queues
        Queue<QueueElement> firstQueue = new LinkedList<>();
        Queue<QueueElement> secondQueue = new LinkedList<>();

        //create locks for queues
        ReentrantLock firstQueueLock = new ReentrantLock();
        ReentrantLock secondQueueLock = new ReentrantLock();

        //create an instance of the callback runnable
        CallBackRunnable callBackRunnable = new CallBackRunnable(firstQueue, secondQueue, firstQueueLock, secondQueueLock);

        //assign a thread to the call back runnable
        Thread callBackThread = new Thread(callBackRunnable);

        //start the call back thread
        callBackThread.start();

        //get transaction id
        String transactionId = transaction.getTransactionId();

        //get acquired lock tree registered for this transaction id
        AcquiredLockTree acquiredLockTree = acquiredLockTreeMap.get(transactionId);

        //get the tree of locks acquired by this transaction
        LinkedList<AcquiredLockTreeElement> databases = acquiredLockTree.getAcquiredLockTree();

        //visit children first and then visit their parents
        //so for every parent to be unlocked, each child of that parent must be unlocked first(multi granularity policy)
        for (int i = 0; i < databases.size(); i++) {

            //get an acquired database element from head of the database list
            AcquiredLockTreeElement acquiredDatabaseElement = databases.removeFirst();

            //first all tables of the database must be unlocked in order for the database to be unlocked
            //so get all locked tables of the database element
            LinkedList<AcquiredLockTreeElement> tables = acquiredDatabaseElement.getChildren();

            //loop through tables and unlock each one
            for (int j = 0; j < tables.size(); j++) {

                //get an acquired table element from head of the table list
                AcquiredLockTreeElement acquiredTableElement = tables.removeFirst();

                //first all record of the table must be unlocked in order for the table to be unlocked
                //so get all lock records of the table element
                LinkedList<AcquiredLockTreeElement> records = acquiredTableElement.getChildren();

                for (int k = 0; k < records.size(); k++) {

                    //get an acquired record element from head of the record list
                    AcquiredLockTreeElement acquiredRecordElement = records.removeFirst();

                    //each acquired element contains an element from the lock tree
                    //get that lock tree element inside of the acquired element
                    LockTreeElement lockTreeElement = acquiredRecordElement.getLockTreeElement();

                    //release the lock held by the transaction and get list of new granted transactions
                    LinkedList<QueueElement> grantedTransactions = lockTreeElement.releaseLock(transactionId);

                    //add granted transactions to the shared memory with call back thread
                    //call back thread will inform each transaction of its granted lock
                    //try to lock the first queue
                    if (firstQueueLock.tryLock()) {

                        //add all transactions to the first queue
                        firstQueue.addAll(grantedTransactions);

                        //unlock the first queue
                        firstQueueLock.unlock();
                    }
                    else if (secondQueueLock.tryLock()) {

                        //add all transactions to the second queue
                        secondQueue.addAll(grantedTransactions);

                        //unlock the second queue
                        secondQueueLock.unlock();
                    }
                }

                //now that every lock held on records of table element by the transaction is released,
                //we can release the lock on table itself
                LinkedList<QueueElement> grantedTransactions = acquiredTableElement.getLockTreeElement().releaseLock(transactionId);

                //add granted transactions to the shared memory with call back thread
                //call back thread will inform each transaction of its granted lock
                //try to lock the first queue
                if (firstQueueLock.tryLock()) {

                    //add all transactions to the first queue
                    firstQueue.addAll(grantedTransactions);

                    //unlock the first queue
                    firstQueueLock.unlock();
                }
                else if (secondQueueLock.tryLock()) {

                    //add all transactions to the second queue
                    secondQueue.addAll(grantedTransactions);

                    //unlock the second queue
                    secondQueueLock.unlock();
                }
            }

            //now that every lock held on tables of database element by the transaction is released,
            //we can release the lock on database itself
            acquiredDatabaseElement.getLockTreeElement().releaseLock(transactionId);
        }

        callBackRunnable.exit();

        //wait for the call back thread to end
        try {
            callBackThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
