package manager.lock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import common.Log;
import manager.lock.LockConstants.LockLevels;
import manager.lock.LockConstants.LockTypes;
import manager.transaction.Transaction;

import javax.swing.text.html.ObjectView;
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
     * Mapping between a transaction and its requested locks
     */
    private HashMap<String, RequestedLockTree> requestedLockTreeMap;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockManager() {

        lockTree = new HashMap<>();

        requestedLockTreeMap = new HashMap<>();
    }

    public static void main(String[] args) throws InterruptedException {

        //TODO implement degrade method
        //TODO implement cycle detection using jgrapht
        //TODO delete unused lock tree elements

        LockManager lockManager = new LockManager();

        Transaction transaction1 = new Transaction("1");
        Transaction transaction2 = new Transaction("2");

        Thread thread1 = new Thread(() -> {
            lockManager.lock(transaction1, new Lock("database1", "table1", LockTypes.EXCLUSIVE));


            lockManager.lock(transaction2, new Lock("database1", "table1", LockTypes.UPDATE));

            lockManager.degradeLock(transaction1, new Lock("database1", "table1", LockTypes.EXCLUSIVE), LockTypes.UPDATE);

            lockManager.unlock(transaction1);

            lockManager.unlock(transaction2);
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

        //manage requested lock tree for this transaction
        String transactionId = transaction.getTransactionId();

        //get requested lock tree registered for this transaction
        RequestedLockTree requestedLockTree = requestedLockTreeMap.get(transactionId);

        //if there is no requested lock tree registered for this transaction -> register one!
        if (requestedLockTree == null)
            requestedLockTreeMap.put(transactionId, new RequestedLockTree());

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
     * Each element in lock tree can be locked if an appropriate lock has been requested on its parent
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

        //if this is a new node in tree -> no lock has been requested on this database yet
        if (databaseElement == null) {

            //create a new database element
            databaseElement = new LockTreeDatabaseElement(databaseName);

            //add this new database element to lock tree
            lockTree.put(databaseName, databaseElement);

            //add transaction to waiting queue of this element
            databaseElement.acquireLock(transaction, originalLock, appliedLock);

            //get transaction Id
            String transactionId = transaction.getTransactionId();

            //add this database element to requested lock tree of the transaction
            this.requestedLockTreeMap.get(transactionId).addDatabaseLock(databaseName, databaseElement);

            //lock is granted
            return true;
        }

        boolean granted = databaseElement.acquireLock(transaction, originalLock, appliedLock);

        String transactionId = transaction.getTransactionId();

        this.requestedLockTreeMap.get(transactionId).addDatabaseLock(databaseName, databaseElement);

        return granted;
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
        boolean databaseLevelGranted = manageDatabaseLevelLock(transaction, databaseAppliedLock, originalLock, databaseName);

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
            tableElement = new LockTreeTableElement(tableName);

            //put this table element in database element that contains it
            databaseElement.putTableElement(tableName, tableElement);

            //add transaction to the queue
            tableElement.acquireLock(transaction, originalLock, appliedLock);

            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get requested lock tree registered for this transaction
            this.requestedLockTreeMap.get(transactionId).addTableLock(databaseName, tableName, tableElement);

            //lock is granted
            return true;
        }

        boolean granted = tableElement.acquireLock(transaction, originalLock, appliedLock);

        String transactionId = transaction.getTransactionId();

        this.requestedLockTreeMap.get(transactionId).addTableLock(databaseName, tableName, tableElement);

        return granted;
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
            recordElement = new LockTreeElement(recordId+"");

            //add created record element to its table
            tableElement.putRecordElement(recordId, recordElement);

            //add transaction to queue
            recordElement.acquireLock(transaction, lock, lock);

            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get requested lock tree for this transaction and add this record to its tree
            requestedLockTreeMap.get(transactionId).addRecordLock(tableName, recordElement);

            //lock is granted
            return true;
        }

        boolean granted = recordElement.acquireLock(transaction, lock, lock);

        String transactionId = transaction.getTransactionId();

        this.requestedLockTreeMap.get(transactionId).addRecordLock(tableName, recordElement);

        return granted;
    }

    /**
     * An interface for transactions to release their lock
     *
     * @param transaction transaction object that wants to release its locks
     * @since 1.0
     */
    public synchronized void unlock(Transaction transaction) {

        //create shared queues
        Queue<LockRequest> firstQueue = new LinkedList<>();
        Queue<LockRequest> secondQueue = new LinkedList<>();

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

        //get requested lock tree registered for this transaction id
        RequestedLockTree requestedLockTree = requestedLockTreeMap.get(transactionId);

        //get the tree of locks requested by this transaction
        LinkedList<RequestedLockTreeElement> databases = requestedLockTree.getRequestedLockTree();

        int databaseListSize = databases.size();

        //visit children first and then visit their parents
        //so for every parent to be unlocked, each child of that parent must be unlocked first(multi granularity policy)
        for (int i = 0; i < databaseListSize; i++) {

            //get an requested database element from head of the database list
            RequestedLockTreeElement requestedDatabaseElement = databases.removeFirst();

            //first all tables of the database must be unlocked in order for the database to be unlocked
            //so get all locked tables of the database element
            LinkedList<RequestedLockTreeElement> tables = requestedDatabaseElement.getChildren();

            int tablesListSize = tables.size();

            //loop through tables and unlock each one
            for (int j = 0; j < tablesListSize; j++) {

                //get an requested table element from head of the table list
                RequestedLockTreeElement requestedTableElement = tables.removeFirst();

                //first all record of the table must be unlocked in order for the table to be unlocked
                //so get all lock records of the table element
                LinkedList<RequestedLockTreeElement> records = requestedTableElement.getChildren();

                int recordsListSize = records.size();

                for (int k = 0; k < recordsListSize; k++) {

                    //get an requested record element from head of the record list
                    RequestedLockTreeElement requestedRecordElement = records.removeFirst();

                    //each requested element contains an element from the lock tree
                    //get that lock tree element inside of the requested element
                    LockTreeElement lockTreeElement = requestedRecordElement.getLockTreeElement();

                    //release the lock held by the transaction and get list of new granted transactions
                    LinkedList<LockRequest> grantedRequests = lockTreeElement.releaseLock(transactionId);

                    //if granted requests is null -> it means there can not be any granted requests
                    //because both granted and waiting list of the element is empty
                    //remove the element from lock tree
                    if (grantedRequests == null) {
                        LockTreeTableElement tableElement = (LockTreeTableElement) requestedTableElement.getLockTreeElement();

                        LockTreeElement recordElement = requestedRecordElement.getLockTreeElement();

                        tableElement.removeRecordElement(recordElement.getName());
                    }

                    //add granted transactions to shared queues so call back thread can inform transactions of their granted locks
                    addToQueue(firstQueueLock, secondQueueLock, firstQueue, secondQueue, grantedRequests);
                }

                //now that every lock held on records of table element by the transaction is released,
                //we can release the lock on table itself
                LinkedList<LockRequest> grantedRequests = requestedTableElement.getLockTreeElement().releaseLock(transactionId);

                //if granted requests is null -> it means there can not be any granted requests
                //because both granted and waiting list of the element is empty
                //remove the element from lock tree
                if (grantedRequests == null) {
                    LockTreeDatabaseElement databaseElement = (LockTreeDatabaseElement) requestedDatabaseElement.getLockTreeElement();

                    LockTreeElement tableElement = requestedTableElement.getLockTreeElement();

                    databaseElement.removeTableElement(tableElement.getName());
                }

                //add granted transactions to shared queues so call back thread can inform transactions of their granted locks
                addToQueue(firstQueueLock, secondQueueLock, firstQueue, secondQueue, grantedRequests);
            }

            //now that every lock held on tables of database element by the transaction is released,
            //we can release the lock on database itself
            LinkedList<LockRequest> grantedRequests = requestedDatabaseElement.getLockTreeElement().releaseLock(transactionId);

            //if granted requests is null -> it means there can not be any granted requests
            //because both granted and waiting list of the element is empty
            //remove the element from lock tree
            if (grantedRequests == null) {

                LockTreeElement databaseElement = requestedDatabaseElement.getLockTreeElement();

                lockTree.remove(databaseElement.getName());

            }

            //add granted transactions to shared queues so call back thread can inform transactions of their granted locks
            addToQueue(firstQueueLock, secondQueueLock, firstQueue, secondQueue, grantedRequests);
        }

        callBackRunnable.exit();

        //delete requested tree lock tree registered for transaction
        this.requestedLockTreeMap.remove(transactionId);

        //wait for the call back thread to end
        try {
            callBackThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Degrades a lock type into a less strict one
     *
     * @param transaction transaction that requested the degrading
     * @param lock        original lock
     * @param lockType    lock type the transaction wants its request to be degraded to
     */
    public synchronized void degradeLock(Transaction transaction, Lock lock, int lockType) {

        //get database name
        String databaseName = lock.getDatabase();

        //get table name
        String tableName = lock.getTable();

        //get requested lock tree created for this transaction
        RequestedLockTree requestedLockTree = requestedLockTreeMap.get(transaction.getTransactionId());

        //find the element in requested lock tree that corresponds to database name
        //retrieve the pointer to database element in lock tree map
        LockTreeDatabaseElement databaseElement = (LockTreeDatabaseElement) requestedLockTree.getRequestedDatabaseElement(databaseName).getLockTreeElement();

        //list of new granted lock requests after degrading the lock
        LinkedList<LockRequest> grantedLockRequests;

        //degrade database element
        if (databaseName != null && tableName == null) {

            //degrade its lock type
            grantedLockRequests = databaseElement.degradeLock(transaction, lockType);
        } else { //degrade table element

            //get table element specified by table name
            LockTreeElement tableElement = databaseElement.getTableElement(tableName);

            //degrade its lock
            grantedLockRequests = tableElement.degradeLock(transaction, lockType);
        }

        //create shared queues
        Queue<LockRequest> firstQueue = new LinkedList<>();
        Queue<LockRequest> secondQueue = new LinkedList<>();

        //create locks for queues
        ReentrantLock firstQueueLock = new ReentrantLock();
        ReentrantLock secondQueueLock = new ReentrantLock();

        //create an instance of the callback runnable
        CallBackRunnable callBackRunnable = new CallBackRunnable(firstQueue, secondQueue, firstQueueLock, secondQueueLock);

        //assign a thread to the call back runnable
        Thread callBackThread = new Thread(callBackRunnable);

        //start the call back thread
        callBackThread.start();

        addToQueue(firstQueueLock, secondQueueLock, firstQueue, secondQueue, grantedLockRequests);

        callBackRunnable.exit();

    }

    /**
     * Adds granted transactions to the first available queue
     *
     * @param firstQueueLock  lock on first queue
     * @param secondQueueLock lock on second queue
     * @param firstQueue      first queue
     * @param secondQueue     second queue
     * @param grantedRequests list of granted transactions
     */
    private void addToQueue(ReentrantLock firstQueueLock, ReentrantLock secondQueueLock,
                            Queue<LockRequest> firstQueue, Queue<LockRequest> secondQueue,
                            LinkedList<LockRequest> grantedRequests) {

        //if there is no granted transaction then there is no element to add! -> return
        if (grantedRequests == null)
            return;

        //add granted transactions to the shared memory with call back thread
        //call back thread will inform each transaction of its granted lock
        //try to lock the first queue
        if (firstQueueLock.tryLock()) {

            //add all transactions to the first queue
            firstQueue.addAll(grantedRequests);

            //unlock the first queue
            firstQueueLock.unlock();
        } else if (secondQueueLock.tryLock()) {

            //add all transactions to the second queue
            secondQueue.addAll(grantedRequests);

            //unlock the second queue
            secondQueueLock.unlock();
        }
    }

}
