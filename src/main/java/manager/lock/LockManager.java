package manager.lock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import common.Log;
import manager.lock.LockConstants.LockLevels;
import manager.lock.LockConstants.LockTypes;
import manager.transaction.Transaction;

import java.util.HashMap;

/**
 * This class manages locks. and provides lock() and unlock() interface.
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
        Transaction transaction2 = new Transaction("2");
        Transaction transaction3 = new Transaction("3");
        Transaction transaction4 = new Transaction("4");



        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                lockManager.lock(transaction1, new Lock("database1", "table1", LockTypes.SHARED));
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                lockManager.lock(transaction2, new Lock("database1" , "table1", LockTypes.EXCLUSIVE));
            }
        });
        Thread thread3 = new Thread(new Runnable() {
            @Override
            public void run() {
                lockManager.lock(transaction3, new Lock("database2", "table2", LockTypes.UPDATE));
            }
        });
        Thread thread4 = new Thread(new Runnable() {
            @Override
            public void run() {
                lockManager.lock(transaction4, new Lock("database2", "table2", LockTypes.UPDATE));
            }
        });
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();

        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();

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

        //according to the lock level call its appropriate manager
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
     * @param transaction transaction that requested the lock
     * @param originalLock original requested lock by transaction
     * @param appliedLock lock to be applied to database element
     * @param databaseName name of the database to be locked
     * @return true if request is granted and false otherwise
     * @since 1.0
     */
    private boolean manageDatabaseLevelLock(Transaction transaction, Lock originalLock, Lock appliedLock, String databaseName) {

        //get type of the lock
        int lockType = appliedLock.getType();

        //get database element with name specified by database variable
        LockTreeDatabaseElement databaseElement = this.lockTree.get(databaseName);

        //if this is a new node in tree -> no lock has been acquired on this database yet
        if (databaseElement == null) {

            //create a new database element
            databaseElement = new LockTreeDatabaseElement();

            //add this new database element to lock tree
            lockTree.put(databaseName, databaseElement);

            //add transaction to waiting queue of this element
            databaseElement.addGranted(transaction, originalLock, appliedLock);

            //set current active lock type to requested type
            databaseElement.setCurrentActiveLockType(lockType);

            //get transaction Id
            String transactionId = transaction.getTransactionId();

            //add this database element to acquired lock tree of the transaction
            this.acquiredLockTreeMap.get(transactionId).addDatabaseLock(databaseName, databaseElement);

            //lock is granted
            return true;
        }

        boolean granted =  lockElement(transaction, originalLock, appliedLock, databaseElement);

        if (granted) {
            //get transaction Id
            String transactionId = transaction.getTransactionId();

            //add this database element to acquired lock tree of the transaction
            this.acquiredLockTreeMap.get(transactionId).addDatabaseLock(databaseName, databaseElement);

            //lock is granted
            return true;
        }
        else
            return false;//lock is not granted
    }

    /**
     * Manages a table level lock request
     *
     * @param transaction transaction that requested the lock
     * @param originalLock original requested lock by transaction
     * @param appliedLock lock to be applied to table element
     * @param databaseName name of the database to be locked
     * @param tableName name of the table to be locked
     * @return true if request is granted and false otherwise
      @since 1.0
     */
    private boolean manageTableLevelLock(Transaction transaction, Lock originalLock, Lock appliedLock, String databaseName, String tableName) {

        int lockType = appliedLock.getType();

        //get appropriate lock type for database that contains the tableName
        int parentLockType = getAppropriateParentLockType(appliedLock);

        //create an applied lock for database element
        Lock databaseAppliedLock   = new Lock(databaseName, parentLockType);

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
            tableElement.addGranted(transaction, originalLock, appliedLock);

            //set current active lock type of the element
            tableElement.setCurrentActiveLockType(lockType);

            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree registered for this transaction
            this.acquiredLockTreeMap.get(transactionId).addTableLock(databaseName, tableName, tableElement);

            //lock is granted
            return true;
        }

        boolean granted = lockElement(transaction, originalLock, appliedLock, tableElement);
        if (granted) {
            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree registered for this transaction
            this.acquiredLockTreeMap.get(transactionId).addTableLock(databaseName, tableName, tableElement);

            //lock is granted
            return true;
        }
        else
            return false;//lock is not granted
    }

    /**
     * Manages a record level locking
     *
     * @param transaction transaction that requested the lock
     * @param lock requested lock by transaction
     * @param databaseName name of the database to be locked
     * @param tableName name of the table to be locked
     * @param recordId id of the record to be locked
     * @return true if request is granted and false otherwise
     */
    private boolean manageRecordLevelLock(Transaction transaction, Lock lock, String databaseName, String tableName, Integer recordId) {
//
//        //get appropriate lock type for table that contains the recordId
//        int parentLockType = getAppropriateParentLockType(lock);
//
//        //create an applied lock for table element
//        Lock tableAppliedLock = new Lock(databaseName, tableName, parentLockType);
//
//        //first try to lock the table with appropriate lock
//        boolean tableLevelGranted = manageTableLevelLock(transaction, lock, tableAppliedLock, databaseName, tableName);
//
//        if (!tableLevelGranted) {
//            return false;
//        }

        //get database element that contains the table which contains requested record
        LockTreeDatabaseElement databaseElement = lockTree.get(databaseName);

        //get table element that contains requested record
        LockTreeTableElement tableElement = databaseElement.getTableElement(tableName);

        //get record element with requested recordId
        LockTreeElement recordElement = tableElement.getRecordElement(recordId);

        int lockType = lock.getType();

        //if there is no lock on this record
        if (recordElement == null) {

            //create a new record element
            recordElement = new LockTreeElement();

            //add created record element to its table
            tableElement.putRecordElement(recordId, recordElement);

            //add transaction to queue
            recordElement.addGranted(transaction, lock, lock);

            //set current active lock type
            recordElement.setCurrentActiveLockType(lockType);

            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree for this transaction and add this record to its tree
            acquiredLockTreeMap.get(transactionId).addRecordLock(tableName, recordElement);

            //lock is granted
            return true;
        }

        boolean granted = lockElement(transaction, lock, lock, recordElement);

        if (granted) {
            //get transaction id
            String transactionId = transaction.getTransactionId();

            //get acquired lock tree for this transaction and add this record to its tree
            acquiredLockTreeMap.get(transactionId).addRecordLock(tableName, recordElement);

            return true;
        }
        else
            return false;
    }

    /**
     * Locks an element in the tree lock
     *
     * @param transaction transaction that requested the lock
     * @param originalLock        lock to be acquired
     * @param element     element to be locked
     * @return true if transactions successfully acquired the lock, false otherwise
     * @since 1.0
     */
    private boolean lockElement(Transaction transaction, Lock originalLock, Lock appliedLock, LockTreeElement element) {

        //get the type of the lock
        int lockType = appliedLock.getType();

        //get current active lock type and use it to see whether requested lock
        //is compatible with current active lock type or not
        int currentActiveLockType = element.getCurrentActiveLockType();

        if (lockType == LockTypes.SHARED) {

            //SHARED lock is compatible with Shared, Exclusive, Update and Intent Shared locks
            //if lock is compatible with current active lock type
            if (currentActiveLockType == LockTypes.SHARED) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.UPDATE) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.INTENT_SHARED) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //Shared lock is more restrictive than Intent shared
                //so current active lock type on this element must change to Shared
                element.setCurrentActiveLockType(LockTypes.SHARED);

                //lock is granted
                return true;
            }

            //lock is incompatible with current active lock
            //add transaction to waiting queue of this element
            element.addWaiting(transaction, originalLock, appliedLock);

            //lock is not granted
            return false;

        } else if (lockType == LockTypes.EXCLUSIVE) {

            //Exclusive lock is not compatible with any lock
            //add transaction to waiting queue of this element
            element.addWaiting(transaction, originalLock, appliedLock);

            //lock is not granted
            return false;

        } else if (lockType == LockTypes.UPDATE) {

            //Update lock is compatible with Intent shared and Shared locks
            //if lock is compatible with current active lock type
            if (currentActiveLockType == LockTypes.INTENT_SHARED) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //Update lock is more restrictive so current active lock type must set to Update
                element.setCurrentActiveLockType(LockTypes.UPDATE);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.SHARED) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //Update lock is more restrictive so current active lock type must set to Update
                element.setCurrentActiveLockType(LockTypes.UPDATE);

                //lock is granted
                return true;
            }

            //lock is incompatible
            //add transaction to waiting queue of this element
            element.addWaiting(transaction, originalLock, appliedLock);

            //lock is not granted
            return false;

        } else if (lockType == LockTypes.INTENT_SHARED) {

            //Intent shared lock is only incompatible with Exclusive lock
            //if lock is incompatible with current active lock type
            if (currentActiveLockType == LockTypes.EXCLUSIVE) {

                //add transaction to waiting queue of this element
                element.addWaiting(transaction, originalLock, appliedLock);

                //lock is not granted
                return false;
            } else {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //lock is granted
                return true;
            }

        } else if (lockType == LockTypes.INTENT_EXCLUSIVE) {

            //Intent Exclusive lock is only compatible with Intent Shared and Intent Exclusive lock
            //if lock is compatible
            if (currentActiveLockType == LockTypes.INTENT_SHARED) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //Intent Exclusive is more restrictive than Intent Shared, so current active lock type must set to Intent Exclusive
                element.setCurrentActiveLockType(LockTypes.INTENT_EXCLUSIVE);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.INTENT_EXCLUSIVE) {

                //add transaction to waiting queue of this element
                element.addGranted(transaction, originalLock, appliedLock);

                //lock is granted
                return true;
            }

            //if lock is incompatible
            //add transaction to waiting queue of this element
            element.addWaiting(transaction, originalLock, appliedLock);

            //lock is not granted
            return false;
        }

        return false;
    }

    public synchronized void unlock() {

    }

    public void printLockTree() {

        System.out.println(this.lockTree);
    }
}
