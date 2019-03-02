package manager.lock;

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

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockManager() {

        lockTree = new HashMap<>();
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

        //get name of the database to be locked
        String database = lock.getDatabase();

        //get name of the table to be lock
        String table = lock.getTable();

        //get id of the record to be locked
        Integer record = lock.getRecord();

        //determine the lock level
        int lockLevel = getLockLevel(lock);
        if (lockLevel == LockLevels.NOT_VALID_LEVEL) {
            Log.log(String.format("Lock requested by transaction: %d is not a valid request", transaction.getTransactionId()), componentName, Log.ERROR);
            return false;
        }

        //shows if requested lock is granted immediately or not
        boolean granted = false;

        //according to the lock level call its appropriate manager
        if (lockLevel == LockLevels.DATABASE_LOCK) {

            granted = manageDatabaseLevelLock(transaction, lock, database);

        } else if (lockLevel == LockLevels.TABLE_LOCK) {

            granted = manageTableLevelLock(transaction, lock, database, table);

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
     * Manages a database level lock request
     *
     * @param transaction transaction that requested the lock
     * @param lock        lock to be acquired
     * @param database    database to be locked
     * @return true if requested is granted, false otherwise
     * @since 1.0
     */
    private boolean manageDatabaseLevelLock(Transaction transaction, Lock lock, String database) {

        //get type of the lock
        int lockType = lock.getType();

        //get database element with name specified by database variable
        LockTreeDatabaseElement databaseElement = this.lockTree.get(database);

        //if this is a new node in tree -> no lock has been acquired on this database yet
        if (databaseElement == null) {

            //create a new database element
            databaseElement = new LockTreeDatabaseElement();

            //add this new database element to lock tree
            lockTree.put(database, databaseElement);

            //add transaction to waiting queue of this element
            databaseElement.addToQueue(transaction, lock);

            //set current active lock type to requested type
            databaseElement.setCurrentActiveLockType(lockType);

            //lock is granted
            return true;

        } else if (lockType == LockTypes.SHARED) {

            //get current active lock type and use it to see whether requested lock
            //is compatible with current active lock type or not
            int currentActiveLockType = databaseElement.getCurrentActiveLockType();

            //SHARED lock is compatible with Shared, Exclusive, Update and Intent Shared locks
            //if lock is compatible with current active lock type
            if (currentActiveLockType == LockTypes.SHARED) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.UPDATE) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.INTENT_SHARED) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //Shared lock is more restrictive than Intent shared
                //so current active lock type on this element must change to Shared
                databaseElement.setCurrentActiveLockType(LockTypes.SHARED);

                //lock is granted
                return true;
            }

            //lock is incompatible with current active lock
            //add transaction to waiting queue of this element
            databaseElement.addToQueue(transaction, lock);

            //lock is not granted
            return false;

        } else if (lockType == LockTypes.EXCLUSIVE) {

            //Exclusive lock is not compatible with any lock
            //add transaction to waiting queue of this element
            databaseElement.addToQueue(transaction, lock);

            //lock is not granted
            return false;

        } else if (lockType == LockTypes.UPDATE) {

            //get current active lock type and use it to see whether requested lock
            //is compatible with current active lock type or not
            int currentActiveLockType = databaseElement.getCurrentActiveLockType();

            //Update lock is compatible with Intent shared and Shared locks
            //if lock is compatible with current active lock type
            if (currentActiveLockType == LockTypes.INTENT_SHARED) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //Update lock is more restrictive so current active lock type must set to Update
                databaseElement.setCurrentActiveLockType(LockTypes.UPDATE);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.SHARED) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //Update lock is more restrictive so current active lock type must set to Update
                databaseElement.setCurrentActiveLockType(LockTypes.UPDATE);

                //lock is granted
                return true;
            }

            //lock is incompatible
            //add transaction to waiting queue of this element
            databaseElement.addToQueue(transaction, lock);

            //lock is not granted
            return false;

        } else if (lockType == LockTypes.INTENT_SHARED) {

            //get current active lock type and use it to see whether requested lock
            //is compatible with current active lock type or not
            int currentActiveLockType = databaseElement.getCurrentActiveLockType();

            //Intent shared lock is only incompatible with Exclusive lock
            //if lock is incompatible with current active lock type
            if (currentActiveLockType == LockTypes.EXCLUSIVE) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //lock is not granted
                return false;
            } else {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //lock is granted
                return true;
            }

        } else if (lockType == LockTypes.INTENT_EXCLUSIVE) {

            //get current active lock type and use it to see whether requested lock
            //is compatible with current active lock type or not
            int currentActiveLockType = databaseElement.getCurrentActiveLockType();

            //Intent Exclusive lock is only compatible with Intent Shared and Intent Exclusive lock
            //if lock is compatible
            if (currentActiveLockType == LockTypes.INTENT_SHARED) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //Intent Exclusive is more restrictive than Intent Shared, so current active lock type must set to Intent Exclusive
                databaseElement.setCurrentActiveLockType(LockTypes.INTENT_EXCLUSIVE);

                //lock is granted
                return true;
            } else if (currentActiveLockType == LockTypes.INTENT_EXCLUSIVE) {

                //add transaction to waiting queue of this element
                databaseElement.addToQueue(transaction, lock);

                //lock is granted
                return true;
            }

            //if lock is incompatible
            //add transaction to waiting queue of this element
            databaseElement.addToQueue(transaction, lock);

            //lock is not granted
            return false;
        }

        return false;
    }


    private boolean manageTableLevelLock(Transaction transaction, Lock lock, String database, String table) {

        return false;
    }

    private boolean manageRecordLevelLock(Transaction transaction, Lock lock, String database, String table, Integer record) {

        return false;
    }

    public synchronized void unlock() {

    }
}
