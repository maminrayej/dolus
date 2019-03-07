package manager.lock;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * This class manages locks that a transaction acquire in a general tree structure.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class AcquiredLockTree {

    /**
     * linked list containing all databases that transaction has locked.
     */
    private LinkedList<AcquiredLockTreeElement> databases;

    /**
     * map between database name and its element in acquire tree
     */
    private HashMap<String, AcquiredLockTreeElement> databaseMap;

    /**
     * map between table name and its element in acquire tree
     */
    private HashMap<String, AcquiredLockTreeElement> tableMap;

    /**
     * default constructor
     *
     * @since 1.0
     */
    public AcquiredLockTree() {

        this.databases = new LinkedList<>();
        this.databaseMap = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    /**
     * add a database lock to the locks that transaction has acquired till now
     *
     * @param databaseName name of the database that is locked by the transaction
     * @param databaseElement lock tree element in lock tree that represents this database
     * @since 1.0
     */
    public void addDatabaseLock(String databaseName, LockTreeElement databaseElement) {

        //create a new acquired lock element to represent the new locked database in acquire lock tree
        AcquiredLockTreeElement acquiredDatabaseElement = new AcquiredLockTreeElement(databaseElement, true);

        //add the new acquired lock to the head of the queue
        this.databases.addFirst(acquiredDatabaseElement);

        //add new acquired database lock to its hash map
        //this lets us retrieve the database lock faster
        this.databaseMap.put(databaseName, acquiredDatabaseElement);
    }

    /**
     * add a table lock to the locks that transaction has acquired till now
     *
     * @param databaseName  name of the database that contains the table name
     * @param tableName name of the table that is locked by the transaction
     * @param tableElement lock tree table element that represents the acquired lock in lock tree
     * @since 1.0
     */
    public void addTableLock(String databaseName, String tableName, LockTreeElement tableElement) {

        //create a new acquired lock element to represent the new locked table in acquire lock tree
        AcquiredLockTreeElement acquiredTableElement = new AcquiredLockTreeElement(tableElement, true);

        //get database element that contains the table
        AcquiredLockTreeElement acquiredDatabaseElement = databaseMap.get(databaseName);

        //add new acquired table element to its database
        acquiredDatabaseElement.addChild(acquiredTableElement);

        //add new acquired table lock to its hash map
        //this lets us retrieve the table lock faster
        tableMap.put(tableName, acquiredTableElement);
    }

    /**
     * add a record lock to the locks that transaction has acquired till now
     *
     * @param table name of the table containing the record
     * @param recordElement lock tree element that represents the acquired lock in lock tree
     * @since 1.0
     */
    public void addRecordLock(String table, LockTreeElement recordElement) {

        //create a new acquired lock element to represent the new locked record in acquire lock tree
        AcquiredLockTreeElement acquiredRecordElement = new AcquiredLockTreeElement(recordElement, false);

        //get table element that contains the record
        AcquiredLockTreeElement acquiredTableElement = tableMap.get(table);

        //add new acquired record element to its table
        acquiredTableElement.addChild(acquiredRecordElement);
    }

    /**
     * get acquired lock tree
     *
     * @return the acquired lock tree
     * @since 1.0
     */
    public LinkedList<AcquiredLockTreeElement> getAcquiredLockTree() {
        return this.databases;
    }
}
