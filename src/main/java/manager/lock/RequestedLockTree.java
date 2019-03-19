package manager.lock;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * This class manages locks that a transaction requested in a general tree structure.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class RequestedLockTree {

    /**
     * linked list containing all databases that transaction has locked.
     */
    private LinkedList<RequestedLockTreeElement> databases;

    /**
     * map between database name and its element in requested lock tree
     */
    private HashMap<String, RequestedLockTreeElement> databaseMap;

    /**
     * map between table name and its element in requested lock tree
     */
    private HashMap<String, RequestedLockTreeElement> tableMap;

    /**
     * default constructor
     *
     * @since 1.0
     */
    public RequestedLockTree() {

        this.databases = new LinkedList<>();
        this.databaseMap = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    /**
     * add a database lock to the locks that transaction has requested till now
     *
     * @param databaseName name of the database that is requested by the transaction
     * @param databaseElement lock tree element in lock tree that represents this database
     * @since 1.0
     */
    public void addDatabaseLock(String databaseName, LockTreeElement databaseElement) {

        //create a new requested lock element to represent the new locked database in acquire lock tree
        RequestedLockTreeElement requestedDatabaseElement = new RequestedLockTreeElement(databaseElement, true);

        //add the new requested lock to the head of the queue
        this.databases.addFirst(requestedDatabaseElement);

        //add new requested database lock to its hash map
        //this lets us retrieve the database lock faster
        this.databaseMap.put(databaseName, requestedDatabaseElement);
    }

    /**
     * add a table lock to the locks that transaction has requested till now
     *
     * @param databaseName  name of the database that contains the table name
     * @param tableName name of the table that is locked by the transaction
     * @param tableElement lock tree table element that represents the requested lock in lock tree
     * @since 1.0
     */
    public void addTableLock(String databaseName, String tableName, LockTreeElement tableElement) {

        //create a new requested lock element to represent the new locked table in requested lock tree
        RequestedLockTreeElement requestedTableElement = new RequestedLockTreeElement(tableElement, true);

        //get database element that contains the table
        RequestedLockTreeElement requestedDatabaseElement = databaseMap.get(databaseName);

        //add new requested table element to its database
        requestedDatabaseElement.addChild(requestedTableElement);

        //add new requested table lock to its hash map
        //this lets us retrieve the table lock faster
        tableMap.put(tableName, requestedTableElement);
    }

    /**
     * add a record lock to the locks that transaction has requested till now
     *
     * @param table name of the table containing the record
     * @param recordElement lock tree element that represents the requested lock in lock tree
     * @since 1.0
     */
    public void addRecordLock(String table, LockTreeElement recordElement) {

        //create a new requested lock element to represent the new locked record in requested lock tree
        RequestedLockTreeElement requestedRecordElement = new RequestedLockTreeElement(recordElement, false);

        //get table element that contains the record
        RequestedLockTreeElement requestedTableElement = tableMap.get(table);

        //add new requested record element to its table
        requestedTableElement.addChild(requestedRecordElement);
    }

    /**
     * get requested lock tree
     *
     * @return the requested lock tree
     * @since 1.0
     */
    public LinkedList<RequestedLockTreeElement> getRequestedLockTree() {
        return this.databases;
    }

    /**
     * get requested database element specified by database name
     *
     * @param databaseName name of the database to retrieve
     * @return retrieved requested database lock element
     * @since 1.0
     */
    public RequestedLockTreeElement getRequestedDatabaseElement(String databaseName) {
        return this.databaseMap.get(databaseName);
    }

    /**
     * get requested table element specified by table name
     *
     * @param tableName name of the table to retrieve
     * @return retrieved requested table lock element
     * @since 1.0
     */
    public RequestedLockTreeElement getRequestedTableElement(String tableName) {
        return this.tableMap.get(tableName);
    }
}
