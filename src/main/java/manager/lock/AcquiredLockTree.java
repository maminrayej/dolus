package manager.lock;

import java.util.HashMap;
import java.util.LinkedList;

public class AcquiredLockTree {

    private LinkedList<AcquiredLockTreeElement> databases;

    private HashMap<String, AcquiredLockTreeElement> databaseMap;
    private HashMap<String, AcquiredLockTreeElement> tableMap;

    public AcquiredLockTree() {

        this.databases = new LinkedList<>();
        this.databaseMap = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    public void addDatabaseLock(String database, LockTreeElement databaseElement) {

        AcquiredLockTreeElement acquiredDatabaseElement = new AcquiredLockTreeElement(databaseElement, true);

        this.databases.addFirst(acquiredDatabaseElement);

        this.databaseMap.put(database, acquiredDatabaseElement);
    }

    public void addTableLock(String database, String table, LockTreeElement tableElement) {

        AcquiredLockTreeElement acquiredTableElement = new AcquiredLockTreeElement(tableElement, true);

        AcquiredLockTreeElement acquiredDatabaseElement = databaseMap.get(database);

        acquiredDatabaseElement.addChild(acquiredTableElement);

        tableMap.put(table, acquiredTableElement);
    }

    public void addRecordLock(String table, LockTreeElement recordElement) {

        AcquiredLockTreeElement acquiredRecordElement = new AcquiredLockTreeElement(recordElement, false);

        AcquiredLockTreeElement acquiredTableElement = tableMap.get(table);

        acquiredTableElement.addChild(acquiredRecordElement);
    }

    public AcquiredLockTreeElement getAcquiredDatabase(String databaseName) {
        return databaseMap.get(databaseName);
    }

    public AcquiredLockTreeElement getAcquiredTable(String tableName) {
        return tableMap.get(tableName);
    }

    public LinkedList<AcquiredLockTreeElement> getAcquiredLockTree() {
        return this.databases;
    }
}
