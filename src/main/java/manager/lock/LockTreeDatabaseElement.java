package manager.lock;

import java.util.HashMap;

/**
 * This class represents database element in lock tree
 *
 * @author m.amin.rayej
 * @version 1.0
 * @since 1.0
 */
public class LockTreeDatabaseElement extends LockTreeElement {

    /**
     * Maps table name to its element
     */
    private HashMap<String,LockTreeTableElement> tableElements;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockTreeDatabaseElement() {

        this.tableElements = new HashMap<>();
    }

    /**
     * get table element with specified table name that this database element contains
     *
     * @param tableName name of the table to retrieve its element
     * @return table element of the specified table name, or null if database element does not contain any table table element with specified name
     * @since 1.0
     */
    public LockTreeTableElement getTableElement(String tableName) {

        return tableElements.get(tableName);
    }

    /**
     * puts a table element with specified table name in this database element
     *
     * @param tableName name of the table to be added
     * @param tableElement element that represents the table name
     * @since 1.0
     */
    public void putTableElement(String tableName, LockTreeTableElement tableElement) {

        tableElements.put(tableName, tableElement);
    }

}
