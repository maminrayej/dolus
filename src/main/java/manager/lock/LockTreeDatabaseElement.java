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

    public LockTreeTableElement getTableElement(String tableName) {

        return tableElements.get(tableName);
    }

    public void putTableElement(String tableName, LockTreeTableElement tableElement) {

        tableElements.put(tableName, tableElement);
    }

    public HashMap<String, LockTreeTableElement> getTableElementsMap() {

        return tableElements;
    }

}
