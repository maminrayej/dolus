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
    private HashMap<Integer,LockTreeElement> tables;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockTreeDatabaseElement() {

        this.tables = new HashMap<>();
    }

}
