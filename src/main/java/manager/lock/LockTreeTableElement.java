package manager.lock;

import java.util.HashMap;

/**
 * This class represents table element in lock tree
 *
 * @author m.amin.rayej
 * @version 1.0
 * @since 1.0
 */
public class LockTreeTableElement extends LockTreeElement {

    /**
     * Maps record id to its element
     */
    private HashMap<Integer,LockTreeElement> recordElements;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockTreeTableElement() {

        this.recordElements = new HashMap<>();
    }

    public LockTreeElement getRecordElement(Integer recordId) {

        return recordElements.get(recordId);
    }

    public void putRecordElement(Integer recordId, LockTreeElement recordElement) {

        recordElements.put(recordId, recordElement);
    }
}
