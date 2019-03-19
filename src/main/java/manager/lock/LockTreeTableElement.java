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
    public LockTreeTableElement(String name) {

        super(name);
        this.recordElements = new HashMap<>();
    }

    /**
     * get the record element specified by record id
     *
     * @param recordId id of the record
     * @return lock tree element that represents the record id, or null if table element does not contain any record element with specified id
     * @since 1.0
     */
    public LockTreeElement getRecordElement(Integer recordId) {

        return recordElements.get(recordId);
    }

    /**
     * puts a record element with specified record id in this table element
     *
     * @param recordId id of the record
     * @param recordElement element that represents the specified id
     * @since 1.0
     */
    public void putRecordElement(Integer recordId, LockTreeElement recordElement) {

        recordElements.put(recordId, recordElement);
    }

    /**
     * Remove record element specified by its unique name in lock tree
     *
     * @param name name of the element to be removed
     * @since 1.0
     */
    public void removeRecordElement(String name) {
        recordElements.remove(name);
    }
}
