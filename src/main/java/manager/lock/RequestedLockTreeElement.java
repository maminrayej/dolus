package manager.lock;

import java.util.LinkedList;

/**
 * This class represents an element in the requested lock tree
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class RequestedLockTreeElement {

    /**
     * element in lock tree
     */
    private LockTreeElement lockTreeElement;

    /**
     * list of children of this element in requested lock tree
     */
    private LinkedList<RequestedLockTreeElement> children;

    /**
     * default constructor
     *
     * @param lockTreeElement element in lock tree
     * @param hasChild determines if this requested element can have child elements or not
     * @since 1.0
     */
    public RequestedLockTreeElement(LockTreeElement lockTreeElement, boolean hasChild) {

        this.lockTreeElement = lockTreeElement;

        if (hasChild)
            this.children = new LinkedList<>();
        else
            this.children = null;
    }

    /**
     * adds a child element to this element
     *
     * @param requestedLockTreeElement element to be added as a child
     * @since 1.0
     */
    public void addChild(RequestedLockTreeElement requestedLockTreeElement) {
        children.addFirst(requestedLockTreeElement);
    }

    /**
     * get the lock element in lock tree
     *
     * @return lock element that this objects is wrapped around
     * @since 1.0
     */
    public LockTreeElement getLockTreeElement() {
        return lockTreeElement;
    }

    /**
     * get list of all children of this element
     *
     * @return list of children elements
     * @since 1.0
     */
    public LinkedList<RequestedLockTreeElement> getChildren() {
        return children;
    }
}
