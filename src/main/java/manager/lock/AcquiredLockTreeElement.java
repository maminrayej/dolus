package manager.lock;

import java.util.LinkedList;

public class AcquiredLockTreeElement {

    private LockTreeElement lockTreeElement;

    private LinkedList<AcquiredLockTreeElement> children;

    public AcquiredLockTreeElement(LockTreeElement lockTreeElement, boolean hasChild) {

        this.lockTreeElement = lockTreeElement;

        if (hasChild)
            this.children = new LinkedList<>();
        else
            this.children = null;
    }

    public void addChild(AcquiredLockTreeElement acquiredLockTreeELement) {
        children.addFirst(acquiredLockTreeELement);
    }

    public LockTreeElement getLockTreeElement() {
        return lockTreeElement;
    }

    public LinkedList<AcquiredLockTreeElement> getChildren() {
        return children;
    }
}
