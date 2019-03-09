package manager.lock;

import java.util.Comparator;

/**
 * this class compares two lock request elements based on the applied lock type.
 * item with the most restrictive lock type will be put at the head of the queue
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class LockRequestComparator implements Comparator<LockRequest> {

    @Override
    public int compare(LockRequest element1, LockRequest element2) {
        return element1.getAppliedLock().getType() - element2.getAppliedLock().getType();
    }

}
