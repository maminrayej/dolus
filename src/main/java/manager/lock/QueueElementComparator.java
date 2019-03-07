package manager.lock;

import java.util.Comparator;

/**
 * this class compares two queue elements based on the lock type that is applied to them.
 * item with the most restrictive lock type will be put at the head of the queue
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueueElementComparator implements Comparator<QueueElement> {

    @Override
    public int compare(QueueElement element1, QueueElement element2) {
        return element1.getAppliedLock().getType() - element2.getAppliedLock().getType();
    }

}
