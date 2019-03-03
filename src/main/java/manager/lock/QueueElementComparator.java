package manager.lock;

import java.util.Comparator;

public class QueueElementComparator implements Comparator<QueueElement> {

    @Override
    public int compare(QueueElement element1, QueueElement element2) {
        return element1.getAppliedLock().getType() - element2.getAppliedLock().getType();
    }

}
