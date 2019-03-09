package manager.lock;

import manager.transaction.Transaction;

import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a background thread to inform transactions of their new granted locks
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class CallBackRunnable implements Runnable {

    private volatile boolean exit = false;

    private Queue<QueueElement> firstQueue;

    private Queue<QueueElement> secondQueue;

    private ReentrantLock firstQueueLock;

    private ReentrantLock secondQueueLock;

    public CallBackRunnable(Queue<QueueElement> firstQueue, Queue<QueueElement> secondQueue, ReentrantLock firstQueueLock, ReentrantLock secondQueueLock) {
        this.firstQueue = firstQueue;
        this.secondQueue = secondQueue;
        this.firstQueueLock = firstQueueLock;
        this.secondQueueLock = secondQueueLock;
    }

    @Override
    public void run() {

        boolean isFirstQueueEmpty = true;
        boolean isSecondQueueEmpty = true;

        while (!(exit && isFirstQueueEmpty && isSecondQueueEmpty)) {

            //try to lock the first queue
            if (firstQueueLock.tryLock()) {

                isFirstQueueEmpty = firstQueue.isEmpty();

                //inform all transactions in the first queue of their granted locks
                informTransaction(firstQueue);

                //unlock the first queue
                firstQueueLock.unlock();
            } else if (secondQueueLock.tryLock()) {

                isSecondQueueEmpty = secondQueue.isEmpty();

                //inform all transactions in the first queue of their granted locks
                informTransaction(secondQueue);

                //unlock the second queue
                secondQueueLock.unlock();
            }
        }


    }

    /**
     * Loops through the given queue and inform each transaction of its granted lock
     *
     * @param queue queue of granted requests
     * @since 1.0
     */
    private void informTransaction(Queue<QueueElement> queue) {

        int queueSize = queue.size();

        //loop through the queue and inform each transaction of its granted lock
        for (int i = 0; i < queueSize; i++) {

            //get the head of the queue
            QueueElement queueElement = queue.remove();

            //get transaction
            Transaction transaction = queueElement.getTransaction();

            //get granted lock
            Lock lock = queueElement.getAppliedLock();

            //inform the transaction that requested lock is granted
            transaction.lockIsGranted(lock);
        }
    }

    public void exit() {
        this.exit = true;
    }
}
