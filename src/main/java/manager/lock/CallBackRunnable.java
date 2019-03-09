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

    private Queue<LockRequest> firstQueue;

    private Queue<LockRequest> secondQueue;

    private ReentrantLock firstQueueLock;

    private ReentrantLock secondQueueLock;

    public CallBackRunnable(Queue<LockRequest> firstQueue, Queue<LockRequest> secondQueue, ReentrantLock firstQueueLock, ReentrantLock secondQueueLock) {
        this.firstQueue = firstQueue;
        this.secondQueue = secondQueue;
        this.firstQueueLock = firstQueueLock;
        this.secondQueueLock = secondQueueLock;
    }

    @Override
    public void run() {

        boolean isFirstQueueEmpty = false;
        boolean isSecondQueueEmpty = false;

        while (!(exit && isFirstQueueEmpty && isSecondQueueEmpty)) {

            //try to lock the first queue
            if (firstQueueLock.tryLock()) {

                isFirstQueueEmpty = firstQueue.isEmpty();

                //inform all transactions in the first queue of their granted locks
                informTransaction(firstQueue);

                //unlock the first queue
                firstQueueLock.unlock();
            }
            if (secondQueueLock.tryLock()) {

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
    private void informTransaction(Queue<LockRequest> queue) {

        int queueSize = queue.size();

        //loop through the queue and inform each transaction of its granted lock
        for (int i = 0; i < queueSize; i++) {

            //get the head of the queue
            LockRequest lockRequest = queue.remove();

            //get transaction
            Transaction transaction = lockRequest.getTransaction();

            //get granted lock
            Lock lock = lockRequest.getAppliedLock();

            //inform the transaction that requested lock is granted
            transaction.lockIsGranted(lock);
        }
    }

    public void exit() {
        this.exit = true;
    }
}
