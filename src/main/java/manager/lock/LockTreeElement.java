package manager.lock;

import manager.transaction.Transaction;

import java.util.LinkedList;
import java.util.Queue;

/**
 * This class represents basic element in lock tree
 *
 * @author m.amin.rayej
 * @version 1.0
 * @since 1.0
 */
public class LockTreeElement {

    /**
     * Waiting queue of transactions that requested this element
     */
    private Queue<QueueElement> requestQueue;

    /**
     * Keeps current active lock type. if a compatible lock acquires this element but
     * is a more restrictive lock, current active lock type should be updated to type of the new lock.
     * for example if a transaction A holds an IS lock on this element and transaction B requests an IX lock
     * of this element, request of transaction B will be granted and active lock type will be set to IX.
     */
    private int currentActiveLockType;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockTreeElement() {
        this.requestQueue = new LinkedList<>();
    }

    /**
     * Adds a transaction to the waiting queue
     *
     * @param transaction transaction to be added to waiting queue
     * @param lock requested lock by transaction on this element
     * @since 1.0
     */
    public void addToQueue(Transaction transaction, Lock lock){

        //wrap a QueueElement around transactions and its requested lock
        QueueElement queueElement = new QueueElement(transaction, lock);

        //add them to waiting queue
        requestQueue.add(queueElement);
    }

    /**
     * Get current active lock type
     *
     * @return current active lock type
     * @since 1.o
     */
    public int getCurrentActiveLockType() {
        return currentActiveLockType;
    }

    /**
     * Set current active lock type
     *
     * @param currentActiveLockType lock type to be set as current active type
     * @since 1.0
     */
    public void setCurrentActiveLockType(int currentActiveLockType) {
        this.currentActiveLockType = currentActiveLockType;
    }

    /**
     * This class is a container for transaction and its requested lock and
     *
     * @author m.amin.rayej
     * @version 1.0
     * @since 1.0
     */
    private class QueueElement {


        private Transaction transaction;
        private Lock        lock;

        /**
         * Constructor
         *
         * @param transaction transaction
         * @param lock requested lock by transaction
         * @since 1.0
         */
        public QueueElement(Transaction transaction, Lock lock) {
            this.transaction = transaction;
            this.lock = lock;
        }

        /**
         * Get transaction
         *
         * @return transaction
         * @since 1.0
         */
        public Transaction getTransaction() {
            return transaction;
        }

        /**
         * Set transaction
         *
         * @param transaction transaction
         * @since 1.0
         */
        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
        }

        /**
         * Get lock
         *
         * @return lock
         * @since 1.0
         */
        public Lock getLock() {
            return lock;
        }

        /**
         * Set lock
         *
         * @param lock lock
         * @since 1.0
         */
        public void setLock(Lock lock) {
            this.lock = lock;
        }
    }
}
