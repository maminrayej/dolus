package manager.lock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import manager.transaction.Transaction;

import java.util.LinkedList;
import java.util.Queue;

/**
 * This class represents basic element in originalLock tree
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
     * Keeps current active originalLock type. if a compatible originalLock acquires this element but
     * is a more restrictive originalLock, current active originalLock type should be updated to type of the new originalLock.
     * for example if a transaction A holds an IS originalLock on this element and transaction B requests an IX originalLock
     * of this element, request of transaction B will be granted and active originalLock type will be set to IX.
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
     * @param transaction  transaction to be added to waiting queue
     * @param originalLock original lock requested by transaction
     * @param appliedLock  lock that must applied to this element
     * @since 1.0
     */
    public void addToQueue(Transaction transaction, Lock originalLock, Lock appliedLock) {

        //wrap a QueueElement around transactions and its requested originalLock
        QueueElement queueElement = new QueueElement(transaction, originalLock, appliedLock);

        //add them to waiting queue
        requestQueue.add(queueElement);
    }

    /**
     * Get current active originalLock type
     *
     * @return current active originalLock type
     * @since 1.o
     */
    public int getCurrentActiveLockType() {
        return currentActiveLockType;
    }

    /**
     * Set current active originalLock type
     *
     * @param currentActiveLockType originalLock type to be set as current active type
     * @since 1.0
     */
    public void setCurrentActiveLockType(int currentActiveLockType) {
        this.currentActiveLockType = currentActiveLockType;
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(requestQueue) + currentActiveLockType;
    }

    /**
     * This class is a container for transaction and its requested originalLock and
     *
     * @author m.amin.rayej
     * @version 1.0
     * @since 1.0
     */
    private class QueueElement {


        private Transaction transaction;
        private Lock originalLock;
        private Lock appliedLock;

        /**
         * Constructor
         *
         * @param transaction  transaction
         * @param originalLock original requested lock by transaction
         * @param appliedLock  lock this is applied to this element
         * @since 1.0
         */
        public QueueElement(Transaction transaction, Lock originalLock, Lock appliedLock) {
            this.transaction = transaction;
            this.originalLock = originalLock;
            this.appliedLock = appliedLock;
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
         * Get originalLock
         *
         * @return originalLock
         * @since 1.0
         */
        public Lock getOriginalLock() {
            return originalLock;
        }

        /**
         * Set originalLock
         *
         * @param originalLock originalLock
         * @since 1.0
         */
        public void setOriginalLock(Lock originalLock) {
            this.originalLock = originalLock;
        }

        /**
         * Get applied lock
         *
         * @return applied lock
         * @since 1.0
         */
        public Lock getAppliedLock() {
            return appliedLock;
        }

        /**
         * Set applied lock
         *
         * @param appliedLock lock to apply on this element
         * @since 1.0
         */
        public void setAppliedLock(Lock appliedLock) {
            this.appliedLock = appliedLock;
        }
    }
}
