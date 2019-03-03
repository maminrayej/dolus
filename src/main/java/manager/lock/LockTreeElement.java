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
     * Queue of transactions which their request to access this element is granted
     */
    private Queue<QueueElement> grantedQueue;

    /**
     * Queue of transactions which their request to access this element is not granted
     */
    private Queue<QueueElement> waitingQueue;

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

        this.grantedQueue = new LinkedList<>();

        this.waitingQueue = new LinkedList<>();
    }

    /**
     * Adds a granted transaction to granted list
     *
     * @param transaction  transaction to be added to granted queue
     * @param originalLock original lock requested by transaction
     * @param appliedLock  lock that must applied to this element
     * @since 1.0
     */
    public void addGranted(Transaction transaction, Lock originalLock, Lock appliedLock) {

        QueueElement queueElement = new QueueElement(transaction, originalLock, appliedLock);

        grantedQueue.add(queueElement);
    }

    /**
     * Adds a blocked transaction to waiting list
     *
     * @param transaction  transaction to be added to waiting queue
     * @param originalLock original lock requested by transaction
     * @param appliedLock  lock that must applied to this element
     * @since 1.0
     */
    public void addWaiting(Transaction transaction, Lock originalLock, Lock appliedLock) {

        QueueElement queueElement = new QueueElement(transaction, originalLock, appliedLock);

        waitingQueue.add(queueElement);
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
        return gson.toJson(this);
    }

}
