package manager.lock;

import manager.transaction.Transaction;

/**
 * This class is a container for transaction and its requested lock and applied lock
 *
 * @author m.amin.rayej
 * @version 1.0
 * @since 1.0
 */
public class LockRequest {

    private Transaction transaction;
    private Lock originalLock;
    private Lock appliedLock;

    /**
     * Constructor
     *
     * @param transaction  transaction
     * @param originalLock original requested lock by transaction
     * @param appliedLock  lock that is applied to the element containing this request lock
     * @since 1.0
     */
    public LockRequest(Transaction transaction, Lock originalLock, Lock appliedLock) {
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