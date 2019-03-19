package manager.lock;

import common.Log;
import manager.lock.LockConstants.LockTypes;
import manager.transaction.Transaction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * This class represents basic element in originalLock tree
 *
 * @author m.amin.rayej
 * @version 1.0
 * @since 1.0
 */
public class LockTreeElement {


    private final String componentName = "LockTreeElement";

    /**
     * List of transactions which their request to access this element is granted
     */
    private List<LockRequest> grantedList;

    /**
     * Queue of transactions which their request to access this element is not granted
     */
    private Queue<LockRequest> waitingQueue;

    /**
     * Mapping between a transaction Id to its element in the granted list
     * this data structure is used to retrieve the granted transactions fast
     */
    private HashMap<String, LockRequest> grantedMap;

    /**
     * Mapping between a transaction id to its element in the waiting list
     * this data structure is used to retrieve the waiting requests fast
     */
    private HashMap<String, LockRequest> waitingMap;

    /**
     * Keeps current active lock type. if a compatible lock request acquires this element but
     * is a more restrictive lock type, current active lock type must be updated to type of the more restrictive lock.
     * for example if a transaction A holds an IS lock on this element and transaction B requests an IX lock
     * on this element, request of transaction B will be granted and active lock type will be set to IX.
     */
    private int currentActiveLockType;

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public LockTreeElement() {

        this.grantedList = new LinkedList<>();

        this.waitingQueue = new LinkedList<>();

        this.grantedMap = new HashMap<>();

        this.waitingMap = new HashMap<>();

        //there is no lock held on this element
        this.currentActiveLockType = LockTypes.NO_LOCK;
    }

    /**
     * Releases the lock held by the transaction(specified by the transaction id) on this element
     *
     * @param transactionId id of the transaction that holds a lock on this element
     * @return a linked list of transactions that are now granted because of the released lock, or null if both granted and waiting list of this element is empty
     * @since 1.0
     */
    public LinkedList<LockRequest> releaseLock(String transactionId) {

        //get queue element that represents this transaction id in granted list
        //we use granted map for fast retrieval of elements
        LockRequest grantedRequest = grantedMap.get(transactionId);

        //there is no granted lock held by this transaction of this element
        if (grantedRequest == null) {

            //retrieve the element in waiting queue that represents the requested lock by transaction
            LockRequest waitingRequest = waitingMap.get(transactionId);

            //remove the lock requested by the transaction from waiting list
            waitingQueue.remove(waitingRequest);

            //remove the transaction request entry from waiting map
            waitingMap.remove(transactionId);

            //if both granted and waiting list of this element is empty,
            //it should be removed by the lock manager from lock tree
            if (waitingQueue.size() == 0 && grantedList.size() == 0) {

                currentActiveLockType = LockTypes.NO_LOCK;

                return null;
            }

            //there are no new granted transactions on this element
            return new LinkedList<>();
        }

        //remove the transaction from granted hash map
        grantedMap.remove(transactionId);

        //remove the transaction from granted list
        grantedList.remove(grantedRequest);

        //update current active lock type after removing the element from granted list
        //if there is no element left is the granted list -> set lock type to no active lock
        //else set the active lock type to the head of the queue
        if (grantedList.size() == 0)
            currentActiveLockType = LockTypes.NO_LOCK;
        else
            currentActiveLockType = grantedList.get(0).getAppliedLock().getType();

        //list of lock requests that are granted now because of the released lock
        LinkedList<LockRequest> grantedLockRequests;

        //get list of requests that are granted because of the released lock
        grantedLockRequests = getGrantedLockRequests();

        //if granted list is changed, it must be sorted
        if (grantedLockRequests.size() != 0)
            grantedList.sort(new LockRequestComparator());

        //check whether there is any requested or granted lock on this element
        if (waitingQueue.size() == 0 && grantedList.size() == 0) {

            currentActiveLockType = LockTypes.NO_LOCK;

            return null;//inform the lock manager to remove this element from lock tree
        }

        return grantedLockRequests;
    }

    /**
     * Using this method a transaction can acquire a lock on this element
     *
     * @param transaction  transaction that requested the lock
     * @param appliedLock  lock to be applied to this element
     * @param originalLock original lock requested by the transaction
     * @return true if request granted and false otherwise
     */
    public boolean acquireLock(Transaction transaction, Lock appliedLock, Lock originalLock) {

        //check compatibility between type of the request lock and current active lock type
        boolean isCompatible = checkCompatibility(appliedLock);

        if (isCompatible) {

            //wrap a LockRequest object around transaction and its lock request
            LockRequest lockRequest = new LockRequest(transaction, originalLock, appliedLock);

            //add granted request to grant list
            grantedList.add(lockRequest);

            //add granted request to granted map
            grantedMap.put(transaction.getTransactionId(), lockRequest);

            //sort the granted list so that most restrictive lock comes to the head of the granted list
            grantedList.sort(new LockRequestComparator());

            //get head of the list
            //get its applied lock on this element
            //get type of that applied lock
            //the retrieved type must be the current active lock type of this element
            currentActiveLockType = grantedList.get(0).getAppliedLock().getType();

            //request is granted
            return true;
        } else {
            //wrap a LockRequest object around transaction and its lock request
            LockRequest lockRequest = new LockRequest(transaction, originalLock, appliedLock);

            //add the element to the waiting queue
            waitingQueue.add(lockRequest);

            //add the request to the waiting map
            waitingMap.put(transaction.getTransactionId(), lockRequest);

            //request is not granted
            return false;
        }
    }

    /**
     * Degrades a lock type into a less strict one
     *
     * @param transaction transaction that requested the degrading
     * @param degradedLockType lock type the transaction wants its request to be degraded to
     * @since 1.0
     */
    public LinkedList<LockRequest> degradeLock(Transaction transaction, int degradedLockType) {

        //get lock request of the transaction on this element
        LockRequest lockRequest = grantedMap.get(transaction.getTransactionId());

        //if there is no granted lock request from this transaction then ignore the request
        if (lockRequest == null)
            return null;

        //get current type of the lock request
        int currentLockType = lockRequest.getAppliedLock().getType();

        //if current lock type less restrict than the degraded lock type: then request is not permitted
        if (degradedLockType <= currentLockType) {
            Log.log(String.format("Transaction: %s wants to degrade %d to %d. Not Permitted",transaction.getTransactionId(), currentLockType, degradedLockType),componentName,Log.WARNING);
        }
        else {
            lockRequest.getAppliedLock().setType(degradedLockType);

            //TODO -> performance optimization -> not every degrading needs sorting the granted list
            //sort the granted list
            grantedList.sort(new LockRequestComparator());

            //update the current active lock type
            currentActiveLockType = grantedList.get(0).getAppliedLock().getType();

            //get new granted lock requests because of degrading the lock
            LinkedList<LockRequest> grantedLockRequests = getGrantedLockRequests();

            //if granted list is changed, it must be sorted
            if (grantedLockRequests.size() != 0)
                grantedList.sort(new LockRequestComparator());

            return grantedLockRequests;
        }

        return null;
    }

    /**
     * Updates granted and waiting queue and returns new granted lock requests
     *
     * @return new granted lock requests
     */
    private LinkedList<LockRequest> getGrantedLockRequests() {

        //list of lock requests that are granted now because of the released lock
        LinkedList<LockRequest> grantedRequestedLocks = new LinkedList<>();

        //loop through transactions in waiting queue and grant each one that is compatible with the current active lock type
        for (int i = 0; i < waitingQueue.size(); i++) {

            //get first element in the waiting queue
            LockRequest waitingRequest = waitingQueue.peek();

            //check whether the requested lock by waiting element is compatible with current active lock type
            boolean isCompatible = checkCompatibility(waitingRequest.getAppliedLock());

            //if requested lock type by waiting element is compatible with current active lock type
            if (isCompatible) {

                //remove the element from waiting queue
                waitingQueue.remove();

                //remove the waiting request from waiting map
                waitingMap.remove(waitingRequest.getTransaction().getTransactionId());

                //add element to granted list
                grantedList.add(waitingRequest);

                //add element to granted map
                grantedMap.put(waitingRequest.getTransaction().getTransactionId(), waitingRequest);

                //add the transaction to the granted transactions list to inform lock manager
                grantedRequestedLocks.addFirst(waitingRequest);

                //update current active lock type of this element
                //this helps us to update the active lock type without having to sort the granted list every time a transaction is granted
                //locks with more restrictions are numbered lower, so if lock type of the new granted element is lower than current active lock type,
                //then current active lock type must be updated
                if (waitingRequest.getAppliedLock().getType() < currentActiveLockType)
                    currentActiveLockType = waitingRequest.getAppliedLock().getType();
            } else// element add the head of queue can not be granted
                break;
        }

        return grantedRequestedLocks;
    }
    /**
     * Checks compatibility between current active lock type and lock type of the argument
     *
     * @param lock lock object to compare its type with current active lock type
     * @return true if lock is compatible with current active lock type
     */
    private boolean checkCompatibility(Lock lock) {

        //get the type of the lock
        int lockType = lock.getType();

        //if there is no active lock on this element -> request is compatible
        if (currentActiveLockType == LockTypes.NO_LOCK)
            return true;

        //if request lock is a shared lock
        if (lockType == LockTypes.SHARED) {

            //shared lock is compatible with SHARED, UPDATE and INTENT SHARED locks
            if (currentActiveLockType == LockTypes.SHARED ||
                    currentActiveLockType == LockTypes.UPDATE ||
                    currentActiveLockType == LockTypes.INTENT_SHARED) {
                return true;
            }
            return false;

        } else if (lockType == LockTypes.EXCLUSIVE) {

            //EXCLUSIVE lock is not compatible with any other lock
            return false;

        } else if (lockType == LockTypes.UPDATE) {

            //UPDATE lock is compatible with INTENT SHARED and SHARED lock
            if (currentActiveLockType == LockTypes.INTENT_SHARED ||
                    currentActiveLockType == LockTypes.SHARED) {
                return true;
            }
            return false;

        } else if (lockType == LockTypes.INTENT_SHARED) {

            //INTENT SHARED lock is only incompatible with EXCLUSIVE lock
            if (currentActiveLockType == LockTypes.EXCLUSIVE) {
                return false;
            }
            return true;

        } else if (lockType == LockTypes.INTENT_EXCLUSIVE) {

            //INTENT EXCLUSIVE lock is compatible with INTENT SHARED and INTENT EXCLUSIVE
            if (currentActiveLockType == LockTypes.INTENT_SHARED ||
                    currentActiveLockType == LockTypes.INTENT_EXCLUSIVE) {
                return true;
            }
            return false;

        }
        return false;
    }
}
