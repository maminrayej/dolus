package manager.lock;

import manager.transaction.Transaction;

import java.util.LinkedList;
import java.util.Queue;

public class LockTreeElement {

    private Queue<Transaction> requestQueue;

    private Lock lock;

    public LockTreeElement() {
        this.requestQueue = new LinkedList<>();
    }
}
