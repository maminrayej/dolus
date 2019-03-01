package manager.lock;

import java.util.HashMap;

public class LockManager {

    private HashMap<String,LockTreeDatabaseElement> lockTree;

    public LockManager() {

        lockTree = new HashMap<>();
    }

    public synchronized void lock() {

    }

    public synchronized void unlock() {

    }
}
