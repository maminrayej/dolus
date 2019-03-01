package manager.lock;

import java.util.HashMap;

public class LockTreeTableElement extends LockTreeElement {

    private HashMap<Integer,LockTreeElement> records;

    public LockTreeTableElement() {

        this.records = new HashMap<>();
    }
}
