package manager.lock;

import java.util.HashMap;

public class LockTreeDatabaseElement extends LockTreeElement {

    private HashMap<Integer,LockTreeElement> tables;

    public LockTreeDatabaseElement() {

        this.tables = new HashMap<>();
    }

}
