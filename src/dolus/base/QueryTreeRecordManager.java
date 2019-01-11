package dolus.base;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;

/**
 * Is a record manager that manages the query records as a tree named Query Activation Tree.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueryTreeRecordManager<K,V> implements RecordManager<K, V, QueryTreeRecord<K, V>> {

    /**
     * Indicates at what depth in Query Activation Tree the record manager is
     */
    private int depth;

    /**
     * Points to the root of the Query Activation Tree
     */
    private QueryTreeRecord<K, V> root;

    /**
     * Points to the current active record
     */
    private QueryTreeRecord<K, V> current;

    /**
     * Keeps track of which child should be retrieved next from current active query
     */
    private Stack<Integer> accessChildrenStack;

    /**
     * Keeps track whether the record manager contains any record or not
     */
    private boolean empty;

    /**
     * QueryTreeRecordManager default constructor.
     * It sets the current depth to zero and initializes the internal variables of record manager
     *
     * @since 1.0
     */
    public QueryTreeRecordManager() {

        this.depth = 0;

        this.empty = true;

        this.accessChildrenStack = new Stack<>();

    }

    /**
     * Finds the value associated with the key parameter.
     * It asks the current record for the value. if it contains the key then manager returns the value.
     * Otherwise it asks parent of the current record. search continues until it reaches the root record.
     *
     * @param key record manager searches for its value
     * @return the value associated with key or null if not found
     * @since 1.0
     */
    @Override
    public V findSymbol(K key) {

        V value = this.current.findValue(key);

        if (value != null)
            return value;
        else {

            QueryRecord<K, V> parent = current.getPrevious();

            while (parent != null) {

                value = parent.findValue(key);

                if (value != null)
                    break;

                parent = parent.getPrevious();
            }

            return value;
        }

    }

    /**
     * Adds a new record to the query activation tree record.
     *
     * @since 1.0
     */
    @Override
    public void addRecord() {

        System.out.println("add record called");
        //if it's the first record, initialize the query activation tree
        if (this.empty) {

            this.root = new QueryTreeRecord<>();

            this.current = this.root;

            accessChildrenStack.push(0);

            this.empty = false;

            return;
        }

        //create a new record and set its parent to the current record
        QueryTreeRecord<K, V> record = new QueryTreeRecord<>(this.current);

        //add created record as a child to the current record
        this.current.addChild(record);

        //update the current record
        this.current = record;

        this.accessChildrenStack.push(0);

        //update current depth of the record manager
        this.depth++;

    }

    /**
     * Add a new symbol to symbol table of the current record
     *
     * @param key   key of new entry
     * @param value value of the new entry
     * @since 1.0
     */
    @Override
    public void addSymbol(K key, V value) {

        this.current.addSymbol(key, value);

    }

    /**
     * Causes the record manager to activate the previous record
     *
     * @since 1.0
     */
    public void decrementDepth() {

        System.out.println("decrement called");
        if (depth == 0)
            return;

        this.current = (QueryTreeRecord<K, V>) this.current.getPrevious();

        this.accessChildrenStack.pop();

        this.depth--;

    }

    /**
     * Causes the record manager to activate next child of the current record in Query Activation Tree.
     *
     * @since 1.0
     */
    public void incrementDepth() {

        int childIndex = this.accessChildrenStack.pop();

        QueryTreeRecord<K,V> child = this.current.getChild(childIndex);

        childIndex = childIndex + 1;

        accessChildrenStack.push(childIndex);

        //current record does not have any child record -> can not go deeper!
        if (child == null)
            return;

        this.current = child;

        //update the depth value
        this.depth++;

    }

    /**
     * Resets the record pointer of the record manager and points it to the root of the activation tree
     *
     * @since 1.0
     */
    public void resetDepth() {
        this.current = this.root;
        this.depth = 0;

        this.accessChildrenStack.clear();
        this.accessChildrenStack.push(0);
    }

    /**
     * Get root record of the query activation tree
     *
     * @return root record of the query activation tree
     * @since 1.0
     */
    protected QueryTreeRecord<K, V> getRoot() {
        return root;
    }

    /**
     * Get current active record of the query activation tree
     *
     * @return current active record of the query activation tree
     * @since 1.0
     */
    protected QueryTreeRecord<K, V> getCurrent() {
        return current;
    }

    /**
     * Get current depth of the query manager in the query activation tree
     *
     * @return depth of the query manager in the query activation tree
     */
    public int getDepth() {
        return depth;
    }

    /**
     * Check whether there are any records in the record manager
     *
     * @return true if there are no records in the record manager and false if there is at least one record
     * @since 1.0
     */
    protected boolean isEmpty() {
        return empty;
    }

    /**
     * Update depth of the record manager
     *
     * @param depth new value of the depth
     * @since 1.0
     */
    protected void setDepth(int depth) {
        this.depth = depth;
    }

    /**
     * Set root record of the record manager
     *
     * @param root root record of the record manager
     * @since 1.0
     */
    protected void setRoot(QueryTreeRecord<K, V> root) {
        this.root = root;
    }

    /**
     * Set current record pointer to the record specified in the param
     *
     * @param current record to be the new current record
     */
    protected void setCurrent(QueryTreeRecord<K, V> current) {
        this.current = current;
    }

    /**
     * Update the empty attribute
     *
     * @param empty value of the empty attribute
     * @since 1.0
     */
    protected void setEmpty(boolean empty) {
        this.empty = empty;
    }

    protected void pushChildIndex(){
        accessChildrenStack.push(0);
    }

    /**
     * Prints the Query Activation Tree row by row
     *
     * @return an snapshot of the records being manage by the query manager
     * @since 1.0
     */
    @Override
    public String toString() {

        //building the snapshot requires lots of string concatenation thus using StringBuilder
        StringBuilder snapshot = new StringBuilder();

        //current node to be printed
        QueryTreeRecord<K, V> current;

        //fifo queue to keep track of nodes of the activation tree that are going to be printed out
        LinkedList<QueryTreeRecord<K, V>> fifo = new LinkedList<>();

        //add root of the query activation tree
        fifo.add(this.root);

        //print all nodes in BFS form until there are no other nodes to print
        while (!fifo.isEmpty()) {
            current = fifo.remove();

            snapshot.append(current.toString());

            snapshot.append("\n\n");


            fifo.addAll(current.getChildren());
        }

        return snapshot.toString();
    }
}
