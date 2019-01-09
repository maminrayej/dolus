package dolus.base;

import java.util.LinkedList;

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
     * Keeps track whether the inserted record is the first record or not
     */
    private boolean firstRecord;

    /**
     * QueryTreeRecordManager default constructor.
     * It sets the current depth to zero and initializes the internal variables of record manager
     *
     * @since 1.0
     */
    public QueryTreeRecordManager() {

        this.depth = 0;

        this.firstRecord = true;

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

        //if it's the first record initialize the query activation tree
        if (this.firstRecord) {

            this.root = new QueryTreeRecord<>();

            this.current = this.root;

            this.firstRecord = false;

            return;
        }

        //create a new record and set its parent to the current record
        QueryTreeRecord<K, V> record = new QueryTreeRecord<>(this.current);

        //add created record as a child to the current record
        this.current.addChild(record);

        //update the current record
        this.current = record;

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

        if (depth == 0)
            return;

        this.current = (QueryTreeRecord<K, V>) this.current.getPrevious();

        this.depth--;

    }

    /**
     * Causes the record manager to activate next child of the current record in Query Activation Tree.
     *
     * @since 1.0
     */
    public void incrementDepth() {

        QueryTreeRecord<K,V> child = this.current.getChild();

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
