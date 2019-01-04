package dolus.common;

import java.util.Stack;

/**
 * Is a record manager that manages the query records as a tree named Query Activation Tree.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueryTreeRecordManager implements RecordManager<String, String, QueryTreeRecord<String,String>> {

    /**
     * Indicates at what depth in Query Activation Tree the record manager is
     */
    private int depth;

    /**
     * Points to the root of the Query Activation Tree
     */
    private QueryTreeRecord<String,String> root;

    /**
     * Points to the current active record
     */
    private QueryTreeRecord<String,String> current;

    /**
     * QueryTreeRecordManager default constructor.
     * It sets the current depth to zero and initializes the root and current record in the tree.
     */
    public QueryTreeRecordManager(){

        this.depth = 0;

        this.root = new QueryTreeRecord<>();

        this.current = this.root;

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
    public String findSymbol(String key) {

        String value = this.current.findValue(key);

        if (value != null)
            return value;
        else{

            QueryRecord<String,String> parent = current.getPrevious();

            while(parent != null){

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
     * @since 1.0
     */
    @Override
    public void addRecord() {

        //create a new record and set its parent to the current record
        QueryTreeRecord<String,String> record = new QueryTreeRecord<>(this.current);

        //add created record as a child to the current record
        this.current.addChild(record);

        //update the current record
        this.current = record;

        //update current depth of the record manager
        this.depth++;

    }

    /**
     * Add a new symbol to symbol table of the current record
     * @param key key of new entry
     * @param value value of the new entry
     * @since 1.0
     */
    @Override
    public void addSymbol(String key, String value) {

        this.current.addSymbol(key,value);

    }

    /**
     * Causes the record manager to activate the previous record
     *
     * @throws IllegalStateException if current depth of the record manager is zero
     * @since 1.0
     */
    public void decrementDepth(){

        if (depth == 0)
            throw new IllegalStateException("Depth can not be smaller than zero");

        this.current = (QueryTreeRecord<String, String>) this.current.getPrevious();

        this.depth--;

    }

    /**
     * Causes the record manager to activate next child of the current record in Query Activation Tree.
     *
     * @throws IndexOutOfBoundsException if current record has no other children to access
     * @since 1.0
     */
    public void incrementDepth(){

        this.current = this.current.getChild();

        //update the depth value
        this.depth++;

    }
}
