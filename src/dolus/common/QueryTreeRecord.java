package dolus.common;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * QueryTreeRecord nodes act as a tree node.
 * Each node has access to its parent and children.
 * Children are indexed from left to right starting with zero.
 * Children of a QueryTreeNode can be accessed with their index.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueryTreeRecord<K,V> extends QueryRecord<K,V> {

    /**
     * Contains the record of the queries nested in the current query
     */
    private List<QueryTreeRecord<K,V>> children;

    /**
     * Default constructor of the QueryTreeRecord.
     * It sets the parent of the current record to null and initializes the symbol table and children list.
     *
     * @since 1.0
     */
    public QueryTreeRecord(){

        this(null);
    }

    /**
     * It sets the parent of the current record and initializes the symbol table and children list
     * @param parent previous active query.
     *
     * @since 1.0
     */
    public QueryTreeRecord(QueryTreeRecord<K,V> parent){
        super(parent, new HashMap<>());

        this.children = new ArrayList<>();
    }

    /**
     * Add a child query record to the current query record
     *
     * @param child record of a query nested in the current query
     * @since 1.0
     */
    public void addChild(QueryTreeRecord<K,V> child){
        children.add(child);
    }

    /**
     * Get record of a nested query in the current query
     *
     * @param index position of the nested query in the Query Activation Tree
     * @return record of the nested query in the position specified by index
     * @throws IndexOutOfBoundsException if index < 0 or index >= size()
     * @since 1.0
     */
    public QueryTreeRecord<K,V> getChild(int index){
        return children.get(index);
    }

}
