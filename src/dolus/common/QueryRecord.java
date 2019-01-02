package dolus.common;

import java.util.HashMap;

/**
 * Query Record contains information about tables names and nicknames associated with the current active query.
 * <p>
 *     Query Record instances store table nicknames as keys and table names as values.
 *     Record can be queried for a table name providing the table nickname.
 * </p>
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueryRecord {

    /**
     * Contains a reference to the previous active query which current query is embedded in
     */
    private QueryRecord parent;

    /**
     * Stores the (table nickname => table name) mapping
     */
    private HashMap<String,String> nicknameToName;

    /**
     * Represents the depth of current query in the query activation tree
     */
    private int depth;

    /**
     * Query Record default constructor.
     * Use this to construct the root node in query activation tree
     * @since 1.0
     */
    public QueryRecord(){

        this(null, 0);
    }

    /**
     * This Constructor is used to construct non-root nodes in the query activation tree
     *
     * @param parent query record which was active before current query
     * @param depth  depth of the current query in query activation tree
     * @since 1.0
     */
    public QueryRecord(QueryRecord parent, int depth){

        this.depth = depth;
        this.parent = parent;

        //initialize the mapping structure
        this.nicknameToName = new HashMap<>();
    }

    /**
     * Searches for the table name associated with nickname parameter in mapping structure
     * @param nickname nickname of the table
     * @return table name if available otherwise null
     * @since 1.0
     */
    public String findTableName(String nickname){

        return nicknameToName.getOrDefault(nickname, null);
    }

    /**
     * Add a new entry to the mapping table of the current query
     * @param tableName value of the entry
     * @param tableNickname key of the entry
     * @since 1.0
     */
    public void addMapping(String tableName, String tableNickname){

        nicknameToName.put(tableNickname, tableName);
    }

    /**
     * Get the depth of the current query in query activation tree
     * @return depth of the current active query
     * @since 1.0
     */
    public int getDepth(){
        return this.depth;
    }

    /**
     * Get previous active query record
     * @return previous active query record
     * @since 1.0
     */
    public QueryRecord getParent(){
        return this.parent;
    }
}
