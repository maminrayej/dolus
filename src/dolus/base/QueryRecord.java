package dolus.base;

import java.util.Map;

/**
 * Query Record contains information about symbols associated with the current active query.
 * <p>
 * Query Record instances store query symbols in a key value structure.
 * Record can be queried for a value providing the key.
 * Record is linked to the previous active record.
 * </p>
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class QueryRecord<K, V> {

    /**
     * Contains a reference to the previous active query which current query is embedded in
     */
    private QueryRecord<K, V> previous;

    /**
     * Stores query symbol mappings
     */
    private Map<K, V> symbolTable;


    /**
     * Query Record default constructor.
     *
     * @since 1.0
     */
    public QueryRecord() {

        this(null, null);
    }

    /**
     * @param previous    query record which was active before current query
     * @param symbolTable mapping between a symbol and its value
     * @since 1.0
     */
    public QueryRecord(QueryRecord<K, V> previous, Map<K, V> symbolTable) {

        this.previous = previous;
        this.symbolTable = symbolTable;
    }

    /**
     * Searches for the value associated with key parameter in mapping structure
     *
     * @param key key of the symbol table
     * @return value if available otherwise null
     * @throws IllegalStateException if the symbol table is not initialized
     * @since 1.0
     */
    public V findValue(K key) {

        if (symbolTable == null)
            throw new IllegalStateException("Symbol table is not initialized");

        return symbolTable.get(key);
    }

    /**
     * Add a new entry to the symbol table of the current query.
     * If symbol table already has the key, old value will be replaced by new value
     *
     * @param value value of the entry
     * @param key   key of the entry
     * @since 1.0
     */
    public void addSymbol(K key, V value) {

        symbolTable.put(key, value);
    }

    /**
     * Get previous active query record
     *
     * @return previous active query record
     * @since 1.0
     */
    public QueryRecord getPrevious() {
        return this.previous;
    }

    /**
     * Displays an snapshot of the record current values
     *
     * @return string representation of the internal values of the record
     * @since 1.0
     */
    @Override
    public String toString() {
        return String.format("{previous: %s\n" +
                "symbols: %s}", this.previous, this.symbolTable);
    }

}
