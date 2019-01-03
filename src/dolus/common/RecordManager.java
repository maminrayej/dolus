package dolus.common;

/**
 * Record Manager manages records in a data structure.
 * Each record contains a symbol table storing symbols and their values.
 * <P>
 *     K parameter indicates the type of the keys in symbol table.
 *     V parameter indicates the type of the values in symbol table.
 *     T parameter indicates the data structure of records.
 * </P>
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public interface RecordManager<K, V, T extends QueryRecord<K,V>> {

    /**
     * Record Manager searches for the value associated with the key parameter
     * Policy of this searching is up to the class.
     *
     * @param key which record manager searches for its value
     * @return the value associated with the key if found, otherwise null
     * @since 1.0
     */
    V findSymbol(K key);

    /**
     * Adds a new record to the Record Manager
     * @param record record to be inserted
     * @since 1.0
     */
    void addRecord(T record);

    /**
     * Add a new symbol to the symbol table of the current active record
     *
     * @param key key of new entry
     * @param value value of the new entry
     * @since 1.0
     */
    void addSymbol(K key, V value);

}
