package dolus.common;

/**
 * Record Manager manages records in a record tree.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public interface RecordManager<T extends QueryRecord> {

    /**
     * Record Manager searches for the table name associated with the nickname parameter
     * Policy of this searching is up to the class.
     *
     * @param nickname key which record manager searches for its value
     * @return the table name associated with the nickname if found, otherwise null
     * @since 1.0
     */
    String findTableName(String nickname);

    /**
     * Adds a new record to the Record Manager
     * @param record record to be inserted
     * @since 1.0
     */
    void   addRecord(T record);

}
