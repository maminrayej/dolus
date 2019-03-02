package manager.lock;

/**
 * This class represents a lock
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class Lock {

    /**
     * name of the database to be locked
     */
    private String database;

    /**
     * name of the table to be locked
     */
    private String table;

    /**
     * name of the record to be locked
     */
    private Integer record;

    /**
     * type of the lock
     */
    private int type;

    /**
     * Lock constructor for record level locking
     *
     * @param database name of the database to be locked
     * @param table    name of the table to be locked
     * @param record   name of the record to be locked
     * @param type     type of the lock
     * @since 1.0
     */
    public Lock(String database, String table, Integer record, int type) {
        this.database = database;
        this.table = table;
        this.record = record;
        this.type = type;
    }

    /**
     * Lock constructor for table level locking
     *
     * @param database name of the database to be locked
     * @param table    name of the table to be locked
     * @param type     type of the lock
     * @since 1.0
     */
    public Lock(String database, String table, int type) {

        this(database, table, null, type);
    }

    /**
     * Lock constructor for database level locking
     *
     * @param database name of the database to be locked
     * @param type     type of the lock
     * @since 1.0
     */
    public Lock(String database, int type) {

        this(database, null, null, type);
    }

    /**
     * Get database name
     *
     * @return database name
     * @since 1.0
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Set database name
     *
     * @param database database name
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Get table name
     *
     * @return table name
     * @since 1.0
     */
    public String getTable() {
        return table;
    }

    /**
     * Set table name
     *
     * @param table table name
     * @since 1.0
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Get record id
     *
     * @return record id
     * @since 1.0
     */
    public Integer getRecord() {
        return record;
    }

    /**
     * Set record id
     *
     * @param record record id
     * @since 1.0
     */
    public void setRecord(Integer record) {
        this.record = record;
    }

    /**
     * Get type of the lock
     *
     * @return type of the lock
     * @since 1.0
     */
    public int getType() {
        return type;
    }

    /**
     * Set type of lock
     *
     * @param type type of the lock
     * @since 1.0
     */
    public void setType(int type) {
        this.type = type;
    }
}
