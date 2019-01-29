package config;

import common.Log;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Contains information about MySQL storage
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MySqlConfig<T extends StorageConfig> extends StorageConfig<T> {

    /**
     * Component name to use in the logging system
     */
    private static final String componentName = "MySqlConfig";

    /**
     * Contains information about each table
     * it is a map between table name and its columns : table name -> [column1, column2 ,...]
     */
    private HashMap<String, HashSet<String>> tablesInfo;

    /**
     * Contains mapping between table names and their primary keys
     */
    private HashMap<String, String> primaryKeys;

    /**
     * Wraps a MySqlConfig object around configs of a MySQL storage
     *
     * @param tablesInfo  information about tables of the storage
     * @param primaryKeys mapping between table names and their primary keys
     * @param id          unique identifier of this storage
     * @param engine      engine to use when querying data of this storage
     * @param parent      parent of this storage in storage graph
     * @param host        host address of storage
     * @param port        port number of storage
     * @param database    database name
     * @param username    username credential
     * @param password    password credential
     * @since 1.0
     */
    public MySqlConfig(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                       String id, String engine, T parent, String host, String port, String database, String username, String password) {

        super(id, engine, parent, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

        Log.log(String.format("MySQL Storage: %s is registered with parent: %s", id, parent.getId()), componentName, Log.INFORMATION);

    }

    /**
     * Wraps a MySqlConfig object around configs of a MySQL storage
     *
     * @param tablesInfo  information about tables of the storage
     * @param primaryKeys mapping between table names and their primary keys
     * @param id          unique identifier of this storage
     * @param engine      engine to use when querying data of this storage
     * @param host        host address of storage
     * @param port        port number of storage
     * @param database    database name
     * @param username    username credential
     * @param password    password credential
     * @since 1.0
     */
    public MySqlConfig(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                       String id, String engine, String host, String port, String database, String username, String password) {

        super(id, engine, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

        Log.log(String.format("MySQL Storage: %s is registered", id), componentName, Log.INFORMATION);

    }

    /**
     * Check whether specified column exists in specified table or not
     *
     * @param tableName  name of the table
     * @param columnName name of the column to search for
     * @return true if table contains the specified column, false if table does not exist or column did not found
     * @since 1.0
     */
    public boolean containsColumn(String tableName, String columnName) {

        if (!tablesInfo.containsKey(tableName)) {
            Log.log(String.format("Table %s is not defined in MySQL storage", tableName), componentName, Log.ERROR);
            return false;
        }

        return tablesInfo.get(tableName).contains(columnName);
    }

    /**
     * Return primary key of the specified table
     *
     * @param tableName name of the table
     * @return PK of the table or null if table does not exist
     * @since 1.0
     */
    public String getPrimaryKey(String tableName) {

        if (!primaryKeys.containsKey(tableName)) {
            Log.log(String.format("Table %s is not defined in MySQL storage", tableName), componentName, Log.ERROR);
            return null;
        }

        return primaryKeys.get(tableName);
    }

    /**
     * Check whether MySQL storage contains the table or not
     *
     * @param tableName name of the table
     * @return true if MySQL contains the table, false otherwise
     */
    public boolean containsTable(String tableName){
        return tablesInfo.containsKey(tableName);
    }

}
