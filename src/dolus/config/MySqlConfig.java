package dolus.config;

import dolus.common.Log;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Contains information about MySQL database
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MySqlConfig extends DatabaseConfig{

    /**
     * Component name of the logging system
     */
    private static final String componentName = "MySqlConfig";

    /**
     * Contains information about each table
     */
    private HashMap<String, HashSet<String>> tablesInfo;

    /**
     * Contains mapping between table name and its primary key
     */
    private HashMap<String, String> primaryKeys;

    /**
     * Wraps a MySqlConfig object around the configs of a MySQL database
     *
     * @param tablesInfo  information about tables of the database
     * @param primaryKeys mapping between table name and its primary key
     * @param host        host address of database
     * @param port        port number of database
     * @param database    database name
     * @param username    username credential
     * @param password    password credential
     * @since 1.0
     */
    public MySqlConfig(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                       String host, String port, String database, String username, String password) {

        super(host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

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
            Log.log(String.format("Table %s is not defined in MySQL database", tableName), componentName, Log.ERROR);
            return false;
        }

        return tablesInfo.get(tableName).contains(columnName);
    }

    /**
     * Return primary key of the specified table
     *
     * @param tableName name of the table
     * @return PK of the table
     * @since 1.0
     */
    public String getPrimaryKey(String tableName) {

        if (!primaryKeys.containsKey(tableName)) {
            Log.log(String.format("Table %s is not defined in MySQL database", tableName), componentName, Log.ERROR);
            return null;
        }

        return primaryKeys.get(tableName);
    }

}
