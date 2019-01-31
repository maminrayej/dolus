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
public class MySqlConfig extends StorageConfig {

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
     * @param parentId    parent id of this storage in storage graph
     * @param host        host address of storage
     * @param port        port number of storage
     * @param database    database name
     * @param username    username credential
     * @param password    password credential
     * @since 1.0
     */
    public MySqlConfig(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                       String id, String engine, String parentId, String host, String port, String database, String username, String password) {

        super(id, engine, parentId, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

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

    }

    @Override
    public boolean containsAttribute(String collectionName, String attributeName) {

        if (!tablesInfo.containsKey(collectionName)) {
            return false;
        }

        return tablesInfo.get(collectionName).contains(attributeName);
    }

    @Override
    public String getPrimaryKey(String collectionName) {

        return primaryKeys.get(collectionName);
    }

    @Override
    public boolean containsCollection(String collectionName) {

        return tablesInfo.containsKey(collectionName);
    }
}
