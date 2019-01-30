package config;

import common.Log;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Contains information about MongoDB storage
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MongoDBConfig extends StorageConfig {

    /**
     * Component name to use in the logging system
     */
    private static final String componentName = "MongoDBConfig";

    /**
     * Contains collection names
     */
    private HashSet<String> collections;

    /**
     * Contains mapping between collection names and their primary keys
     */
    private HashMap<String, String> primaryKeys;

    /**
     * Wraps a MongoDBConfig object around the configs of a MongoDB storage
     *
     * @param collections set of collection names
     * @param primaryKeys mapping between collection names and their primary keys
     * @param id          unique identifier of this storage
     * @param engine      engine to use when querying data of this storage
     * @param parent      parent of this storage in storage graph
     * @param host        host address
     * @param port        port number
     * @param database    database name
     * @param username    username credentials
     * @param password    password credentials
     * @since 1.0
     */
    public MongoDBConfig(HashSet<String> collections, HashMap<String,String> primaryKeys,
                         String id, String engine, StorageConfig parent, String host, String port, String database, String username, String password) {

        super(id, engine, parent, host, port, database, username, password);
        this.collections = collections;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Wraps a MongoDBConfig object around the configs of a MongoDB storage
     *
     * @param collections set of collection names
     * @param primaryKeys mapping between collection names and their primary keys
     * @param id          unique identifier of this storage
     * @param engine      engine to use when querying data of this storage
     * @param host        host address
     * @param port        port number
     * @param database    database name
     * @param username    username credentials
     * @param password    password credentials
     * @since 1.0
     */
    public MongoDBConfig(HashSet<String> collections, HashMap<String,String> primaryKeys,
                         String id, String engine, String host, String port, String database, String username, String password) {

        super(id, engine, host, port, database, username, password);
        this.collections = collections;
        this.primaryKeys = primaryKeys;

    }

    public boolean containsCollection(String collectionName) {

        return this.collections.contains(collectionName);
    }

    @Override
    public boolean containsAttribute(String collectionName, String attributeName) {
        return true;
    }

    public String getPrimaryKey(String collectionName) {

        return primaryKeys.get(collectionName);
    }

}
