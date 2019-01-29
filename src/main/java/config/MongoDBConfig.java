package config;

import common.Log;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Contains information about MongoDB database
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MongoDBConfig extends DatabaseConfig{

    /**
     * Component name of the logging system
     */
    private static final String componentName = "MongoDBConfig";

    /**
     * Contains collection names
     */
    private HashSet<String> collections;

    /**
     * Contains mapping between collection name and its primary key
     */
    private HashMap<String, String> primaryKeys;

    /**
     * Wraps a MongoDBConfig object around the configs of a MongoDB database
     *
     * @param collections set of collection names
     * @param primaryKeys mapping between collection name and its primary key
     * @param host        host address
     * @param port        port number
     * @param database    database name
     * @param username    username credentials
     * @param password    password credentials
     * @since 1.0
     */
    public MongoDBConfig(HashSet<String> collections, HashMap<String,String> primaryKeys, String host, String port, String database, String username, String password) {

        super(host, port, database, username, password);
        this.collections = collections;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Checks whether mongoDB contains specified collection or not
     *
     * @param collectionName name of the collection
     * @return true if database contains the collection, false otherwise
     * @since 1.0
     */
    public boolean containsCollection(String collectionName) {

        return this.collections.contains(collectionName);
    }

    /**
     * Return primary key of the specified collection
     *
     * @param collectionName name of the collection
     * @return PK of the table
     * @since 1.0
     */
    public String getPrimaryKey(String collectionName) {

        if (!primaryKeys.containsKey(collectionName)) {
            Log.log(String.format("Collection %s is not defined in MongoDB database", collectionName), componentName, Log.ERROR);
            return null;
        }

        return primaryKeys.get(collectionName);
    }

}
