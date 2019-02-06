package config;

import common.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Contains information about MongoDB storage
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MongoDBConfigContainer extends StorageConfigContainer {

    /**
     * Component name to use in the logging system
     */
    private static final String componentName = "MongoDBConfigContainer";

    /**
     * Contains collection names
     */
    private HashSet<String> collections;

    /**
     * Contains mapping between collection names and their primary keys
     */
    private HashMap<String, String> primaryKeys;

    /**
     * Wraps a MongoDBConfigContainer object around the configs of a MongoDB storage
     *
     * @param collections set of collection names
     * @param primaryKeys mapping between collection names and their primary keys
     * @param id          unique identifier of this storage
     * @param engine      engine to use when querying data of this storage
     * @param parentId    parent id of this storage in storage graph
     * @param host        host address
     * @param port        port number
     * @param database    database name
     * @param username    username credentials
     * @param password    password credentials
     * @since 1.0
     */
    public MongoDBConfigContainer(HashSet<String> collections, HashMap<String, String> primaryKeys,
                                  String id, String engine, String parentId, String host, String port, String database, String username, String password) {

        super(id, engine, parentId, host, port, database, username, password);
        this.collections = collections;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Wraps a MongoDBConfigContainer object around the configs of a MongoDB storage
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
    public MongoDBConfigContainer(HashSet<String> collections, HashMap<String, String> primaryKeys,
                                  String id, String engine, String host, String port, String database, String username, String password) {

        super(id, engine, host, port, database, username, password);
        this.collections = collections;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public MongoDBConfigContainer() {
        super();

        this.collections = null;
        this.primaryKeys = null;
    }

    /**
     * Parses MongoDB configurations
     *
     * @param mongoDBConfigContainer  container to store extracted configurations in
     * @param mongoDBConfigJSONObject json object containing configurations
     * @param validEngines            valid engines
     * @param visitedIds              visited ids
     * @return true if parsing and storing configurations was successful, false otherwise
     */
    public static boolean parseMongoDBConfig(MongoDBConfigContainer mongoDBConfigContainer, JSONObject mongoDBConfigJSONObject, String[] validEngines, HashSet<String> visitedIds) {

        //first parse basic configurations
        boolean successful = StorageConfigContainer.parseStorageConfig(mongoDBConfigContainer, mongoDBConfigJSONObject, validEngines, visitedIds);

        //make sure parsing basic configurations was successful
        if (!successful) {
            Log.log("MySQL storage config parsing failed", componentName, Log.ERROR);
            return false;
        }

        //get id of the current storage(is used for logging more helpful messages)
        String id = mongoDBConfigContainer.getId();

        //set of all collection names in mongodb database
        HashSet<String> collections = new HashSet<>();

        //map each collection name with its primary key
        HashMap<String, String> primaryKeys = new HashMap<>();

        //get array of collections present in the storage
        JSONArray collectionArray = (JSONArray) mongoDBConfigJSONObject.get("collections");

        if (collectionArray == null || collectionArray.size() == 0) {
            Log.log("For MongoDB storage: " + id + ", collections attribute is not defined or it does not contain any collection", componentName, Log.ERROR);
            return false;
        }

        //for each table, extract its meta data and store it
        for (Object collectionObject : collectionArray) {

            JSONObject collectionJsonObject = (JSONObject) collectionObject;

            /////////////////// collection name //////////////////

            //extract collection name
            String collectionName = (String) collectionJsonObject.get("name");

            //make sure collection name is specified
            if (collectionName == null || collectionName.length() == 0) {
                Log.log("For MongoDB storage: " + id + ", collection name attribute is not defined for one of its collections", componentName, Log.ERROR);
                return false;
            }

            /////////////////// primary key //////////////////

            //extract collection primary key
            String collectionPrimaryKey = (String) collectionJsonObject.get("pk");

            //if primary key is not defined or is empty -> set it to default mongoDB primary kry : _id
            if (collectionPrimaryKey == null || collectionPrimaryKey.length() == 0)
                collectionPrimaryKey = "_id";

            //add extracted collection to set of collections
            collections.add(collectionName);

            //map current collection to its primary key
            primaryKeys.put(collectionName, collectionPrimaryKey);
        }

        //store set of collections in container
        mongoDBConfigContainer.setCollections(collections);

        //store primary keys in container
        mongoDBConfigContainer.setPrimaryKeys(primaryKeys);

        return true;

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

    /**
     * Set collections set
     *
     * @param collections set of collections
     * @since 1.0
     */
    public void setCollections(HashSet<String> collections) {
        this.collections = collections;
    }

    /**
     * Set primary keys
     *
     * @param primaryKeys mapping between collection name and its primary key
     * @since 1.0
     */
    public void setPrimaryKeys(HashMap<String, String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
