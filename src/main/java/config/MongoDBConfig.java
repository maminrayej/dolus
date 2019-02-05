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
     * @param parentId    parent id of this storage in storage graph
     * @param host        host address
     * @param port        port number
     * @param database    database name
     * @param username    username credentials
     * @param password    password credentials
     * @since 1.0
     */
    public MongoDBConfig(HashSet<String> collections, HashMap<String,String> primaryKeys,
                         String id, String engine, String parentId, String host, String port, String database, String username, String password) {

        super(id, engine, parentId, host, port, database, username, password);
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

    public MongoDBConfig(){
        super();

        this.collections = null;
        this.primaryKeys = null;
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

    public void setCollections(HashSet<String> collections) {
        this.collections = collections;
    }

    public void setPrimaryKeys(HashMap<String, String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public static boolean parseMongoDBConfig(MongoDBConfig mongoDBConfig, JSONObject mongoDBConfigJSONObject, String[] engines, HashSet<String> visitedIds) {

        boolean successful = StorageConfig.parseStorageConfig(mongoDBConfig, mongoDBConfigJSONObject, engines, visitedIds);

        if (!successful){
            Log.log("MySQL storage config parsing failed", componentName, Log.ERROR);
            return false;
        }

        String id = mongoDBConfig.getId();

        //storage meta data
        HashSet<String> collections = new HashSet<>();//set of all collection names in mongodb database
        HashMap<String, String> primaryKeys = new HashMap<>();//map each collection name with its primary key

        //meta data extraction
        JSONArray collectionArray = (JSONArray) mongoDBConfigJSONObject.get("collections");
        if (collectionArray == null || collectionArray.size() == 0) {
            Log.log("MongoDB collections attribute is not defined or it does not contain any collection for: " + id, componentName, Log.ERROR);
            return false;
        }

        //meta data holders
        String collectionName;
        JSONObject collection;

        //for each collection extract its name and primary key
        for (Object collectionObject : collectionArray) {

            collection = (JSONObject) collectionObject;

            //extract collection name
            collectionName = (String) collection.get("name");

            if (collectionName == null || collectionName.length() == 0) {
                Log.log("MongoDB collection name attribute is not defined for one of its collections for: " + id, componentName, Log.ERROR);
                return false;
            }

            //extract collection primary key
            String collectionPrimaryKey = (String) collection.get("pk");

            //if primary key is not defined or is empty -> set it to default mongoDB primary kry : _id
            if (collectionPrimaryKey == null || collectionPrimaryKey.length() == 0)
                collectionPrimaryKey = "_id";

            collections.add(collectionName);

            primaryKeys.put(collectionName, collectionPrimaryKey);
        }

        mongoDBConfig.setCollections(collections);
        mongoDBConfig.setPrimaryKeys(primaryKeys);

        return true;

    }
}
