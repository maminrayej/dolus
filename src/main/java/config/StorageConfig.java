package config;

import common.Log;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Contains configuration of a storage
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public abstract class StorageConfig {

    /**
     * Component name to use in logging system
     */
    private static final String componentName = "StorageConfig";

    /**
     * Unique id of the storage
     */
    private String id;

    /**
     * Unique parent id of this storage
     */
    private String parentId;

    /**
     * Engine to use when querying data of the storage
     */
    private String engine;

    /**
     * Parent storage of this storage in storage graph
     */
    private StorageConfig parent;

    /**
     * List of children storage belonging to this storage
     */
    private List<StorageConfig> children;

    /**
     * Host address of storage
     */
    private String host;

    /**
     * Port number which storage in listening on
     */
    private String port;

    /**
     * Name of the database
     */
    private String database;


    /**
     * Credentials
     */
    private String username;
    private String password;

    /**
     * Wraps a StorageConfig object around the configs of a storage system
     *
     * @param id       unique identifier of this storage
     * @param engine   engine to use when querying data of this storage
     * @param parentId parent id of this storage in storage graph
     * @param host     host address of storage
     * @param port     port number of storage
     * @param database storage name
     * @param username username credential
     * @param password password credential
     * @since 1.0
     */
    public StorageConfig(String id, String engine, String parentId, String host, String port, String database, String username, String password) {

        this.id = id;
        this.engine = engine;
        this.parentId = parentId;
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;

        this.children = new ArrayList<>();

    }

    /**
     * Wraps a StorageConfig object around the configs of a storage
     *
     * @param id       unique identifier of this storage
     * @param engine   engine to use when querying data of this storage
     * @param host     host address of storage
     * @param port     port number of storage
     * @param database storage name
     * @param username username credential
     * @param password password credential
     * @since 1.0
     */
    public StorageConfig(String id, String engine, String host, String port, String database, String username, String password){
        this(id, engine, null, host, port, database, username, password);
    }

    public StorageConfig(){
        this(null, null, null, null, null, null, null, null);
    }

    /**
     * Host address of the storage
     *
     * @return host address of the storage
     * @since 1.0
     */
    public String getHost() {
        return host;
    }

    /**
     * Port number of the storage
     *
     * @return port of the storage
     * @since 1.0
     */
    public String getPort() {
        return port;
    }

    /**
     * Database name
     *
     * @return name of the database
     * @since 1.0
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Username credential
     *
     * @return username credential
     * @since 1.0
     */
    public String getUsername() {
        return username;
    }

    /**
     * Password credential
     *
     * @return password credential
     * @since 1.0
     */
    public String getPassword() {
        return password;
    }

    /**
     * Storage Identifier
     *
     * @return id of this storage
     * @since 1.0
     */
    public String getId(){
        return this.id;
    }

    /**
     * Parent id of this storage
     *
     * @return id of parent storage
     * @since 1.0
     */
    public String getParentId(){
        return this.parentId;
    }

    /**
     * Query Engine
     *
     * @return engine name to use when querying data of this storage
     * @since 1.0
     */
    public String getEngine(){
        return this.engine;
    }

    /**
     * Parent storage
     *
     * @return parent storage of this storage in storage graph
     * @since 1.0
     */
    public StorageConfig getParent(){
        return this.parent;
    }

    /**
     * Set parent storage
     *
     * @param storageConfig parent of this storage in storage graph
     * @since 1.0
     */
    public void setParent(StorageConfig storageConfig){
        this.parent = storageConfig;
    }

    /**
     * Adds a child to the list of children
     *
     * @param child child storage belonging to this storage
     * @since 1.0
     */
    public void addChild(StorageConfig child){
        this.children.add(child);
    }

    /**
     * List of children storage
     *
     * @return list of children storage belonging to this storage
     * @since 1.0
     */
    public List<StorageConfig> getChildren(){
        return this.children;
    }

    /**
     * Searches for the named collection in the storage configuration
     *
     * @param collectionName name of the collection
     * @return true if storage contains a collection with specified name, false otherwise
     * @since 1.0
     */
    public abstract boolean containsCollection(String collectionName);

    /**
     * Searches for an attribute in the named collection
     *
     * @param collectionName name of the collection
     * @param attributeName name of the attribute to search for
     * @return true if collection has the attribute, false otherwise
     * @since 1.0
     */
    public abstract boolean containsAttribute(String collectionName, String attributeName);

    /**
     * Get primary key of the name collection
     *
     * @param collectionName name of the collection
     * @return name of the attribute which is primary key of the named collection, null otherwise
     * @since 1.0
     */
    public abstract String getPrimaryKey(String collectionName);

    /**
     * Set id of this storage
     *
     * @param id id of this storage
     * @since 1.0
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Set parent id of this storage
     *
     * @param parentId parent id of this storage
     * @since 1.0
     */
    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    /**
     * Set engine to use when querying data of this storage
     *
     * @param engine engine to use when querying data of this storage
     * @since 1.0
     */
    public void setEngine(String engine) {
        this.engine = engine;
    }

    /**
     * Set host of this storage
     *
     * @param host host of this storage
     * @since 1.0
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     *
     *
     * @param port port of this storage
     * @since 1.0
     */
    public void setPort(String port) {
        this.port = port;
    }

    /**
     * Set database of this storage
     *
     * @param database database of this storage
     * @since 1.0
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Set username of this storage
     *
     * @param username username of this storage
     * @since 1.0
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Set password of this storage
     *
     * @param password password of this storage
     * @since 1.0
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public static boolean parseStorageConfig(StorageConfig storageConfig, JSONObject storageConfigJsonObject, String[] validEngines, HashSet<String> visitedIds){

        //storage configurations
        String id;
        String parentId;
        String engine;
        String host;
        String port;
        String database;
        String username;
        String password;

        //configuration extraction
        parentId = (String) storageConfigJsonObject.get("parent");

        if (parentId == null || parentId.length() == 0)
            parentId = null;

        storageConfig.setParentId(parentId);

        id = (String) storageConfigJsonObject.get("id");
        if (id == null || id.length() == 0) {
            Log.log("Storage id attribute is not defined", componentName, Log.ERROR);
            return false;
        }

        //make sure visited id is unique
        if (visitedIds.contains(id)){
            Log.log("Storage id: " + id + " is used before", componentName, Log.ERROR);
            return false;
        }

        //if id is unique add it to visited ids
        visitedIds.add(id);

        engine = (String) storageConfigJsonObject.get("engine");
        if (engine == null || engine.length() == 0) {
            Log.log("Storage engine attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        //make sure specified engine is valid
        boolean isValid = false;
        for (String validEngine : validEngines)
            if (validEngine.equals(engine)){
                isValid = true;
                break;
            }
        if (!isValid){
            Log.log("Specified engine: " + engine + " is not a valid query engine for: " + id, componentName, Log.ERROR);
            return false;
        }

        host = (String) storageConfigJsonObject.get("host");
        if (host == null || host.length() == 0) {
            Log.log("Storage host attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        port = (String) storageConfigJsonObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)) {
            Log.log("Storage port attribute is not defined or is not a valid integer number for: " + id, componentName, Log.ERROR);
            return false;
        }

        database = (String) storageConfigJsonObject.get("database");
        if (database == null || database.length() == 0) {
            Log.log("Storage database name attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        username = (String) storageConfigJsonObject.get("username");
        if (username == null || username.length() == 0) {
            Log.log("Storage username attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        password = (String) storageConfigJsonObject.get("password");
        if (password == null || password.length() == 0) {
            Log.log("Storage password attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        return true;
    }


    /**
     * Checks if the specified string is a integer in form of [0-9]+
     *
     * @param integer string integer
     * @return true if string is a valid integer, false otherwise
     * @since 1.0
     */
    private static boolean isInteger(String integer) {

        for (int i = 0; i < integer.length(); i++)
            if (!Character.isDigit(integer.charAt(i)))
                return false;

        return true;
    }
}
