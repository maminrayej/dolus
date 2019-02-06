package config;

import common.Log;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * This class contains basic configurations of a storage
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public abstract class StorageConfigContainer {

    /**
     * Component name to use in logging system
     */
    private static final String componentName = "StorageConfigContainer";

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
    private StorageConfigContainer parent;

    /**
     * List of child storage systems belonging to this storage
     */
    private List<StorageConfigContainer> children;

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
     * Wraps a StorageConfigContainer object around the configs of a storage system
     *
     * @param id       unique identifier of this storage
     * @param engine   engine to use when querying data of this storage
     * @param parentId parent id of this storage in storage graph
     * @param host     host address of storage
     * @param port     port number of storage
     * @param database database name
     * @param username username credential
     * @param password password credential
     * @since 1.0
     */
    public StorageConfigContainer(String id, String engine, String parentId, String host, String port, String database, String username, String password) {

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
     * Wraps a StorageConfigContainer object around the configs of a storage
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
    public StorageConfigContainer(String id, String engine, String host, String port, String database, String username, String password) {
        this(id, engine, null, host, port, database, username, password);
    }

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public StorageConfigContainer() {
        this(null, null, null, null, null, null, null, null);
    }

    /**
     * Parses basic configurations of a storage
     *
     * @param storageConfigContainer  container to put the extracted information in
     * @param storageConfigJsonObject json object containing configuration
     * @param validEngines            valid engines
     * @param visitedIds              visited ids
     * @return true if parsing and storing configs was successful, false otherwise
     * @since 1.0
     */
    public static boolean parseStorageConfig(StorageConfigContainer storageConfigContainer, JSONObject storageConfigJsonObject, String[] validEngines, HashSet<String> visitedIds) {

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

        ////////////// parent ID ////////////
        parentId = (String) storageConfigJsonObject.get("parent");

        //if parent Id is not specified, set it to null
        if (parentId == null || parentId.length() == 0)
            parentId = null;

        ////////////// storage ID ////////////
        id = (String) storageConfigJsonObject.get("id");

        //make sure storage id is specified
        if (id == null || id.length() == 0) {
            Log.log("Storage id attribute is not defined", componentName, Log.ERROR);
            return false;
        }

        //make sure storage id is unique
        if (visitedIds.contains(id)) {
            Log.log("Storage id: " + id + " is used before", componentName, Log.ERROR);
            return false;
        }

        //if id is unique add it to visited ids
        visitedIds.add(id);

        ////////////// engine ////////////
        engine = (String) storageConfigJsonObject.get("engine");

        //make sure engine is specified
        if (engine == null || engine.length() == 0) {
            Log.log("Storage engine attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        //make sure specified engine is valid
        boolean isValid = false;
        for (String validEngine : validEngines)
            if (validEngine.equals(engine)) {
                isValid = true;
                break;
            }
        if (!isValid) {
            Log.log("Specified engine: " + engine + " is not a valid query engine for: " + id, componentName, Log.ERROR);
            return false;
        }

        ////////////// host ////////////
        host = (String) storageConfigJsonObject.get("host");

        //make sure host is specified
        if (host == null || host.length() == 0) {
            Log.log("Storage host attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        ////////////// port ////////////
        port = (String) storageConfigJsonObject.get("port");

        //make sure port is specified
        if (port == null || port.length() == 0 || !isInteger(port)) {
            Log.log("Storage port attribute is not defined or is not a valid integer number for: " + id, componentName, Log.ERROR);
            return false;
        }

        ////////////// database ////////////
        database = (String) storageConfigJsonObject.get("database");

        //make sure database is specified
        if (database == null || database.length() == 0) {
            Log.log("Storage database name attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        ////////////// username ////////////
        username = (String) storageConfigJsonObject.get("username");

        //make sure username is specified
        if (username == null || username.length() == 0) {
            Log.log("Storage username attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        ////////////// password ////////////
        password = (String) storageConfigJsonObject.get("password");

        //make sure password is specified
        if (password == null || password.length() == 0) {
            Log.log("Storage password attribute is not defined for: " + id, componentName, Log.ERROR);
            return false;
        }

        //configure container with extracted information
        storageConfigContainer.setId(id);
        storageConfigContainer.setParentId(parentId);
        storageConfigContainer.setEngine(engine);
        storageConfigContainer.setHost(host);
        storageConfigContainer.setPort(port);
        storageConfigContainer.setDatabase(database);
        storageConfigContainer.setUsername(username);
        storageConfigContainer.setPassword(password);

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

    /**
     * Get host address
     *
     * @return host address of the storage
     * @since 1.0
     */
    public String getHost() {
        return host;
    }

    /**
     * Set host address
     *
     * @param host host of this storage
     * @since 1.0
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Get port number
     *
     * @return port of the storage
     * @since 1.0
     */
    public String getPort() {
        return port;
    }

    /**
     * Set port number
     *
     * @param port port of this storage
     * @since 1.0
     */
    public void setPort(String port) {
        this.port = port;
    }

    /**
     * Get database name
     *
     * @return name of the database
     * @since 1.0
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Set database name
     *
     * @param database database of this storage
     * @since 1.0
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Get username credential
     *
     * @return username credential
     * @since 1.0
     */
    public String getUsername() {
        return username;
    }

    /**
     * Set username
     *
     * @param username username of this storage
     * @since 1.0
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Get password credential
     *
     * @return password credential
     * @since 1.0
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set password
     *
     * @param password password of this storage
     * @since 1.0
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get storage Identifier
     *
     * @return id of this storage
     * @since 1.0
     */
    public String getId() {
        return this.id;
    }

    /**
     * Set storage id
     *
     * @param id id of this storage
     * @since 1.0
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get parent id of this storage
     *
     * @return id of parent storage
     * @since 1.0
     */
    public String getParentId() {
        return this.parentId;
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
     * Get query Engine
     *
     * @return engine name to use when querying data of this storage
     * @since 1.0
     */
    public String getEngine() {
        return this.engine;
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
     * Get parent storage
     *
     * @return parent storage of this storage in storage graph
     * @since 1.0
     */
    public StorageConfigContainer getParent() {
        return this.parent;
    }

    /**
     * Set parent storage
     *
     * @param storageConfigContainer parent of this storage in storage graph
     * @since 1.0
     */
    public void setParent(StorageConfigContainer storageConfigContainer) {
        this.parent = storageConfigContainer;
    }

    /**
     * Adds a child storage system to the list of children
     *
     * @param child child storage belonging to this storage
     * @since 1.0
     */
    public void addChild(StorageConfigContainer child) {
        this.children.add(child);
    }

    /**
     * Get list of child storage systems
     *
     * @return list of children storage belonging to this storage
     * @since 1.0
     */
    public List<StorageConfigContainer> getChildren() {
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
     * @param attributeName  name of the attribute to search for
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
}
