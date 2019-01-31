package config;

import java.util.ArrayList;
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

}
