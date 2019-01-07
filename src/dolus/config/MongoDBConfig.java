package dolus.config;

import java.util.HashSet;

/**
 * Contains information about MongoDB database
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MongoDBConfig {

    /**
     * Contains collection names
     */
    private HashSet<String> collections;

    /**
     * Host address of MySQL database
     */
    private String host;

    /**
     * Port number which database in listening on
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
     * Wraps a MongoDBConfig object around the configs of a MongoDB database
     *
     * @param collections set of collection names
     * @param host        host address
     * @param port        port number
     * @param database    database name
     * @param username    username credentials
     * @param password    password credentials
     * @since 1.0
     */
    public MongoDBConfig(HashSet<String> collections, String host, String port, String database, String username, String password) {

        this.collections = collections;
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
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
     * Host address of the database
     *
     * @return host address of the database
     * @since 1.0
     */
    public String getHost() {
        return host;
    }

    /**
     * Port number of the database
     *
     * @return port of the database
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
}
