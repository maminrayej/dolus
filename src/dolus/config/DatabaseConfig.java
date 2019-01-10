package dolus.config;

/**
 * Contains configuration of the database
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class DatabaseConfig {

    /**
     * Host address of database
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
     * Wraps a DatabaseConfig object around the configs of a database
     *
     * @param host     host address of database
     * @param port     port number of database
     * @param database database name
     * @param username username credential
     * @param password password credential
     * @since 1.0
     */
    public DatabaseConfig(String host, String port, String database, String username, String password) {

        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;

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
