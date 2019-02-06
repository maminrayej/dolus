package config;

import common.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Contains information about MySQL storage
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class MySqlConfigContainer extends StorageConfigContainer {

    /**
     * Component name to use in the logging system
     */
    private static final String componentName = "MySqlConfigContainer";

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
     * Wraps a MySqlConfigContainer object around configs of a MySQL storage
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
    public MySqlConfigContainer(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                                String id, String engine, String parentId, String host, String port, String database, String username, String password) {

        super(id, engine, parentId, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Wraps a MySqlConfigContainer object around configs of a MySQL storage
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
    public MySqlConfigContainer(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                                String id, String engine, String host, String port, String database, String username, String password) {

        super(id, engine, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Default constructor
     *
     * @since 1.0
     */
    public MySqlConfigContainer() {

        super();

        this.tablesInfo = null;
        this.primaryKeys = null;
    }

    /**
     * Parses MySql configurations
     *
     * @param mySqlConfigContainer  container to store extracted configurations in
     * @param mySqlConfigJsonObject json object containing configurations
     * @param validEngines          valid engines
     * @param visitedIds            visited ids
     * @return true if parsing and storing configurations was successful, false otherwise
     * @since 1.0
     */
    public static boolean parseMySQLConfig(MySqlConfigContainer mySqlConfigContainer, JSONObject mySqlConfigJsonObject, String[] validEngines, HashSet<String> visitedIds) {

        //first parse basic configurations
        boolean successful = StorageConfigContainer.parseStorageConfig(mySqlConfigContainer, mySqlConfigJsonObject, validEngines, visitedIds);

        //make sure parsing basic configurations was successful
        if (!successful) {
            Log.log("MySQL storage config parsing failed", componentName, Log.ERROR);
            return false;
        }

        //get id of the current storage(is used for logging more helpful messages)
        String id = mySqlConfigContainer.getId();

        //map each table with its column set: table name -> [column1 ,column2, ...]
        HashMap<String, HashSet<String>> tablesInfo = new HashMap<>();

        //map tables with their primary keys: table name -> primary key
        HashMap<String, String> primaryKeys = new HashMap<>();

        //get array of tables present in the storage
        JSONArray tables = (JSONArray) mySqlConfigJsonObject.get("tables");

        //make sure storage has at least on table
        if (tables == null || tables.size() == 0) {
            Log.log("For MySQL storage: " + id + ", tables attribute is not defined or it does not contain any table", componentName, Log.ERROR);
            return false;
        }

        //for each table, extract its meta data and store it
        for (Object tableObject : tables) {

            //set containing all columns of current table
            HashSet<String> columnsSet = new HashSet<>();

            //contains data about current table
            JSONObject tableJsonObject = (JSONObject) tableObject;

            /////////////////// table name //////////////////
            //name of current table
            String tableName = (String) tableJsonObject.get("name");

            //make sure table name is specified
            if (tableName == null || tableName.length() == 0) {
                Log.log("For MySQL storage: " + id + ", table name attribute is not defined for one of its tables", componentName, Log.ERROR);
                return false;
            }

            /////////////////// columns //////////////////

            //contains data about column of current table
            JSONArray columnsArray = (JSONArray) tableJsonObject.get("columns");

            if (columnsArray == null || columnsArray.size() == 0) {
                Log.log("For MySQL storage: " + id + ", columns attribute is not defined for table: " + tableName + " or the table does not contain any columns", componentName, Log.ERROR);
                return false;
            }

            //extract column names and add them to the column set
            for (Object columnNameObject : columnsArray) {

                //name of current column
                String columnName = (String) columnNameObject;

                columnsSet.add(columnName);
            }

            /////////////////// primary key //////////////////

            //primary key of current table
            String primaryKey = (String) tableJsonObject.get("pk");

            //make sure primary key is specified
            if (primaryKey == null || primaryKey.length() == 0) {
                Log.log("For MySQL storage: " + id + ", primary key(pk) attribute is not defined for table: " + tableName, componentName, Log.ERROR);
                return false;
            }

            //make sure pk attribute is one of the columns
            if (!columnsSet.contains(primaryKey)) {
                Log.log("For MySQL storage: " + id + ", PK attribute: " + primaryKey + " is not present as one of the columns for table: " + tableName, componentName, Log.ERROR);
                return false;
            }

            //map table with its column set
            tablesInfo.put(tableName, columnsSet);

            //map table with its primary key
            primaryKeys.put(tableName, primaryKey);

        }

        //store tables info in container
        mySqlConfigContainer.setTablesInfo(tablesInfo);

        //store primary keys in container
        mySqlConfigContainer.setPrimaryKeys(primaryKeys);

        return true;
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

    /**
     * Set tables info
     *
     * @param tablesInfo mapping between table name and its columns
     * @since 1.0
     */
    public void setTablesInfo(HashMap<String, HashSet<String>> tablesInfo) {
        this.tablesInfo = tablesInfo;
    }

    /**
     * Set primary keys
     *
     * @param primaryKeys mapping between table name and its primary key
     * @since 1.0
     */
    public void setPrimaryKeys(HashMap<String, String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
