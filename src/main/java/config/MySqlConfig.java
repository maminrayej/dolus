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
public class MySqlConfig extends StorageConfig {

    /**
     * Component name to use in the logging system
     */
    private static final String componentName = "MySqlConfig";

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
     * Wraps a MySqlConfig object around configs of a MySQL storage
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
    public MySqlConfig(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                       String id, String engine, String parentId, String host, String port, String database, String username, String password) {

        super(id, engine, parentId, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

    }

    /**
     * Wraps a MySqlConfig object around configs of a MySQL storage
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
    public MySqlConfig(HashMap<String, HashSet<String>> tablesInfo, HashMap<String, String> primaryKeys,
                       String id, String engine, String host, String port, String database, String username, String password) {

        super(id, engine, host, port, database, username, password);

        this.tablesInfo = tablesInfo;
        this.primaryKeys = primaryKeys;

    }

    public MySqlConfig(){

        super();

        this.tablesInfo = null;
        this.primaryKeys = null;
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

    public void setTablesInfo(HashMap<String, HashSet<String>> tablesInfo) {
        this.tablesInfo = tablesInfo;
    }

    public void setPrimaryKeys(HashMap<String, String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public static boolean parseMySQLConfig(MySqlConfig mySqlConfig, JSONObject mySqlConfigObject, String[] engines, HashSet<String> visitedIds){

        boolean successful = StorageConfig.parseStorageConfig(mySqlConfig, mySqlConfigObject, engines, visitedIds);

        if (!successful){
            Log.log("MySQL storage config parsing failed", componentName, Log.ERROR);
            return false;
        }

        String id = mySqlConfig.getId();

        //storage meta data
        HashMap<String, HashSet<String>> tablesInfo = new HashMap<>();//map each table with its column set: table name -> [column1 ,column2, ...]
        HashMap<String, String> primaryKeys = new HashMap<>();//map tables with their primary keys: table name -> primary key

        //meta data extraction
        JSONArray tables = (JSONArray) mySqlConfigObject.get("tables");
        if (tables == null || tables.size() == 0) {
            Log.log("MySQL tables attribute is not defined or it does not contain any table for: " + id, componentName, Log.ERROR);
            return false;
        }

        //meta data holders
        JSONObject table;//contains data about current table
        JSONArray columns;//contains data about column of current table
        HashSet<String> columnsSet;//set containing all columns of current table
        String column;//name of current column
        String tableName;//name of current table
        String primaryKey;//primary key of current table

        //for each table, extract its meta data and store it
        for (Object tableObject : tables) {

            //make a new column set
            columnsSet = new HashSet<>();

            table = (JSONObject) tableObject;

            tableName = (String) table.get("name");
            if (tableName == null || tableName.length() == 0) {
                Log.log("MySQL table name attribute is not defined for one of its tables for: " + id, componentName, Log.ERROR);
                return false;
            }

            primaryKey = (String) table.get("pk");
            if (primaryKey == null || primaryKey.length() == 0) {
                Log.log("MySQL primary key(pk) attribute is not defined for one of its tables for: " + id, componentName, Log.ERROR);
                return false;
            }

            columns = (JSONArray) table.get("columns");
            if (columns == null || columns.size() == 0) {
                Log.log("MySQL columns attribute is not defined for one of its table or the table does not contain any columns for: " + id, componentName, Log.ERROR);
                return false;
            }

            //extract column names and add them to the column set
            for (Object columnNameObject : columns) {

                column = (String) columnNameObject;

                columnsSet.add(column);
            }

            //check whether pk attribute is one of the columns
            if (!columnsSet.contains(primaryKey)){
                Log.log("MySQL PK attribute: " + primaryKey + " is not present as one of the columns for: " + id, componentName, Log.ERROR);
                return false;
            }

            //map each table with its column set
            tablesInfo.put(tableName, columnsSet);

            //map each table with its primary key
            primaryKeys.put(tableName, primaryKey);

        }

        mySqlConfig.setPrimaryKeys(primaryKeys);
        mySqlConfig.setTablesInfo(tablesInfo);

        return true;
    }
}
