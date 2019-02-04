package config;

import common.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * This class is responsible for parsing config files
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
class ConfigParser {

    /**
     * Component name to use in logging system
     */
    private static final String componentName = "ConfigParser";

    /**
     * Parses contents of the main config file
     *
     * @param configContent contents of the main config file
     * @return true if parsing was successful, false otherwise
     * @since 1.0
     */
    static boolean parseMainConfigFileContents(String configContent, HashMap<String, String> configuration) {

        //initialize a json parser
        JSONParser parser = new JSONParser();

        //result of parsing
        boolean result = true;

        try {
            //get root object of the json file
            JSONObject root = (JSONObject) parser.parse(configContent);

            //get directory of the log file
            String logDir = (String) root.get("log_dir");
            //make sure log dir is specified
            if (logDir == null || logDir.length() == 0) {
                Log.log("log_dir is not specified in main config file", componentName, Log.ERROR);
                result = false;
            } else
                configuration.put("log_dir", logDir);

            //get directory of the storage config file
            String storageConfigDir = (String) root.get("storage_config_dir");
            //make sure storage config dir is specified
            if (storageConfigDir == null || storageConfigDir.length() == 0) {
                Log.log("storage_config_dir is not specified in main config file", componentName, Log.ERROR);
                result = false;
            }

            configuration.put("storage_config_dir", storageConfigDir);

        } catch (ParseException e) {
            Log.log("Can not parse contents of the main config file. Check JSON syntax", componentName, Log.ERROR);
            result = false;
        }

        return result;
    }

    /**
     * Parses contents of the storage config file and stores extracted storage metadata
     *
     * @param configContent contents of the storage config file
     * @return true if parsing was successful and meta data stored successfully, false otherwise
     * @since 1.0
     */
    static boolean parseStorageConfigFileContents(String configContent, List<StorageConfig> storageConfigList, String[] storageTypes, String[] engines,
                                                  HashMap<String, StorageConfig> topLevelStorageSystems, ArrayList<StorageConfig> children) {

        //initialize a json parser
        JSONParser parser = new JSONParser();

        //initialize a HashSet to keep track of visited ids
        HashSet<String>  visitedIds = new HashSet<>();

        try {
            //get root object of json file
            JSONObject root = (JSONObject) parser.parse(configContent);

            //get array storage systems
            JSONArray storageArray = (JSONArray) root.get("storage");

            //make sure at least one storage is defined
            if (storageArray == null || storageArray.size() < 1) {
                Log.log("At least one storage must be specified in storage config file", componentName, Log.ERROR);
                return false;
            }

            //for each storage system, call its appropriate parser
            for (Object storageObject : storageArray) {

                JSONObject storage = (JSONObject) storageObject;

                //get storage type
                String storageType = (String) storage.get("type");

                //make sure that storage type attribute is specified
                if (storageType == null || storageType.length() == 0) {
                    Log.log("Storage type is not specified for one of the storage systems", componentName, Log.ERROR);
                    return false;
                }

                //make sure type is valid
                boolean isValid = false;
                for (String validType : storageTypes)
                    if (validType.equals(storageType)) {
                        isValid = true;
                        break;
                    }
                if (!isValid){
                    Log.log("Specified type: " + storageType + " is not a valid storage type", componentName, Log.ERROR);
                    return false;
                }

                if (storageType.equals("mysql")) {
                    MySqlConfig mySqlConfig = parseMySQLConfig(storage, engines, visitedIds);
                    if (mySqlConfig == null) {
                        Log.log("Parsing MySQL storage config failed", componentName, Log.ERROR);
                        return false;
                    }
                    //check whether storage is top level or not
                    if (mySqlConfig.getParentId() == null) {
                        topLevelStorageSystems.put(mySqlConfig.getId(), mySqlConfig);
                        storageConfigList.add(mySqlConfig);
                    } else
                        children.add(mySqlConfig);
                } else if (storageType.equals("mongodb")) {
                    MongoDBConfig mongoDBConfig = parseMongoDBConfig(storage, engines, visitedIds);
                    if (mongoDBConfig == null) {
                        Log.log("Parsing MongoDB storage config failed", componentName, Log.ERROR);
                        return false;
                    }
                    //check whether storage is top level or not
                    if (mongoDBConfig.getParentId() == null) {
                        topLevelStorageSystems.put(mongoDBConfig.getId(), mongoDBConfig);
                        storageConfigList.add(mongoDBConfig);
                    } else children.add(mongoDBConfig);
                }

            }

        } catch (ParseException e) {
            Log.log("Can not parse contents of the storage config file. Check JSON syntax", componentName, Log.ERROR);
            return false;
        }

        return true;
    }

    /**
     * Extract MySQL meta data and configurations
     *
     * @param mySqlConfigObject json object that contains MySQL meta data and configurations
     * @return true if parsing and storing meta data and configurations was successful, false otherwise
     * @since 1.0
     */
    private static MySqlConfig parseMySQLConfig(JSONObject mySqlConfigObject, String[] engines, HashSet<String> visitedIds) {

        //storage configurations
        String id;
        String parentId;
        String engine;
        String host;
        String port;
        String database;
        String username;
        String password;

        //storage meta data
        HashMap<String, HashSet<String>> tablesInfo = new HashMap<>();//map each table with its column set: table name -> [column1 ,column2, ...]
        HashMap<String, String> primaryKeys = new HashMap<>();//map tables with their primary keys: table name -> primary key

        //configuration extraction
        parentId = (String) mySqlConfigObject.get("parent");

        id = (String) mySqlConfigObject.get("id");
        if (id == null || id.length() == 0) {
            Log.log("MySQL id attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        //make sure visited id is unique
        if (visitedIds.contains(id)){
            Log.log("MySQL id: " + id + " is used before", componentName, Log.ERROR);
            return null;
        }

        //if id is unique add it to visited ids
        visitedIds.add(id);

        engine = (String) mySqlConfigObject.get("engine");
        if (engine == null || engine.length() == 0) {
            Log.log("MySQL engine attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        //make sure specified engine is valid
        boolean isValid = false;
        for (String validEngine : engines)
            if (validEngine.equals(engine)){
                isValid = true;
                break;
            }
        if (!isValid){
            Log.log("Specified engine: " + engine + " is not a valid query engine for: " + id, componentName, Log.ERROR);
            return null;
        }

        host = (String) mySqlConfigObject.get("host");
        if (host == null || host.length() == 0) {
            Log.log("MySQL host attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        port = (String) mySqlConfigObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)) {
            Log.log("MySQL port attribute is not defined or is not a valid integer number for: " + id, componentName, Log.ERROR);
            return null;
        }

        database = (String) mySqlConfigObject.get("database");
        if (database == null || database.length() == 0) {
            Log.log("MySQL database name attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        username = (String) mySqlConfigObject.get("username");
        if (username == null || username.length() == 0) {
            Log.log("MySQL username attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        password = (String) mySqlConfigObject.get("password");
        if (password == null || password.length() == 0) {
            Log.log("MySQL password attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        //meta data extraction
        JSONArray tables = (JSONArray) mySqlConfigObject.get("tables");
        if (tables == null || tables.size() == 0) {
            Log.log("MySQL tables attribute is not defined or it does not contain any table for: " + id, componentName, Log.ERROR);
            return null;
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
                return null;
            }

            primaryKey = (String) table.get("pk");
            if (primaryKey == null || primaryKey.length() == 0) {
                Log.log("MySQL primary key(pk) attribute is not defined for one of its tables for: " + id, componentName, Log.ERROR);
                return null;
            }

            columns = (JSONArray) table.get("columns");
            if (columns == null || columns.size() == 0) {
                Log.log("MySQL columns attribute is not defined for one of its table or the table does not contain any columns for: " + id, componentName, Log.ERROR);
                return null;
            }

            //extract column names and add them to the column set
            for (Object columnNameObject : columns) {

                column = (String) columnNameObject;

                columnsSet.add(column);
            }

            //check whether pk attribute is one of the columns
            if (!columnsSet.contains(primaryKey)){
                Log.log("MySQL PK attribute: " + primaryKey + " is not present as one of the columns for: " + id, componentName, Log.ERROR);
                return null;
            }

            //map each table with its column set
            tablesInfo.put(tableName, columnsSet);

            //map each table with its primary key
            primaryKeys.put(tableName, primaryKey);

        }

        //initialize MySQL configuration container with extracted data
        return new MySqlConfig(tablesInfo, primaryKeys, id, engine, parentId, host, port, database, username, password);
    }

    /**
     * Extract MongoDB meta data and configurations
     *
     * @param mongoDBConfigObject json object that contains MongoDB meta data and configurations
     * @return true if parsing and storing meta data and configurations was successful
     * @since 1.0
     */
    private static MongoDBConfig parseMongoDBConfig(JSONObject mongoDBConfigObject, String[] engines, HashSet<String> visitedIds) {

        //storage configurations
        String id;
        String parentId;
        String engine;
        String host;
        String port;
        String database;
        String username;
        String password;

        //storage meta data
        HashSet<String> collections = new HashSet<>();//set of all collection names in mongodb database
        HashMap<String, String> primaryKeys = new HashMap<>();//map each collection name with its primary key

        //configuration extraction
        parentId = (String) mongoDBConfigObject.get("parent");

        id = (String) mongoDBConfigObject.get("id");
        if (id == null || id.length() == 0) {
            Log.log("MongoDB id attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        //make sure visited id is unique
        if (visitedIds.contains(id)){
            Log.log("MongoDB id: " + id + " is used before", componentName, Log.ERROR);
            return null;
        }

        //if id is unique add it to visited ids
        visitedIds.add(id);

        engine = (String) mongoDBConfigObject.get("engine");
        if (engine == null || engine.length() == 0) {
            Log.log("MongoDB engine attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        //make sure specified engine is valid
        boolean isValid = false;
        for (String validEngine : engines)
            if (validEngine.equals(engine)){
                isValid = true;
                break;
            }
        if (!isValid){
            Log.log("Specified engine: " + engine + " is not a valid query engine for: " + id, componentName, Log.ERROR);
            return null;
        }

        host = (String) mongoDBConfigObject.get("host");
        if (host == null || host.length() == 0) {
            Log.log("MongoDB host attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        port = (String) mongoDBConfigObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)) {
            Log.log("MongoDB port attribute is not defined or is not a valid integer number for: " + id, componentName, Log.ERROR);
            return null;
        }

        database = (String) mongoDBConfigObject.get("database");
        if (database == null || database.length() == 0) {
            Log.log("MongoDB database name attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        username = (String) mongoDBConfigObject.get("username");
        if (username == null || username.length() == 0) {
            Log.log("MongoDB username attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        password = (String) mongoDBConfigObject.get("password");
        if (password == null || password.length() == 0) {
            Log.log("MongoDB password attribute is not defined for: " + id, componentName, Log.ERROR);
            return null;
        }

        //meta data extraction
        JSONArray collectionArray = (JSONArray) mongoDBConfigObject.get("collections");
        if (collectionArray == null || collectionArray.size() == 0) {
            Log.log("MongoDB collections attribute is not defined or it does not contain any collection for: " + id, componentName, Log.ERROR);
            return null;
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
                return null;
            }

            //extract collection primary key
            String collectionPrimaryKey = (String) collection.get("pk");

            //if primary key is not defined or is empty -> set it to default mongoDB primary kry : _id
            if (collectionPrimaryKey == null || collectionPrimaryKey.length() == 0)
                collectionPrimaryKey = "_id";

            collections.add(collectionName);

            primaryKeys.put(collectionName, collectionPrimaryKey);
        }

        //initialize MongoDB configuration container with extracted data
        return new MongoDBConfig(collections, primaryKeys, id, engine, parentId, host, port, database, username, password);

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

