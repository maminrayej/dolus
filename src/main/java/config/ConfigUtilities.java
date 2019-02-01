package config;

import common.Log;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This class is responsible for reading and parsing the config files of Dolus
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class ConfigUtilities {

    /**
     * Component name to use in logging system
     */
    private static final String componentName = "ConfigUtilities";

    /**
     * Path to the storage config file
     */
    private static String storageConfigDir;

    /**
     * Path to log file using by logging system
     */
    private static String logDir;

    /**
     * List of all storage systems present in storage config
     * this list actually contains just top level storage systems
     * child storage systems can be access through their parents.
     */
    private static List<StorageConfig> storageConfigList;

    /**
     * Flag indicates whether main config file loaded successfully or not
     */
    private static boolean mainConfigLoaded = false;

    /**
     * Flag indicates whether storage config file loaded successfully or not
     */
    private static boolean storageConfigLoaded = false;

    /**
     * Loads the main config file
     *
     * @param configDir path to main config file
     * @return true if loading configuration was successful, false otherwise
     * @since 1.0
     */
    public static boolean loadMainConfig(String configDir) {

        File configFile = new File(configDir);

        //check whether config file exists and dolus has read permission
        if (!configFile.exists()) {
            Log.log("Main config file can not be located: " + configDir, componentName, Log.ERROR);
            return false;
        } else if (!configFile.canRead()) {
            Log.log("Dolus does not have permission to read the main config file", componentName, Log.ERROR);
            return false;
        }

        //read contents of the config file
        String configContent = readConfigFileContents(configFile);
        if (configContent == null) {
            Log.log("Something went wrong during reading contents of the main configuration file", componentName, Log.ERROR);
            return false;
        }

        Log.log("Contents of the main config file read successfully", componentName, Log.INFORMATION);

        //parse config content and extract configurations
        mainConfigLoaded = parseMainConfigFileContents(configContent);

        if (mainConfigLoaded)
            Log.log("Contents of the main config file parsed and stored successfully", componentName, Log.INFORMATION);
        else
            Log.log("Storing main configuration failed", componentName, Log.ERROR);

        return mainConfigLoaded;
    }

    /**
     * Loads the storage config file
     *
     * @return true if loading configuration was successful, false otherwise
     * @since 1.0
     */
    public static boolean loadStorageConfig() {

        if (!mainConfigLoaded) {
            Log.log("Main config file is not loaded. can not load storage config file", componentName, Log.ERROR);
            return false;
        }

        File configFile = new File(storageConfigDir);

        //check whether config file exists and dolus has read permission
        if (!configFile.exists()) {
            Log.log("Storage config file can not be located", componentName, Log.ERROR);
            return false;
        } else if (!configFile.canRead()) {
            Log.log("Dolus does not have permission to read the storage config file", componentName, Log.ERROR);
            return false;
        }

        //read contents of the config file
        String configContent = readConfigFileContents(configFile);
        if (configContent == null) {

            Log.log("Something went wrong during reading contents of the storage configuration file", componentName, Log.ERROR);
            return false;
        }

        Log.log("Contents of the storage config file loaded successfully", componentName, Log.INFORMATION);

        //initialize storage config list
        storageConfigList = new ArrayList<>();

        //parse config content and extract configurations
        storageConfigLoaded = parseStorageConfigFileContents(configContent, storageConfigList);

        if (storageConfigLoaded)
            Log.log("Contents of the storage config file parsed and stored successfully", componentName, Log.INFORMATION);
        else
            Log.log("Storing storage configuration failed", componentName, Log.ERROR);

        return storageConfigLoaded;
    }

    /**
     * Parses contents of the main config file
     *
     * @param configContent contents of the main config file
     * @return true if parsing was successful, false otherwise
     * @since 1.0
     */
    private static boolean parseMainConfigFileContents(String configContent) {

        //initialize a json parser
        JSONParser parser = new JSONParser();

        //result of parsing
        boolean result = true;

        try {
            //get root object of the json file
            JSONObject root = (JSONObject) parser.parse(configContent);

            //get directory of the log file
            logDir = (String) root.get("log_dir");
            //make sure log dir is specified
            if (logDir == null || logDir.length() == 0){
                Log.log("log_dir is not specified in main config file", componentName, Log.ERROR);
                result = false;
            }else
                Log.setLogDir(logDir);//configure logging directory

            //get directory of the storage config file
            storageConfigDir = (String) root.get("storage_config_dir");
            //make sure storage config dir is specified
            if (storageConfigDir == null || storageConfigDir.length() == 0) {
                Log.log("storage_config_dir is not specified in main config file", componentName, Log.ERROR);
                result = false;
            }

        } catch (ParseException e) {
            Log.log("Can not parse contents of the main config file. Check json syntax", componentName, Log.ERROR);
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
    private static boolean parseStorageConfigFileContents(String configContent, List<StorageConfig> storageConfigList) {

        //initialize a json parser
        JSONParser parser = new JSONParser();

        /*
        * once scanning the storage config file is not enough to link parent and child storage systems
        * because storage systems can appear in arbitrary orders in storage config file
        * therefore parser store top level(parent) storage systems separately from child storage systems
        * then loops through the child storage systems and link each child to its parent
        * parents are stored in a hash map structure to be found fast
        * children are stored in a list
        * */
        //Hash map of top level storage systems: storage id -> storage object
        HashMap<String, StorageConfig> topLevelStorageSystems = new HashMap<>();

        //List of storage systems having parents
        ArrayList<StorageConfig> children = new ArrayList<>();

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

                if (storageType.equals("mysql")) {
                    MySqlConfig mySqlConfig = parseMySQLConfig(storage);
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
                    MongoDBConfig mongoDBConfig = parseMongoDBConfig(storage);
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
            Log.log("Can not parse contents of the storage config file", componentName, Log.ERROR);
            return false;
        }

        //create storage graph by connecting child and parent storage systems together
        for (StorageConfig child : children) {

            String parentId = child.getParentId();

            //find parent storage among top level storage systems
            StorageConfig parent = topLevelStorageSystems.get(parentId);

            //check whether parent id really exists
            if (parent == null) {
                Log.log("Parent id: %s defined by storage: %s does not exist in storage config file", componentName, Log.ERROR);
                return false;
            }

            parent.addChild(child);

            child.setParent(parent);

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
    private static MySqlConfig parseMySQLConfig(JSONObject mySqlConfigObject) {

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

        engine = (String) mySqlConfigObject.get("engine");
        if (engine == null || engine.length() == 0) {
            Log.log("MySQL engine attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        host = (String) mySqlConfigObject.get("host");
        if (host == null || host.length() == 0) {
            Log.log("MySQL host attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        port = (String) mySqlConfigObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)) {
            Log.log("MySQL port attribute is not defined or is not a valid integer number", componentName, Log.ERROR);
            return null;
        }

        database = (String) mySqlConfigObject.get("database");
        if (database == null || database.length() == 0) {
            Log.log("MySQL database name attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        username = (String) mySqlConfigObject.get("username");
        if (username == null || username.length() == 0) {
            Log.log("MySQL username attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        password = (String) mySqlConfigObject.get("password");
        if (password == null || password.length() == 0) {
            Log.log("MySQL password attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        //meta data extraction
        JSONArray tables = (JSONArray) mySqlConfigObject.get("tables");
        if (tables == null || tables.size() == 0) {
            Log.log("MySQL tables attribute is not defined or it does not contain any table", componentName, Log.ERROR);
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
                Log.log("MySQL table name attribute is not defined for one of its tables", componentName, Log.ERROR);
                return null;
            }

            primaryKey = (String) table.get("pk");
            if (primaryKey == null || primaryKey.length() == 0) {
                Log.log("MySQL primary key(pk) attribute is not defined for one of its tables", componentName, Log.ERROR);
                return null;
            }

            columns = (JSONArray) table.get("columns");
            if (columns == null || columns.size() == 0) {
                Log.log("MySQL columns attribute is not defined for one of its table or the table does not contain any columns", componentName, Log.ERROR);
                return null;
            }

            //extract column names and add them to the column set
            for (Object columnNameObject : columns) {

                column = (String) columnNameObject;

                columnsSet.add(column);
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
    private static MongoDBConfig parseMongoDBConfig(JSONObject mongoDBConfigObject) {

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

        engine = (String) mongoDBConfigObject.get("engine");
        if (engine == null || engine.length() == 0) {
            Log.log("MongoDB engine attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        host = (String) mongoDBConfigObject.get("host");
        if (host == null || host.length() == 0) {
            Log.log("MongoDB host attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        port = (String) mongoDBConfigObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)) {
            Log.log("MongoDB port attribute is not defined or is not a valid integer number", componentName, Log.ERROR);
            return null;
        }

        database = (String) mongoDBConfigObject.get("database");
        if (database == null || database.length() == 0) {
            Log.log("MongoDB database name attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        username = (String) mongoDBConfigObject.get("username");
        if (username == null || username.length() == 0) {
            Log.log("MongoDB username attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        password = (String) mongoDBConfigObject.get("password");
        if (password == null || password.length() == 0) {
            Log.log("MongoDB password attribute is not defined", componentName, Log.ERROR);
            return null;
        }

        //meta data extraction
        JSONArray collectionArray = (JSONArray) mongoDBConfigObject.get("collections");
        if (collectionArray == null || collectionArray.size() == 0) {
            Log.log("MongoDB collections attribute is not defined or it does not contain any collection", componentName, Log.ERROR);
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
                Log.log("MongoDB collection name attribute is not defined for one of its collections", componentName, Log.ERROR);
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
     * Reads content of the config file
     *
     * @param configFile configuration file
     * @return contents of the config file or null if an error occurs
     * @since 1.0
     */
    private static String readConfigFileContents(File configFile) {

        try {

            Scanner scanner = new Scanner(configFile);

            StringBuilder content = new StringBuilder();

            while (scanner.hasNextLine())
                content.append(scanner.nextLine());

            return content.toString();
        } catch (FileNotFoundException e) {
            return null;
        }

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
     * Searches among registered storage systems to find one containing the named collection
     *
     * @param collectionName name of the collection
     * @return storage containing the named collection if found, null otherwise
     * @since 1.0
     */
    public static StorageConfig findStorage(String collectionName){

        if (!storageConfigLoaded){
            Log.log("Storage config is not loaded. can not search for collections", componentName, Log.ERROR);
            return null;
        }

        StorageConfig foundStorage = null;

        //queue is used to walk through storage graph and visit top level storage systems first
        ArrayBlockingQueue<StorageConfig> queue = new ArrayBlockingQueue<>(1);

        queue.addAll(storageConfigList);

        while (!queue.isEmpty()){

            StorageConfig current = queue.remove();

            if (current.containsCollection(collectionName)) {
                foundStorage = current;
                break;
            }

            queue.addAll(current.getChildren());
        }

        return foundStorage;
    }

}
