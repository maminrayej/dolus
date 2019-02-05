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
                    MySqlConfig mySqlConfig = new MySqlConfig();
                    boolean successful = MySqlConfig.parseMySQLConfig(mySqlConfig, storage, engines, visitedIds);
                    if (!successful)
                        return false;

                    //check whether storage is top level or not
                    if (mySqlConfig.getParentId() == null) {
                        topLevelStorageSystems.put(mySqlConfig.getId(), mySqlConfig);
                        storageConfigList.add(mySqlConfig);
                    } else
                        children.add(mySqlConfig);
                } else if (storageType.equals("mongodb")) {
                    MongoDBConfig mongoDBConfig = new MongoDBConfig();
                    boolean successful = MongoDBConfig.parseMongoDBConfig(mongoDBConfig, storage, engines, visitedIds);
                    if (!successful)
                        Log.log("Parsing MongoDB storage config failed", componentName, Log.ERROR);

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

}

