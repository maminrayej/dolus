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
     * @param mainConfigContent contents of the main config file
     * @param configuration     object to store extracted configurations in
     * @return true if parsing was successful, false otherwise
     * @since 1.0
     */
    static boolean parseMainConfigFileContents(String mainConfigContent, HashMap<String, String> configuration) {

        //initialize a json parser
        JSONParser parser = new JSONParser();

        //result of parsing
        boolean result = true;

        try {
            //get root object of the json file
            JSONObject root = (JSONObject) parser.parse(mainConfigContent);

            /////////////// log directory //////////////

            //get directory of the log file
            String logDir = (String) root.get("log_dir");

            //make sure log dir is specified
            if (logDir == null || logDir.length() == 0) {
                Log.log("log_dir is not specified in main config file", componentName, Log.ERROR);
                result = false;
            } else
                configuration.put("log_dir", logDir);

            /////////////// storage config directory //////////////

            //get directory of the storage config file
            String storageConfigDir = (String) root.get("storage_config_dir");

            //make sure storage config dir is specified
            if (storageConfigDir == null || storageConfigDir.length() == 0) {
                Log.log("storage_config_dir is not specified in main config file", componentName, Log.ERROR);
                result = false;
            } else
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
     * @param storageConfigContent       contents of the storage config file
     * @param validStorageTypes          valid storage types
     * @param validEngines               valid engines
     * @param storageConfigContainerList list to store top level storage systems in
     * @param topLevelStorageSystems     contains mapping between top level storage systems ids and their storage config objects
     * @param childStorageSystems        contains list of storage systems which are not top level(have parents)
     * @return true if parsing was successful and meta data stored successfully, false otherwise
     * @since 1.0
     */
    static boolean parseStorageConfigFileContents(String storageConfigContent, String[] validStorageTypes, String[] validEngines,
                                                  List<StorageConfigContainer> storageConfigContainerList, HashMap<String, StorageConfigContainer> topLevelStorageSystems, ArrayList<StorageConfigContainer> childStorageSystems) {

        //initialize a json parser
        JSONParser parser = new JSONParser();

        /*
         * initialize a HashSet to keep track of visited ids.
         * this data structure is used to make sure all ids in the storage config file are unique
         */
        HashSet<String> visitedIds = new HashSet<>();

        try {
            //get root object of json file
            JSONObject root = (JSONObject) parser.parse(storageConfigContent);

            //get array of storage systems
            JSONArray storageConfigArray = (JSONArray) root.get("storage");

            //make sure at least one storage is defined
            if (storageConfigArray == null || storageConfigArray.size() < 1) {
                Log.log("At least one storage must be specified in storage config file", componentName, Log.ERROR);
                return false;
            }

            //for each storage system, call its appropriate parser
            for (Object storageConfigObject : storageConfigArray) {

                JSONObject storageConfigJsonObject = (JSONObject) storageConfigObject;

                //get storage type
                String storageType = (String) storageConfigJsonObject.get("type");

                //make sure that storage type attribute is specified
                if (storageType == null || storageType.length() == 0) {
                    Log.log("Storage type is not specified for one of the storage systems", componentName, Log.ERROR);
                    return false;
                }

                //make sure specified storage type is valid
                boolean isValid = false;
                for (String validType : validStorageTypes)
                    if (validType.equals(storageType)) {
                        isValid = true;
                        break;
                    }
                if (!isValid) {
                    Log.log("Specified type: " + storageType + " is not a valid storage type", componentName, Log.ERROR);
                    return false;
                }

                //call appropriate parser for each storage based on their type
                if (storageType.equals("mysql")) {

                    //initialize a new MySQL config container
                    MySqlConfigContainer mySqlConfigContainer = new MySqlConfigContainer();

                    //parse MySQL config
                    boolean successful = MySqlConfigContainer.parseMySQLConfig(mySqlConfigContainer, storageConfigJsonObject, validEngines, visitedIds);
                    if (!successful)
                        return false;

                    //check whether storage is top level or not
                    if (mySqlConfigContainer.getParentId() == null) {

                        //add storage to top level storage systems
                        topLevelStorageSystems.put(mySqlConfigContainer.getId(), mySqlConfigContainer);

                        //add storage to storage graph
                        storageConfigContainerList.add(mySqlConfigContainer);

                    } else
                        childStorageSystems.add(mySqlConfigContainer);

                } else if (storageType.equals("mongodb")) {

                    MongoDBConfigContainer mongoDBConfigContainer = new MongoDBConfigContainer();

                    boolean successful = MongoDBConfigContainer.parseMongoDBConfig(mongoDBConfigContainer, storageConfigJsonObject, validEngines, visitedIds);
                    if (!successful)
                        return false;

                    //check whether storage is top level or not
                    if (mongoDBConfigContainer.getParentId() == null) {

                        //add storage to top level storage systems
                        topLevelStorageSystems.put(mongoDBConfigContainer.getId(), mongoDBConfigContainer);

                        //add storage to storage graph
                        storageConfigContainerList.add(mongoDBConfigContainer);

                    } else
                        childStorageSystems.add(mongoDBConfigContainer);
                }

            }

        } catch (ParseException e) {
            Log.log("Can not parse contents of the storage config file. Check JSON syntax", componentName, Log.ERROR);
            return false;
        }

        return true;
    }

}

