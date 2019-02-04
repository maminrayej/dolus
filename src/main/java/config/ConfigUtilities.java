package config;

import common.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
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
     * Available storage types
     */
    private static String[] storageTypes = {"mysql", "mongodb"};

    /**
     * Available query engines
     */
    private static String[] engines = {"apache_drill"};

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
        HashMap<String, String> configuration = new HashMap<>();
        mainConfigLoaded = ConfigParser.parseMainConfigFileContents(configContent, configuration);

        if (mainConfigLoaded) {
            Log.log("Contents of the main config file parsed and stored successfully", componentName, Log.INFORMATION);

            //configure storage config directory and log system based on extracted data
            storageConfigDir = configuration.get("storage_config_dir");
            Log.setLogDir(configuration.get("log_dir"));
        } else
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
            Log.log("Storage config file can not be located: " + storageConfigDir, componentName, Log.ERROR);
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

        Log.log("Contents of the storage config file read successfully", componentName, Log.INFORMATION);

        //initialize storage config list
        storageConfigList = new ArrayList<>();

        /*
         * once scanning the storage config file is not enough to link parent and child storage systems
         * because storage systems can appear in arbitrary orders in storage config file
         * therefore parser store top level(parent) storage systems separately from child storage systems
         * then loops through the child storage systems and link each child to its parent
         * parents are stored in a hash map structure to be found fast
         * children are stored in a list
         */
        //Hash map of top level storage systems: storage id -> storage object
        HashMap<String, StorageConfig> topLevelStorageSystems = new HashMap<>();

        //List of storage systems having parents
        ArrayList<StorageConfig> children = new ArrayList<>();

        //parse config content and extract configurations
        storageConfigLoaded = ConfigParser.parseStorageConfigFileContents(configContent, storageConfigList, storageTypes, engines, topLevelStorageSystems, children);

        //create storage graph by connecting child and parent storage systems together
        for (StorageConfig child : children) {

            String parentId = child.getParentId();

            //find parent storage among top level storage systems
            StorageConfig parent = topLevelStorageSystems.get(parentId);

            //check whether parent id really exists
            if (parent == null) {
                Log.log(String.format("Parent id: %s defined by storage: %s is not a top level storage", parentId, child.getId()), componentName, Log.ERROR);
                return false;
            }

            parent.addChild(child);

            child.setParent(parent);

        }

        if (storageConfigLoaded)
            Log.log("Contents of the storage config file parsed and stored successfully", componentName, Log.INFORMATION);
        else
            Log.log("Storing storage configuration failed", componentName, Log.ERROR);

        return storageConfigLoaded;
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
     * Searches among registered storage systems to find one containing the named collection
     *
     * @param collectionName name of the collection
     * @return storage containing the named collection if found, null otherwise
     * @since 1.0
     */
    public static StorageConfig findStorage(String collectionName) {

        if (!storageConfigLoaded) {
            Log.log("Storage config is not loaded. can not search for collections", componentName, Log.ERROR);
            return null;
        }

        StorageConfig foundStorage = null;

        //queue is used to walk through storage graph and visit top level storage systems first
        ArrayBlockingQueue<StorageConfig> queue = new ArrayBlockingQueue<>(1);

        queue.addAll(storageConfigList);

        while (!queue.isEmpty()) {

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
