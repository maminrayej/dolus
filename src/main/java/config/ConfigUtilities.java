package config;

import common.Log;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This class is responsible for loading config files of Dolus
 * <p>
 * There are two config files. main config file and storage config file.
 * main config file contains global configurations and tweaks about dolus.
 * it determines where necessary files are, how dolus should behave and etc.
 * storage config file contains information about what storage systems are present and how they are arranged.
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
     * Valid storage types
     */
    private static String[] validStorageTypes = {"mysql", "mongodb"};

    /**
     * Valid query engines
     */
    private static String[] validEngines = {"apache_drill"};

    /**
     * List of all storage systems present in storage config
     * this list actually contains just top level storage systems
     * child storage systems can be access through their parents.
     * this data structure represent storage graph.
     * example:
     * [ mysql1  ----> mongo1 ----> mysql2 ]
     * /     \                       |
     * mongo2  mongo3                 mongo4
     * <p>
     * this data structure only contains mysql1, mongo1, mysql2
     * other storage systems can be accessed by these three.
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
     * @param mainConfigDir path to main config file
     * @return true if loading configuration was successful, false otherwise
     * @since 1.0
     */
    public static boolean loadMainConfig(String mainConfigDir) {

        /*
         * this method has three phase:
         * 1- Checking phase: checks whether main config file exists and is readable
         * 2- Reading phase : reads the content of the file
         * 3- Parsing phase : parses contents of the file
         * and if all three phases were successful, it configures the system based on extracted configurations.
         */

        ////////////////////// Checking phase //////////////////////////
        File mainConfigFile = new File(mainConfigDir);

        //check whether main config file exists and dolus has permission to read
        if (!mainConfigFile.exists()) {
            Log.log("Main config file can not be located here: " + mainConfigDir, componentName, Log.ERROR);
            return false;
        } else if (!mainConfigFile.canRead()) {
            Log.log("Dolus does not have permission to read the main config file", componentName, Log.ERROR);
            return false;
        }

        ////////////////////// Reading phase //////////////////////////
        //read contents of the config file
        String mainConfigContent = readConfigFileContents(mainConfigFile);
        if (mainConfigContent == null) {
            Log.log("Something went wrong during reading contents of the main configuration file", componentName, Log.ERROR);
            return false;
        }

        Log.log("Contents of the main configuration file read successfully", componentName, Log.INFORMATION);

        ////////////////////// Parsing phase //////////////////////////
        /*
         * data structure to contain main configuration contents.
         * main config file contains some key value pairs for example: "log_dir" : "/home/user/...".
         * this object is used to store these key value pairs.
         */
        HashMap<String, String> configuration = new HashMap<>();

        //parse mainConfigContent and store its key value pair configurations in configurations object
        mainConfigLoaded = ConfigParser.parseMainConfigFileContents(mainConfigContent, configuration);

        //if main config contents parsed successfully
        if (mainConfigLoaded) {
            Log.log("Contents of the main config file parsed and stored successfully", componentName, Log.INFORMATION);

            //configure storage directory
            storageConfigDir = configuration.get("storage_config_dir");

            //configure logging system directory
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

        /*
         * this method has four phase:
         * 1- Checking phase  : checks that storage config file exists, is readable and main config file was loaded successfully before
         * 2- Reading phase   : reads the content of the file
         * 3- Parsing phase   : parses contents of the file
         * 4- Construct phase : constructs the storage graph based on storage systems specified in storage config file
         */

        ////////////////////// Checking phase //////////////////////////
        //make sure main config file was loaded successfully
        if (!mainConfigLoaded) {
            Log.log("Main config file is not loaded. can not load storage config file", componentName, Log.ERROR);
            return false;
        }

        File storageConfigFile = new File(storageConfigDir);

        //check whether config file exists and dolus has permission to read
        if (!storageConfigFile.exists()) {
            Log.log("Storage config file can not be located here: " + storageConfigDir, componentName, Log.ERROR);
            return false;
        } else if (!storageConfigFile.canRead()) {
            Log.log("Dolus does not have permission to read the storage config file", componentName, Log.ERROR);
            return false;
        }

        ////////////////////// Reading phase //////////////////////////
        //read contents of the config file
        String storageConfigContent = readConfigFileContents(storageConfigFile);
        if (storageConfigContent == null) {

            Log.log("Something went wrong during reading contents of the storage configuration file", componentName, Log.ERROR);
            return false;
        }

        Log.log("Contents of the storage config file read successfully", componentName, Log.INFORMATION);

        ////////////////////// Parsing phase //////////////////////////
        //initialize storage config list
        storageConfigList = new ArrayList<>();

        /*
         * once scanning the storage config file is not enough to link parent and child storage systems
         * and construct the storage graph because storage systems can appear in arbitrary orders in storage config file
         * therefore parser store top level(parent) storage systems separately from child storage systems
         * then loops through the child storage systems and link each child to its parent
         * parents are stored in a hash map structure to be found fast
         * children are stored in a list
         */
        //Hash map of top level storage systems: storage id -> storage object
        HashMap<String, StorageConfig> topLevelStorageSystems = new HashMap<>();

        //List of storage systems which have parents
        ArrayList<StorageConfig> children = new ArrayList<>();

        //parse storage config content and extract configurations
        storageConfigLoaded = ConfigParser.parseStorageConfigFileContents(storageConfigContent, storageConfigList, validStorageTypes, validEngines, topLevelStorageSystems, children);

        ////////////////////// Construct phase //////////////////////////
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
     * Reads contents of the config file
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
        } catch (IOException e) {
            return null;
        }

    }

    /**
     * Searches among registered storage systems to find one which contains the named collection and attribute
     *
     * @param collectionName name of the collection
     * @param attributeName  name of the attribute in the collection
     * @return storage containing the named collection if found, null otherwise
     * @since 1.0
     */
    public static StorageConfig findStorage(String collectionName, String attributeName) {

        //make sure storage config is loaded successfully
        if (!storageConfigLoaded) {
            Log.log("Storage graph is not configured yet. Can not search for collections", componentName, Log.ERROR);
            return null;
        }

        /*
         * search all storage systems for the specified collection and attribute using BFS.
         * this method uses BFS because dolus search among top level storage systems first.
         * if there is a storage that contains the collection name, only its sub tree will be searched to find the storage containing attribute
         * it means there can not be two storage systems with same collection name unless they are in a sub tree
         * for example:
         *      mysql1(table1, table2) ----> mongo1(table3,table4) ----> mysql2(table4,table5)
         *      /  \                                                            |
         *    ...  ...                                                         ...
         *
         *    if we search for table4, mongo1 will be found and mysql2 will be deleted from the queue.
         */

        //add top level storage systems to queue
        Queue<StorageConfig> queue = new LinkedList<>(storageConfigList);

        //search the storage graph using BFS
        while (!queue.isEmpty()) {

            StorageConfig current = queue.remove();

            //if current storage contains collection search for attribute in its sub tree
            if (current.containsCollection(collectionName)) {
                if (current.containsAttribute(collectionName, attributeName))
                    return current;
                else{
                    queue.clear();
                    queue.addAll(current.getChildren());
                }
            }
            else
                queue.addAll(current.getChildren());
        }

        return null;
    }

}
