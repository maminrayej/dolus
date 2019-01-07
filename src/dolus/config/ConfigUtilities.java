package dolus.config;

import dolus.common.Log;
import dolus.language.mysql.utilities.MySqlParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

/**
 * This class is responsible for reading and parsing the config file of Dolus
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class ConfigUtilities {

    /**
     * Directory of the configuration file
     */
    private static final String configDir = "/home/amin/programming/projects/java/Dolus/config.json";

    /**
     * Structure that stores MySQL meta data and configurations
     */
    private MySqlConfig mySqlConfig;

    /**
     * Structure that stores MongoDB meta data and configuration
     */
    private MongoDBConfig mongoDBConfig;

    /**
     * Loads the config file and reads the contents.
     *
     * @throws IOException if config can not be located or can not be read due to lack of permission
     * @since 1.0
     */
    public void loadConfig() throws IOException {

        //open config file and check for reading permission
        File configFile = new File(configDir);

        if (!configFile.exists())
            throw new FileNotFoundException("Config file can not be located");
        else if (!configFile.canRead())
            throw new IOException("Does not have permission to read config file");

        //read contents of the config file
        String configContent = readConfigContents(configFile);

        //parse config content and extract configurations

    }

    /**
     * Reads content of the config file
     *
     * @param configFile configuration file
     * @return contents of the config file
     * @throws FileNotFoundException if config file can not be located
     * @since 1.0
     */
    private String readConfigContents(File configFile) throws FileNotFoundException {

        Scanner scanner = new Scanner(configFile);

        StringBuilder content = new StringBuilder();

        while (scanner.hasNextLine())
            content.append(scanner.nextLine());

        return content.toString();

    }

    /**
     * Parses contents of the config file and stores extracted database metadata as a structure
     *
     * @param configContent contents of the
     * @return true if parsing was successful and meta data stored successfully, false otherwise
     * @since 1.0
     */
    private boolean parseConfigContent(String configContent){

        //initialize a json parser
        JSONParser parser = new JSONParser();

        try{
            //get root object of json file
            JSONObject root = (JSONObject) parser.parse(configContent);

            //get database vendors
            JSONArray vendors = (JSONArray) root.get("vendors");

            //check that at least two vendors are defined
            if (vendors == null || vendors.size() < 2){
                Log.log("At least two vendor must be specified in config file", Log.ERROR);
                return false;
            }

            //for each vendor, call its appropriate parser
            for (Object vendorObject : vendors){

                JSONObject vendor = (JSONObject) vendorObject;

                //get vendor name
                String vendorName = (String) vendor.get("vendor");

                //check whether vendor name attribute is specified
                if (vendorName == null || vendorName.length() == 0){
                    Log.log("Vendor name is not specified for one the vendors", Log.ERROR);
                    return false;
                }

                //call vendor specific parser
                if (vendorName.equalsIgnoreCase("MySQL")){
                    boolean result = parseMySQLConfig(vendor);
                    if (!result)
                        return false;
                }
                else if (vendorName.equalsIgnoreCase("MongoDB")){
                    boolean result = parseMongoDBConfig(vendor);
                    if (!result)
                        return false;
                }
            }

        }
        catch (ParseException e){
            Log.log("Could not parse the config file", Log.ERROR);
            return false;
        }

        return true;

    }

    /**
     * Extract MySQL meta data and configurations
     *
     * @param mySqlConfigObject json object that contains MySQL meta data and configurations
     * @return true if parsing and storing meta data and configurations was successful
     * @since 1.0
     */
    private boolean parseMySQLConfig(JSONObject mySqlConfigObject){

        //configurations
        String host;
        String port;
        String database;
        String username;
        String password;

        //meta data
        HashMap<String, HashSet<String>> tablesInfo = new HashMap<>();//map each table with its column set
        HashMap<String,String> primaryKeys = new HashMap<>();//map each table with its primary key

        //configuration extraction
        host = (String) mySqlConfigObject.get("host");
        if (host == null || host.length() == 0){
            Log.log("MySQL host attribute is not defined", Log.ERROR);
            return false;
        }

        port = (String) mySqlConfigObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)){
            Log.log("MySQL port attribute is not defined or is not a valid integer number",Log.ERROR);
            return false;
        }

        database = (String) mySqlConfigObject.get("database");
        if (database == null || database.length() == 0){
            Log.log("MySQL database name attribute is not defined", Log.ERROR);
            return false;
        }

        username = (String) mySqlConfigObject.get("username");
        if (username == null || username.length() == 0){
            Log.log("MySQL username attribute is not defined", Log.ERROR);
            return false;
        }

        password = (String) mySqlConfigObject.get("password");
        if (password == null || password.length() == 0){
            Log.log("MySQL password attribute is not defined", Log.ERROR);
            return false;
        }

        //meta data extraction
        JSONArray tables = (JSONArray) mySqlConfigObject.get("tables");
        if (tables == null || tables.size() == 0){
            Log.log("MySQL tables attribute is not defined or it does not contain any tables", Log.ERROR);
            return false;
        }

        //meta data holders
        JSONObject table;
        String tableName;
        String primaryKey;
        JSONArray columns;
        HashSet<String> columnsSet = new HashSet<>();
        String column;

        //for each table, extract its meta data and store it
        for (Object tableObject : tables){

            table = (JSONObject) tableObject;

            tableName = (String) table.get("name");
            if (tableName == null || tableName.length() == 0){
                Log.log("MySQL table name attribute is not defined for one of its tables", Log.ERROR);
                return false;
            }

            primaryKey = (String) table.get("pk");
            if (primaryKey == null || primaryKey.length() == 0){
                Log.log("MySQL primary key(pk) attribute is not defined for one of its tables", Log.ERROR);
                return false;
            }

            columns = (JSONArray) table.get("columns");
            if (columns == null || columns.size() == 0){
                Log.log("MySQL columns attribute is not defined for one of its table or the table does not contain any columns", Log.ERROR);
                return false;
            }

            //extract column names and add them to the column set
            for (Object columnNameObject : columns){

                column = (String) columnNameObject;

                columnsSet.add(column);
            }

            //map each table with its column set
            tablesInfo.put(tableName, columnsSet);

            //map each table with its primary key
            primaryKeys.put(tableName, primaryKey);

            //clear the columns set holder for next iteration
            columnsSet.clear();

        }

        //initialize MySQL configuration container with extracted data
        this.mySqlConfig = new MySqlConfig(tablesInfo, primaryKeys, host, port, database, username, password);

        return true;
    }

    /**
     * Extract MongoDB meta data and configurations
     *
     * @param mongoDBConfigObject json object that contains MongoDB meta data and configurations
     * @return true if parsing and storing meta data and configurations was successful
     * @since 1.0
     */
    private boolean parseMongoDBConfig(JSONObject mongoDBConfigObject){

        //configurations
        String host;
        String port;
        String database;
        String username;
        String password;

        //meta data
        HashSet<String> collections = new HashSet<>();//set of all collection names in mongodb database

        //configuration extraction
        host = (String) mongoDBConfigObject.get("host");
        if (host == null || host.length() == 0){
            Log.log("MongoDB host attribute is not defined", Log.ERROR);
            return false;
        }

        port = (String) mongoDBConfigObject.get("port");
        if (port == null || port.length() == 0 || !isInteger(port)){
            Log.log("MongoDB port attribute is not defined or is not a valid integer number",Log.ERROR);
            return false;
        }

        database = (String) mongoDBConfigObject.get("database");
        if (database == null || database.length() == 0){
            Log.log("MongoDB database name attribute is not defined", Log.ERROR);
            return false;
        }

        username = (String) mongoDBConfigObject.get("username");
        if (username == null || username.length() == 0){
            Log.log("MongoDB username attribute is not defined", Log.ERROR);
            return false;
        }

        password = (String) mongoDBConfigObject.get("password");
        if (password == null || password.length() == 0){
            Log.log("MongoDB password attribute is not defined", Log.ERROR);
            return false;
        }

        //meta data extraction
        JSONArray collectionArray = (JSONArray) mongoDBConfigObject.get("collections");
        if (collectionArray == null || collectionArray.size() == 0){
            Log.log("MongoDB collections attribute is not defined or it does not contain any collection", Log.ERROR);
            return false;
        }

        //meta data holders
        String collectionName;
        JSONObject collection;

        //for each collection extract its name
        for (Object collectionObject : collectionArray){

            collection = (JSONObject) collectionObject;

            collectionName = (String) collection.get("name");

            if (collectionName == null || collectionName.length() == 0){
                Log.log("MongoDB collection name attribute is not defined for one of its collections", Log.ERROR);
                return false;
            }

            collections.add(collectionName);
        }

        //initialize MongoDB configuration container with extracted data
        this.mongoDBConfig = new MongoDBConfig(collections, host, port, database, username, password);

        return true;
    }

    /**
     *
     * @return MySqlConfig container that holds configuration and meta data about MySQL database
     */
    public MySqlConfig getMySqlConfig() {
        return mySqlConfig;
    }

    /**
     *
     * @return MongoDBConfig container that holds configuration and meta data about MongoDB database
     */
    public MongoDBConfig getMongoDBConfig() {
        return mongoDBConfig;
    }

    /**
     * Checks if the specified string is a integer in form of [0-9]+
     * @param integer string integer
     * @return true if string is a valid integer, false otherwise
     * @since 1.0
     */
    private boolean isInteger(String integer){

        for (int i = 0; i < integer.length(); i++)
            if (!Character.isDigit(integer.charAt(i)))
                return false;

        return true;
    }

}
