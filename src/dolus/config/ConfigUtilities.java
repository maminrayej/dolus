package dolus.config;

import dolus.exception.IllegalJsonFormatException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
     * Loads the config file and reads the contents.
     *
     * @throws IOException if config can not be located or can not be read due to lack of permission
     * @since 1.0
     */
    public static void loadConfig() throws IOException, IllegalJsonFormatException {

        //open config file and check for reading permission
        File configFile = new File(configDir);

        if (!configFile.exists())
            throw new FileNotFoundException("Config file can not be located");
        else if (!configFile.canRead())
            throw new IOException("Does not have permission to read config file");

        //read contents of the config file
        String configContent = readConfigContents(configFile);

        //parse the config file
        parseConfigContent(configContent);
    }

    /**
     * Reads content of the config file
     *
     * @param configFile configuration file
     * @return contents of the config file
     * @throws FileNotFoundException if config file can not be located
     */
    private static String readConfigContents(File configFile) throws FileNotFoundException {

        Scanner scanner = new Scanner(configFile);

        StringBuilder content = new StringBuilder();

        while (scanner.hasNextLine())
            content.append(scanner.nextLine());

        return content.toString();

    }

    /**
     * Parse config contents and stores config information
     *
     * @param configContent content of the config file
     * @return true if parsing was successful, false otherwise
     * @throws IllegalJsonFormatException if the format of the content is not json
     */
    private static boolean parseConfigContent(String configContent) throws IllegalJsonFormatException {

        try {

            JSONParser parser = new JSONParser();

            JSONObject root = (JSONObject) parser.parse(configContent);

            //extract database vendors
            JSONArray vendors = (JSONArray) root.get("vendors");

            //if vendors does not exist or number of specified vendors is zero
            if (vendors == null || vendors.size() == 0) {

                System.out.println("Database vendors are not specified");

                return false;

            } else if (vendors.size() == 1) {

                System.out.println("Must at least provide two database vendors");

                return false;
            }

            //extract information about each vendor
            JSONObject vendor;//object contains info about each vendor
            String vendorName;
            boolean result;//keeps the result of parsing information about each vendor

            //at each loop, extract the name of vendor and call the appropriate vendor parser
            for (Object vendor1 : vendors) {

                vendor = (JSONObject) vendor1;

                //extract vendor name
                vendorName = (String) vendor.get("vendor");

                //call vendor specific parser
                if (vendorName.equalsIgnoreCase("mysql")) {
                    result = parseMySQLConfig(vendor);
                    if (!result)
                        return false;
                } else if (vendorName.equalsIgnoreCase("mongodb")) {
                    result = parseMongoDBConfig(vendor);
                    if (!result)
                        return false;
                }
            }


        } catch (ParseException e) {
            throw new IllegalJsonFormatException("Config file is malformed");
        }

        return true;
    }

    /**
     * Parse MySQL configurations
     *
     * @param mySQLConfig object that contains sql configurations
     * @return true if parsing was successful, false otherwise
     */
    private static boolean parseMySQLConfig(JSONObject mySQLConfig) {

        //extract primary configurations
        String host;
        String port;
        String database;

        host = (String) mySQLConfig.get("host");
        port = (String) mySQLConfig.get("port");
        database = (String) mySQLConfig.get("database");

        //check whether primary configs are defined well or not
        if (host == null || host.length() == 0 ||
                port == null || port.length() == 0 ||
                database == null || database.length() == 0) {

            System.out.println("Host, port or database is empty or not defined in MySQL config");

            return false;
        }

        //extract secondary information

        //extract tables
        JSONArray tables = (JSONArray) mySQLConfig.get("tables");

        if (tables == null){
            System.out.println("No table defined in MySQL configuration");

            return false;
        }

        //extract table information
        HashMap<String,String[]> tableInfo = new HashMap<>();// table name -> (column1, column2, ...)
        HashMap<String,String> primaryKeys = new HashMap<>();// table name -> primary key

        String tableName;
        String primaryKey;
        JSONArray columns;
        ArrayList<String> columnList = new ArrayList<>();

        for (Object tableObject : tables){

            JSONObject table = (JSONObject) tableObject;

            tableName  = (String) table.get("name");
            if (tableName == null || tableName.length() == 0) return false;

            primaryKey = (String) table.get("pk");
            if (primaryKey == null || primaryKey.length() == 0) return false;

            columns    = (JSONArray) table.get("columns");
            if (columns == null || columns.size() == 0) return false;

            //extract columns of the current table
            for (Object column : columns)
                columnList.add((String) column);

            String[] template = new String[0];//hints the column list to convert the list to a String array not Object
            tableInfo.put(tableName, columnList.toArray(template));

            primaryKeys.put(tableName, primaryKey);

        }


        return true;
    }

    /**
     * Parse MongoDB configurations
     *
     * @param mongoDBConfig object that contains mongoDB configurations
     * @return true if parsing was successful, false otherwise
     */
    private static boolean parseMongoDBConfig(JSONObject mongoDBConfig) {

        //add code here
        return false;
    }
}
