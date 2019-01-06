package dolus.config;

import dolus.exception.IllegalJsonFormatException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
     * @return contents of the
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

    private static boolean parseMySQLConfig(JSONObject mySQLConfig) {

        //add code here
        return false;
    }

    private static boolean parseMongoDBConfig(JSONObject mongoDBConfig) {

        //add code here
        return false;
    }
}
