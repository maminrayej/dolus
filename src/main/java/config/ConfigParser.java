package config;

import common.Log;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashMap;

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
}

