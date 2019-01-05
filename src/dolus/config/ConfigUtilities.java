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

        //parse the config file and store configuration
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

    private static void parseConfigContent(String configContent) throws IllegalJsonFormatException {
        //add code here
    }
}
