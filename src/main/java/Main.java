import config.ConfigUtilities;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;


public class Main {

    public static void main(String[] args) throws Exception {

        ConfigUtilities.loadMainConfig("/home/amin/programming/projects/dolus/dolus-config.json");

        ConfigUtilities.loadStorageConfig();

    }
}
