package common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class has static methods to work with the log file of Dolus
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class Log {

    /**
     * Path to log file
     */
    private static String logDir = null;

    /**
     * For errors and exceptions happen in dolus
     */
    public static final int ERROR = 1;

    /**
     * For error and exceptions happen in dolus but program can function properly
     */
    public static final int WARNING = 2;

    /**
     * Minor problems that would be nice to solve them!
     */
    public static final int NOTICE = 3;

    /**
     * Regular messages
     */
    public static final int INFORMATION = 4;

    /**
     * Logs the message with specified type
     *
     * @param msg           message to be logged
     * @param componentName name of the component the message belongs to
     * @param type          type of the log
     * @since 1.0
     */
    public static void log(String msg, String componentName, int type) {

        //set date and time format to yyyy-MM-dd HH:mm:ss
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //get current date and time
        Date now = new Date();

        //apply specified format
        String currentDateTime = dateFormat.format(now);

        //create type string
        String typeStr = "Information";
        if (type == ERROR)
            typeStr = "Error";
        else if (type == WARNING)
            typeStr = "Warning";
        else if (type == NOTICE)
            typeStr = "Notice";
        else if (type == INFORMATION)
            typeStr = "Information";

        //check whether log file is configured or not
        if (logDir == null){
            System.out.println("Log file is not configured, printing message to standard output");
            System.out.println(String.format("[%s][%s][%s]%s\n", currentDateTime, typeStr, componentName, msg));
            return;
        }
        //point to the log file
        File logFile = new File(logDir);

        //check whether file exists or not
        if (!logFile.exists()) {
            System.out.println("Log file does not exist");
            return;
        }

        try {
            //create a file writer to write the message to log file
            FileWriter writer = new FileWriter(logFile,true);
            writer.write(String.format("[%s][%s][%s]%s\n", currentDateTime, typeStr, componentName, msg));

            //close the writer resource
            writer.close();

        } catch (IOException e) {
            System.out.println("Something went wrong during logging the message");
        }

    }

    /**
     * Sets log dir property
     *
     * @param logDir path to log file
     * @since 1.0
     */
    public static void setLogDir(String logDir){
        Log.logDir = logDir;
    }


}
