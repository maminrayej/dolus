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

    private static final String logDir = "/home/amin/programming/projects/java/Dolus/dolus.log";

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

        //point to the log file
        File logFile = new File(logDir);

        //check whether file exists or not
        if (!logFile.exists())
            System.out.println("Log file does not exist");

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


}