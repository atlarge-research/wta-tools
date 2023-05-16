package com.asml.apa.wta.spark;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Logger for the Spark plugin.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class PluginLogger {

    private static final Logger logger = LogManager.getLogger(PluginLogger.class);

    /**
     * This method should be in called in the entry point in of the plugin. It loads the
     * log4j2.properties file.
     * @author Pil Kyu Cho
     * @since 1.0.0
     */
    public static void loadConfig() {
        try {
            System.out.println("Initilaized properties");
            Properties props = new Properties();
            props.load(new FileInputStream("adapter/spark/log4j2.properties"));
            PropertyConfigurator.configure(props);
        }
        catch(IOException e) {
            System.out.println("Error in loading log4j2.properties file");
        }
    }

    /**
     * Any class that wants to log a message should call this method.
     *
     * @param msg   The message to be logged
     * @author Pil Kyu Cho
     * @since 1.0.0
     */
    public static void log(String msg) {
        logger.info(msg);
    }
}
