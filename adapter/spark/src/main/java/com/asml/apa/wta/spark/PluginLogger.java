package com.asml.apa.wta.spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logger for the Spark plugin. Singleton design pattern so that one Logger is present
 * at all times.
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class PluginLogger {

  private static Logger instance;

  /**
   * Private constructor to prevent instantiation from other classes.
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private PluginLogger() {
    instance = LoggerFactory.getLogger(PluginLogger.class);
  }

  /**
   * Returns the instance of the Logger. If the instance is null, it will create a new one.
   * @return Logger The instance of the Logger
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static Logger getInstance() {
    if (instance == null) {
      instance = LoggerFactory.getLogger(PluginLogger.class);
    }
    return instance;
  }

  /**
   * This method should be in called in the entry point of the plugin. It loads the
   * log4j2.properties file.
   * @param filePath      The path to the log4j2.properties file
   * @throws IOException  If the file is not found
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void loadConfig(String filePath) throws IOException {
    Properties props = new Properties();
    props.load(new FileInputStream(filePath));
    PropertyConfigurator.configure(props);
  }
}
