package com.asml.apa.wta.spark;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Logger for the Spark plugin. Singleton design pattern so that one Logger is present
 * at all times.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class PluginLogger {

  private static Logger instance;

  /**
   * Private constructor to prevent instantiation from other classes
   */
  private PluginLogger() {
    instance = LogManager.getLogger(PluginLogger.class);
  }

  /**
   * Returns the instance of the Logger. If the instance is null, it will create a new one.
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static Logger getInstance() {
    if (instance == null) {
      instance = LogManager.getLogger(PluginLogger.class);
    }
    return instance;
  }

  /**
   * This method should be in called in the entry point of the plugin. It loads the
   * log4j2.properties file.
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void loadConfig() {
    try {
      Properties props = new Properties();
      props.load(new FileInputStream("adapter/spark/log4j2.properties"));
      PropertyConfigurator.configure(props);
    } catch (IOException e) {
      System.out.println("Error in loading log4j2.properties file");
    }
  }
}
