package com.asml.apa.wta.core.logger;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.utils.WtaUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Logging utils to set up part of the Log4j2 configuration.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class Log4j2Configuration {

  /**
   * Private constructor that throws an exception to ensure the utils class cannot be instantiated.
   *
   * @throws IllegalStateException when invoked
   * @author Atour Mousavi Gouraib
   * @since 1.0.0
   */
  private Log4j2Configuration() {
    throw new IllegalStateException();
  }

  /**
   * Sets up the parts of the logging configuration that are set by user input.
   * Sets the logging level in accordance with the user input in config.json.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public static void setUpLoggingConfig() {
    RuntimeConfig config = WtaUtils.readConfig();
    Level logLevel;
    switch (config.getLogLevel()) {
      case "TRACE":
        logLevel = Level.TRACE;
        break;
      case "OFF":
        logLevel = Level.OFF;
        break;
      case "ALL":
        logLevel = Level.ALL;
        break;
      case "DEBUG":
        logLevel = Level.DEBUG;
        break;
      case "FATAL":
        logLevel = Level.FATAL;
        break;
      case "WARN":
        logLevel = Level.WARN;
        break;
      case "INFO":
        logLevel = Level.INFO;
        break;
      case "ERROR":
      default:
        logLevel = Level.ERROR;
        break;
    }
    Configurator.setRootLevel(logLevel);
  }
}
