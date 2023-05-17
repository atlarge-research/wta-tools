package com.asml.apa.wta.core.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

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
    throw new AssertionError();
  }

  /**
   * Returns the instance of the Logger. If the instance is null, it will create a new one.
   * @return Logger The instance of the Logger
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
   * Switch statement to turn log level string into log level instance.
   * @param level   String of the log level
   * @return        Log level instance of the string passed in
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private static Level getLogLevel(String level) {
    switch(level) {
      case "ALL": return Level.ALL;
      case "DEBUG": return Level.DEBUG;
      case "INFO": return Level.INFO;
      case "ERROR": return Level.ERROR;
      case "FATAL": return Level.FATAL;
      case "OFF": return Level.OFF;
      default: return Level.TRACE;
    }
  }

  /**
   * Loads the configuration for the logger. This requires that the user defined config file
   * is parsed before calling this method. Alternatively, if the user hasn't defined the log settings,
   * it will be passed the default values in the RuntimeConfig class.
   * @param logLevel      String of the log level
   * @param doConsoleLog  Boolean to determine if console logging is enabled
   * @param doFileLog     Boolean to determine if file logging is enabled
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void loadConfig(String logLevel, boolean doConsoleLog, boolean doFileLog) {
    Level level = getLogLevel(logLevel);
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5level %logger{36} - %msg%n").build();
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();
    LoggerConfig loggerConfig = configuration.getRootLogger();
    if (doConsoleLog) {
      ConsoleAppender consoleAppender = ConsoleAppender.newBuilder()
              .setName("stdout")
              .setConfiguration(configuration)
              .setLayout(layout)
              .build();
      consoleAppender.start();
      loggerConfig.addAppender(consoleAppender, level, null);
      context.updateLoggers();
    }

    if (doFileLog) {
      RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
              .setName("R")
              .withFileName("logging/example.log")
              .setIgnoreExceptions(false)
              .withFilePattern("core/logging/app.%i.log.gz")
              .withAppend(true)
              .withLocking(false)
              .withPolicy(SizeBasedTriggeringPolicy.createPolicy("1MB"))
              .setLayout(layout)
              .build();
      rollingFileAppender.start();
      loggerConfig.addAppender(rollingFileAppender, level, null);
      context.updateLoggers();
    }
  }
}
