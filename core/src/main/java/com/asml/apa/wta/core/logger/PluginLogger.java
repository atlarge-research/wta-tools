package com.asml.apa.wta.core.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
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
      instance = LoggerFactory.getLogger(PluginLogger.class);
    }
    return instance;
  }

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

  public static void loadConfig(String logLevel, boolean doConsoleLog, boolean doFileLog) {
//    RuntimeConfig runtimeConfig = WtaUtils.readConfig();
    Level level = getLogLevel(logLevel);
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5level %logger{36} - %msg%n").build();
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();
    LoggerConfig loggerConfig = configuration.getLoggerConfig(instance.getName());

    if (doConsoleLog) {
      ConsoleAppender consoleAppender = ConsoleAppender.newBuilder()
              .setConfiguration(configuration)
              .withLayout(layout)
              .build();
      loggerConfig.addAppender(consoleAppender, level, null);
    }

    if (doFileLog) {
      RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
              .withFileName("core/logging/example.log")
              .withAppend(true)
              .withLocking(false)
              .withPolicy(SizeBasedTriggeringPolicy.createPolicy("1MB"))
              .withLayout(layout)
              .build();
      loggerConfig.addAppender(rollingFileAppender, level, null);
    }
    context.updateLoggers();
  }
}
