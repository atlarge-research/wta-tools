package com.asml.apa.wta.core.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

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
    switch (level) {
      case "ALL":
        return Level.ALL;
      case "DEBUG":
        return Level.DEBUG;
      case "ERROR":
        return Level.ERROR;
      case "FATAL":
        return Level.FATAL;
      case "OFF":
        return Level.OFF;
      case "TRACE":
        return Level.TRACE;
      default:
        return Level.INFO;
    }
  }

  /**
   * Loads the logger configurations. It sets the log level and creates the defined Appender(s).
   * User must have defined the values in the config file. Otherwise, it will receive the default, values
   * in the RuntimeConfig class.
   * @param logLevel      String of the log level
   * @param doConsoleLog  Boolean to determine if console logging is enabled
   * @param doFileLog     Boolean to determine if file logging is enabled
   * @param logPath       String of the path to write log files
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void loadConfig(String logLevel, boolean doConsoleLog, boolean doFileLog, String logPath) {
    String pattern = "%d [%t] %p - %m%n";
    ConfigurationBuilder<BuiltConfiguration> configBuilder = ConfigurationBuilderFactory.newConfigurationBuilder();
    Level level = getLogLevel(logLevel);
    configBuilder.setStatusLevel(level);
    configBuilder.setConfigurationName("DefaultLogger");
    RootLoggerComponentBuilder rootLogger = configBuilder.newRootLogger(level);

    if (doConsoleLog) {
      AppenderComponentBuilder appenderBuilder = configBuilder
          .newAppender("Console", "CONSOLE")
          .addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
      appenderBuilder.add(configBuilder.newLayout("PatternLayout").addAttribute("pattern", pattern));
      rootLogger.add(configBuilder.newAppenderRef("Console"));
      configBuilder.add(appenderBuilder);
    }

    if (doFileLog) {
      LayoutComponentBuilder layoutBuilder =
          configBuilder.newLayout("PatternLayout").addAttribute("pattern", pattern);
      ComponentBuilder triggeringPolicy = configBuilder
          .newComponent("Policies")
          .addComponent(configBuilder
              .newComponent("SizeBasedTriggeringPolicy")
              .addAttribute("size", "10MB"));
      AppenderComponentBuilder appenderBuilder = configBuilder
          .newAppender("Roller", "ROLLINGFILE")
          .addAttribute("fileName", logPath)
          .addAttribute("filePattern", logPath + "-%d{MM-dd-yy-HH-mm-ss}.log.")
          .add(layoutBuilder)
          .addComponent(triggeringPolicy);
      configBuilder.add(appenderBuilder);
      rootLogger.add(configBuilder.newAppenderRef("Roller"));
    }
    configBuilder.add(rootLogger);
    Configurator.reconfigure(configBuilder.build());
  }
}
