package com.asml.apa.wta.core.logger;

import com.asml.apa.wta.core.config.RuntimeConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.rolling.action.Duration;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

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
   * Sets up the logging configuration.
   *
   * @param logLevel the logging level to be used
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public static void setUpLoggingConfig(Level logLevel) {
    ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

    LayoutComponentBuilder layoutBuilder = builder.newLayout("PatternLayout")
        .addAttribute("pattern", "%d{yyyy-MM-dd HH:mm:ss.SSS} [%-5p] [%t] %C{3}.%M(%F:%L) - %m%n");
    AppenderComponentBuilder consoleAppenderBuilder =
        builder.newAppender("consoleLogger", "console").add(layoutBuilder);
    ComponentBuilder<?> policyBuilder = builder.newComponent("Policies")
        .addComponent(builder.newComponent("TimeBasedTriggeringPolicy")
            .addAttribute("interval", 1)
            .addAttribute("modulate", true))
        .addComponent(builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "10MB"));
    ComponentBuilder<?> strategyBuilder = builder.newComponent("DefaultRolloverStrategy")
        .addAttribute("max", 10)
        .addAttribute("min", 1)
        .addAttribute("fileIndex", "min")
        .addComponent(builder.newComponent("Delete")
            .addAttribute("basePath", "log")
            .addAttribute("maxDepth", 10)
            .addComponent(
                builder.newComponent("IfLastModified").addAttribute("age", Duration.parse("365d"))));
    AppenderComponentBuilder fileAppenderBuilder = builder.newAppender("fileLogger", "RollingFile")
        .addAttribute("fileName", "logs/wta.log")
        .addAttribute("filePattern", "logs/wta_%d{yyyyMMdd}_%i.log")
        .add(layoutBuilder)
        .addComponent(policyBuilder)
        .addComponent(strategyBuilder);
    builder.add(consoleAppenderBuilder).add(fileAppenderBuilder);

    RootLoggerComponentBuilder consoleComponent = builder.newRootLogger(logLevel)
        .add(builder.newAppenderRef("consoleLogger"))
        .add(builder.newAppenderRef("fileLogger"));
    builder.add(consoleComponent);

    Configuration configuration = builder.build();

    Configurator.initialize(configuration);
    Configurator.setRootLevel(logLevel);
  }

  /**
   * Sets up the parts of the logging configuration that are set by user input.
   * Sets the logging level in accordance with the user input in config.json.
   *
   * @param config the runtime configuration to be used
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public static void setUpLoggingConfig(RuntimeConfig config) {
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
    setUpLoggingConfig(logLevel);
  }
}
