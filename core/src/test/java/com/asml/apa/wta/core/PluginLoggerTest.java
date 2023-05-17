package com.asml.apa.wta.core;

import com.asml.apa.wta.core.logger.PluginLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class PluginLoggerTest {

  private Logger logger;

  @Test
  public void loggerNotNullAfterInit() {
    assertThat(logger).isNull();
    logger = PluginLogger.getInstance();
    assertThat(logger).isNotNull();
  }

  @Test
  public void loggerGetInstanceReturnsSame() {
    logger = PluginLogger.getInstance();
    assertThat(logger).isEqualTo(PluginLogger.getInstance());
  }

  @Test
  public void loggerLoadConfigWithoutGetInstanceThrowsNull() {
    assertThatNullPointerException().isThrownBy(() -> {
        PluginLogger.loadConfig(
                        "INFO",
                        false,
                        false,
                        "core/logging/app.log"
        );
        logger.info("Something");
      }
    );
  }

  @Test
  public void loggerLoadConfigAfterGetInstanceCall() {
    assertThatNoException().isThrownBy(() -> {
      logger = PluginLogger.getInstance();
      PluginLogger.loadConfig(
        "INFO",
        false,
        false,
        "core/logging/app.log"
      );
    });
  }

  @Test
  public void loggerHasCorrectLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig(
            "INFO",
            false,
            false,
            "core/logging/app.log"
    );
    assertThat(logger.getLevel()).isNotEqualTo(Level.ALL);
    assertThat(logger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void loggerHasNoAppender() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig(
            "INFO",
            false,
            false,
            "core/logging/app.log"
    );
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(0);
  }

  @Test
  public void loggerHasOnlyConsoleAppender() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig(
            "INFO",
            true,
            false,
            "core/logging/app.log"
    );
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(1);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Console")).isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Console").getClass()).isEqualTo(ConsoleAppender.class);
  }

  @Test
  public void loggerHasOnlyRollingLogAppender() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig(
            "INFO",
            false,
            true,
            "core/logging/app.log"
    );
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(1);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Roller")).isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Roller").getClass()).isEqualTo(RollingFileAppender.class);
  }

  @Test
  public void loggerHasBothAppenders() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig(
            "INFO",
            true,
            true,
            "core/logging/app.log"
    );
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(2);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Console")).isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Console").getClass()).isEqualTo(ConsoleAppender.class);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Roller")).isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Roller").getClass()).isEqualTo(RollingFileAppender.class);
  }
}
