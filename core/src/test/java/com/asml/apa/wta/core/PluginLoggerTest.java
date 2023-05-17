package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.*;

import com.asml.apa.wta.core.logger.PluginLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.junit.jupiter.api.Test;

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
      PluginLogger.loadConfig("INFO", false, false, "logging/app.log");
      logger.info("Something");
    });
  }

  @Test
  public void loggerLoadConfigAfterGetInstanceCall() {
    assertThatNoException().isThrownBy(() -> {
      logger = PluginLogger.getInstance();
      PluginLogger.loadConfig("INFO", false, false, "logging/app.log");
    });
  }

  @Test
  public void loggerHasCorrectAllLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("ALL", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.ALL);
  }

  @Test
  public void loggerHasCorrectDebugLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("DEBUG", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.DEBUG);
  }

  @Test
  public void loggerHasCorrectErrorLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("ERROR", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.ERROR);
  }

  @Test
  public void loggerHasCorrectFatalLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("FATAL", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.FATAL);
  }

  @Test
  public void loggerHasCorrectOffLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("OFF", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.OFF);
  }

  @Test
  public void loggerHasCorrectTraceLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("TRACE", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.TRACE);
  }

  @Test
  public void loggerHasCorrectInfoLogLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("INFO", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void loggerDefaultsToInfoLevel() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("UNKNOWN", false, false, "logging/app.log");
    assertThat(logger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void loggerHasNoAppender() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("INFO", false, false, "logging/app.log");
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(0);
  }

  @Test
  public void loggerHasOnlyConsoleAppender() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("INFO", true, false, "logging/app.log");
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(1);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Console"))
        .isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Console").getClass())
        .isEqualTo(ConsoleAppender.class);
  }

  @Test
  public void loggerHasOnlyRollingLogAppender() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("INFO", false, true, "logging/app.log");
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(1);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Roller"))
        .isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Roller").getClass())
        .isEqualTo(RollingFileAppender.class);
  }

  @Test
  public void loggerHasBothAppenders() {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("INFO", true, true, "logging/app.log");
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(2);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Console"))
        .isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Console").getClass())
        .isEqualTo(ConsoleAppender.class);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Roller"))
        .isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Roller").getClass())
        .isEqualTo(RollingFileAppender.class);
  }
}
