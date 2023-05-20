package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.logger.PluginLogger;
import com.asml.apa.wta.core.utils.WtaUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.junit.jupiter.api.Test;

class PluginLoggerIntegrationTest {

  private Logger logger;

  @Test
  void readsConfigFileCorrectly() {
    RuntimeConfig cr = WtaUtils.readConfig("src/test/resources/testConfig.json");
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig(cr.getLogLevel(), cr.isDoConsoleLog(), cr.isDoFileLog(), cr.getLogPath());
    assertThat(logger.getLevel()).isEqualTo(Level.INFO);
    LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    assertThat(logContext.getConfiguration().getAppenders().size()).isEqualTo(1);
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Console"))
        .isFalse();
    assertThat(logContext.getConfiguration().getAppenders().containsKey("Roller"))
        .isTrue();
    assertThat(logContext.getConfiguration().getAppenders().get("Roller").getClass())
        .isEqualTo(RollingFileAppender.class);
  }
}
