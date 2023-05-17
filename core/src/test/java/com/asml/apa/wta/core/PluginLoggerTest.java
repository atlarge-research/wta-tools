package com.asml.apa.wta.core;

import static com.asml.apa.wta.core.logger.PluginLogger.loadConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.asml.apa.wta.core.logger.PluginLogger;
import org.apache.logging.log4j.Logger;
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
  public void loggerSameInstance() {
    logger = PluginLogger.getInstance();
    assertThat(logger).isEqualTo(PluginLogger.getInstance());
  }

  @Test
  public void loggerLoadConfigWithoutGetInstanceCall() {
    assertThatNoException().isThrownBy(() -> PluginLogger.loadConfig("INFO", true, true));
  }

  @Test
  public void loggerLoadConfigAfterGetInstanceCall() {
    PluginLogger.loadConfig("INFO", true, true);
    logger = PluginLogger.getInstance();
    logger.info("Hello World!");
  }
}
