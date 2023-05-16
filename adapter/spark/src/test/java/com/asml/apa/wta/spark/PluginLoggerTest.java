package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

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
  public void shouldNotThrowException() throws IOException {
    logger = PluginLogger.getInstance();
    PluginLogger.loadConfig("src/test/resources/log4j2.properties");
  }
}
