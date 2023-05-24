package com.asml.apa.wta.spark.datasource;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.asml.apa.wta.core.logger.Log4j2Configuration;
import java.io.IOException;
import org.apache.logging.log4j.Level;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class SparkOperatingSystemDataSourceTest {

  @Mock
  Logger log;

  @Mock
  PluginContext pluginContext;

  @InjectMocks
  SparkOperatingSystemDataSource sut;

  @BeforeAll
  static void setUpLogging() {
    Log4j2Configuration.setUpLoggingConfig(Level.ERROR);
  }

  @Test
  void gatherMetrics() throws IOException {
    sut.gatherMetrics();

    verify(pluginContext).send(any(SparkOperatingSystemDataSource.Dto.class));
    verifyNoInteractions(log);
  }

  @Test
  void gatherMetricsWhereSendThrowsException() throws IOException {
    doThrow(IOException.class).when(pluginContext).send(any());

    sut.gatherMetrics();

    verify(log).error(anyString());
  }
}
