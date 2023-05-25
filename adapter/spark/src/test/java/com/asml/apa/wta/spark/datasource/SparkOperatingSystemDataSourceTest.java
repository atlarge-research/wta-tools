package com.asml.apa.wta.spark.datasource;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.asml.apa.wta.spark.dto.SparkOperatingSystemDataSourceDto;
import java.io.IOException;
import org.apache.spark.api.plugin.PluginContext;
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

  @Test
  void gatherMetrics() throws IOException {
    sut.gatherMetrics();

    verify(pluginContext).send(any(SparkOperatingSystemDataSourceDto.class));
    verifyNoInteractions(log);
  }

  @Test
  void gatherMetricsWhereSendThrowsException() throws IOException {
    doThrow(IOException.class).when(pluginContext).send(any());

    sut.gatherMetrics();

    verify(log).error(anyString());
    verifyNoMoreInteractions(log);
  }
}
