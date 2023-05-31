package com.asml.apa.wta.spark.executor.plugin;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.HashMap;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

class WtaExecutorPluginTest {

  PluginContext mockPluginContext;

  WtaExecutorPlugin sut;

  @BeforeEach
  void setup() {
    mockPluginContext = mock(PluginContext.class);
    when(mockPluginContext.executorID()).thenReturn("test-executor-id");

    sut = spy(new WtaExecutorPlugin());
  }

  @AfterEach
  void tearDown() {
    sut.shutdown();
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void pingsGetSentToDriver() throws IOException {
    sut.init(mockPluginContext, new HashMap<>());
    verify(mockPluginContext, timeout(5000L).atLeastOnce()).send(any());
  }
}
