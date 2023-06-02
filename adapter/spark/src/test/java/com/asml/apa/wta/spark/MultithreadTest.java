package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import com.asml.apa.wta.spark.executor.engine.SparkSupplierExtractionEngine;
import com.asml.apa.wta.spark.executor.plugin.WtaExecutorPlugin;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class MultithreadTest {

  PluginContext mockPluginContext;
  SparkSupplierExtractionEngine sutSupplierExtractionEngine;

  WtaExecutorPlugin sutExecutorPlugin;

  @BeforeEach
  void setup() {
    mockPluginContext = mock(PluginContext.class);
    when(mockPluginContext.executorID()).thenReturn("test-executor-id");

    sutSupplierExtractionEngine = spy(new SparkSupplierExtractionEngine(mockPluginContext));

    sutExecutorPlugin = spy(new WtaExecutorPlugin());
  }

  @Test
  void startAndStopPingingWorksAsIntended() {
    sutSupplierExtractionEngine.startPinging(1000);

    verify(sutSupplierExtractionEngine, timeout(10000L).atLeast(4)).ping();

    assertThat(sutSupplierExtractionEngine.getBuffer()).hasSize(3);

    sutSupplierExtractionEngine.stopPinging();
  }

  @Test
  @Timeout(value = 1000L, unit = TimeUnit.MILLISECONDS)
  void pingWorksAsIntended() {
    CompletableFuture<Void> result = sutSupplierExtractionEngine.ping();

    result.join();

    List<SparkBaseSupplierWrapperDto> buffer = sutSupplierExtractionEngine.getAndClear();
    assertThat(buffer).hasSize(1);
    assertThat(sutSupplierExtractionEngine.getBuffer()).hasSize(0);

    SparkBaseSupplierWrapperDto testObj = buffer.get(0);

    assertThat(testObj.getExecutorId()).isEqualTo("test-executor-id");
    assertThat(testObj.getOsInfoDto().getAvailableProcessors()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void pingsGetSentToDriver() throws IOException {
    sutExecutorPlugin.init(mockPluginContext, new HashMap<>());
    verify(mockPluginContext, timeout(15000L).atLeastOnce()).send(any());
    sutExecutorPlugin.shutdown();
  }
}
