package com.asml.apa.wta.spark.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.Test;

class WtaDriverPluginTest {

  protected final SparkContext mockedSparkContext = mock(SparkContext.class);

  protected final PluginContext mockedPluginContext = mock(PluginContext.class);

  protected final WtaDriverPlugin sut = spy(new WtaDriverPlugin());

  void injectConfig() {
    System.setProperty("configFile", "src/test/resources/config.json");
  }

  @Test
  void wtaDriverPluginInitialized() {
    injectConfig();
    assertThat(sut.isError()).isFalse();
    Map<String, String> configMap = sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(configMap).containsKeys("executorSynchronizationInterval", "resourcePingInterval", "errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("false");
    assertThat(sut.getSparkDataSource()).isNotNull();
    assertThat(sut.isError()).isFalse();
    verify(sut, times(1)).initListeners();
    verify(sut, times(0)).removeListeners();
    try {
      sut.shutdown();
    } catch (Exception ignored) {
    }
    verify(sut, times(1)).removeListeners();
  }

  @Test
  void wtaDriverPluginInitializeThrowsException() {
    System.setProperty("configFile", "non-existing-file.json");
    assertThat(sut.isError()).isFalse();
    Map<String, String> configMap = sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(configMap).containsKeys("errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("true");
    assertThat(sut.getSparkDataSource()).isNull();
    assertThat(sut.isError()).isTrue();
    verify(sut, times(0)).initListeners();
    try {
      sut.shutdown();
    } catch (Exception ignored) {
    }
    verify(sut, times(0)).removeListeners();
  }

  @Test
  void wtaDriverPluginDoesNotInitializeWithNegativeResourceTimer() {
    System.setProperty("configFile", "testConfigNegativeResourcePingInterval.json");
    assertThat(sut.isError()).isFalse();
    Map<String, String> configMap = sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(configMap).containsKeys("errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("true");
    assertThat(sut.isError()).isTrue();
    verify(sut, times(0)).initListeners();
    verify(sut, times(0)).removeListeners();
  }

  @Test
  void receiveAddsIntoMetricStreamCorrectly() {
    injectConfig();
    sut.init(mockedSparkContext, mockedPluginContext);
    sut.receive(new ResourceCollectionDto(List.of(
        SparkBaseSupplierWrapperDto.builder().executorId("1").build(),
        SparkBaseSupplierWrapperDto.builder().executorId("2").build())));
    assertThat(sut.getMetricStreamingEngine()
            .getExecutorResourceStream()
            .onKey("1")
            .isEmpty())
        .isFalse();
    assertThat(sut.getMetricStreamingEngine()
            .getExecutorResourceStream()
            .onKey("2")
            .isEmpty())
        .isFalse();
  }
}
