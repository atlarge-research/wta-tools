package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.utils.WtaUtils;
import com.asml.apa.wta.spark.driver.WtaDriverPlugin;
import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class WtaDriverPluginTest {

  protected SparkContext mockedSparkContext;

  protected PluginContext mockedPluginContext;

  protected WtaDriverPlugin sut;

  protected WtaUtils wtaUtils;

  @BeforeEach
  void setup() {
    sut = Mockito.spy(new WtaDriverPlugin());
    mockedSparkContext = mock(SparkContext.class);
    wtaUtils = mock(WtaUtils.class);
  }

  void injectConfig() {
    System.setProperty("configFile", "src/test/resources/config.json");
  }

  @Test
  void wtaDriverPluginInitialized() {
    injectConfig();
    assertThat(sut.isError()).isFalse();
    assertThat(sut.init(mockedSparkContext, mockedPluginContext))
        .containsKeys("executorSynchronizationInterval", "resourcePingInterval");
    assertThat(sut.getSparkDataSource()).isNotNull();
    assertThat(sut.getParquetUtil()).isNotNull();
    assertThat(sut.isError()).isFalse();
    verify(sut, times(0)).shutdown();
    verify(sut, times(1)).initListeners();
    verify(sut, times(0)).removeListeners();
  }

  @Test
  void wtaDriverPluginInitializeThrowsException() {
    System.setProperty("configFile", "non-existing-file.json");
    assertThat(sut.isError()).isFalse();
    sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(sut.getSparkDataSource()).isNull();
    assertThat(sut.getParquetUtil()).isNull();
    assertThat(sut.isError()).isTrue();
    verify(sut, times(1)).shutdown();
    verify(sut, times(0)).initListeners();
    verify(sut, times(0)).removeListeners();
  }

  @Test
  void wtaDriverPluginDoesNotInitializeWithNegativeResourceTimer() {
    System.setProperty("configFile", "testConfigNegativeResourcePingInterval.json");
    assertThat(sut.isError()).isFalse();
    Map<String, String> result = sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(sut.isError()).isTrue();
    assertThat(result).isEmpty();
  }

  @Test
  void wtaDriverPluginShutdown() {
    assertThat(sut.isError()).isFalse();
    sut.shutdown();
    verify(sut, times(1)).removeListeners();
  }

  @Test
  void wtaDriverPluginShutdownFromError() {
    System.setProperty("configFile", "non-existing-file.json");
    sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(sut.isError()).isTrue();
    sut.shutdown();
    verify(sut, times(0)).removeListeners();
  }

  @Test
  void receiveAddsIntoMetricStreamCorrectly() {
    injectConfig();
    sut.init(mockedSparkContext, mockedPluginContext);
    sut.receive(new ResourceCollectionDto(
        List.of(new SparkBaseSupplierWrapperDto("1"), new SparkBaseSupplierWrapperDto("2"))));
    assertThat(sut.getMetricStreamingEngine()
            .getResourceStream()
            .getStreams()
            .get("1")
            .isEmpty())
        .isFalse();
    assertThat(sut.getMetricStreamingEngine()
            .getResourceStream()
            .getStreams()
            .get("2")
            .isEmpty())
        .isFalse();
  }
}
