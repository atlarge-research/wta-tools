package com.asml.apa.wta.spark.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WtaDriverPluginTest {

  protected SparkContext sc;

  protected final PluginContext mockedPluginContext = mock(PluginContext.class);

  protected WtaDriverPlugin sut = spy(WtaDriverPlugin.class);

  /**
   * Creates a SparkConf object with the given config file. Needed to set the config file path as a
   * Spark driver option.
   *
   * @param configFile The path to the config file.
   */
  private void createSparkConf(String configFile) {
    SparkConf conf = new SparkConf()
            .setAppName("SystemTest")
            .setMaster("local")
            .set("spark.driver.extraJavaOptions", "-DconfigFile=" + configFile);
    sc = SparkSession.builder().config(conf).getOrCreate().sparkContext();
  }

  /**
   * Clears the SparkContext of all SparkConf after each test.
   */
  @AfterEach
  void tearDown() {
    sc.stop();
  }

  @Test
  void wtaDriverPluginInitialized() {
    createSparkConf("src/test/resources/config.json");
    assertThat(sut.isError()).isFalse();
    Map<String, String> configMap = sut.init(sc, mockedPluginContext);
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
  void wtaDriverPluginInitializeNonExistingConfigFilepathThrowsException() {
    createSparkConf("non-existing-file.json");
    assertThat(sut.isError()).isFalse();
    Map<String, String> configMap = sut.init(sc, mockedPluginContext);
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
    createSparkConf("src/test/resources/testConfigNegativeResourcePingInterval.json");
    assertThat(sut.isError()).isFalse();
    Map<String, String> configMap = sut.init(sc, mockedPluginContext);
    assertThat(configMap).containsKeys("errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("true");
    assertThat(sut.isError()).isTrue();
    verify(sut, times(0)).initListeners();
    verify(sut, times(0)).removeListeners();
  }

  @Test
  void receiveAddsIntoMetricStreamCorrectly() {
    createSparkConf("src/test/resources/config.json");
    sut.init(sc, mockedPluginContext);
    sut.receive(new ResourceCollectionDto(List.of(
        SparkBaseSupplierWrapperDto.builder().executorId("1").build(),
        SparkBaseSupplierWrapperDto.builder().executorId("2").build())));
    assertThat(sut.getMetricStreamingEngine().getResourceStream().onKey("1").isEmpty())
        .isFalse();
    assertThat(sut.getMetricStreamingEngine().getResourceStream().onKey("2").isEmpty())
        .isFalse();
  }
}
