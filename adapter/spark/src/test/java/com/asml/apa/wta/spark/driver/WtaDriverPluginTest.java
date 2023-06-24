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
import org.junit.jupiter.api.Test;

class WtaDriverPluginTest {

  private final SparkContext mockedSparkContext = mock(SparkContext.class);

  private final PluginContext mockedPluginContext = mock(PluginContext.class);

  private final WtaDriverPlugin sut = spy(WtaDriverPlugin.class);

  /**
   * Creates a SparkConf object with the given config file. Needed to set the config filepath as a
   * Spark driver option.
   *
   * @param configFile The path to the config file.
   */
  private Map<String, String> createSparkConfAndInitialize(String configFile) {
    when(mockedSparkContext.getConf()).thenReturn(new SparkConf());
    mockedSparkContext
        .getConf()
        .setAppName("SystemTest")
        .setMaster("local")
        .set("spark.driver.extraJavaOptions", "-DconfigFile=" + configFile);
    when(mockedSparkContext.conf())
        .thenReturn(new SparkConf()
            .setAppName("SystemTest")
            .setMaster("local")
            .set("spark.driver.extraJavaOptions", "-DconfigFile=" + configFile));
    assertThat(sut.isError()).isFalse();
    return sut.init(mockedSparkContext, mockedPluginContext);
  }

  /**
   * Creates a SparkConf object with no config options set.
   */
  private Map<String, String> createSparkConfWithNoConfig() {
    when(mockedSparkContext.getConf()).thenReturn(new SparkConf());
    mockedSparkContext.getConf().setAppName("SystemTest").setMaster("local");
    when(mockedSparkContext.conf())
        .thenReturn(new SparkConf().setAppName("SystemTest").setMaster("local"));
    assertThat(sut.isError()).isFalse();
    return sut.init(mockedSparkContext, mockedPluginContext);
  }

  @Test
  void wtaDriverPluginRegistersAndRemovesTaskLevelListeners() {
    createSparkConfAndInitialize("src/test/resources/config.json");
    assertThat(sut.isError()).isFalse();
    assertThat(sut.getSparkDataSource().getRuntimeConfig().isStageLevel()).isFalse();
    verify(sut, times(1)).initListeners();

    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getTaskLevelListener());
    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getStageLevelListener());
    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getJobLevelListener());
    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getApplicationLevelListener());

    sut.shutdown();

    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getTaskLevelListener());
    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getStageLevelListener());
    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getJobLevelListener());
    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getApplicationLevelListener());
  }

  @Test
  void wtaDriverPluginRegistersAndRemovesStageLevelListeners() {
    createSparkConfAndInitialize("src/test/resources/config-stage.json");
    assertThat(sut.isError()).isFalse();

    assertThat(sut.getSparkDataSource().getRuntimeConfig().isStageLevel()).isTrue();
    verify(sut, times(1)).initListeners();

    verify(mockedSparkContext, times(0))
        .addSparkListener(sut.getSparkDataSource().getTaskLevelListener());
    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getStageLevelListener());
    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getJobLevelListener());
    verify(mockedSparkContext, times(1))
        .addSparkListener(sut.getSparkDataSource().getApplicationLevelListener());

    sut.shutdown();

    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getTaskLevelListener());
    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getStageLevelListener());
    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getJobLevelListener());
    verify(mockedSparkContext, times(1))
        .removeSparkListener(sut.getSparkDataSource().getApplicationLevelListener());
  }

  @Test
  void wtaDriverPluginInitialized() {
    Map<String, String> configMap = createSparkConfAndInitialize("src/test/resources/config.json");
    assertThat(sut.isError()).isFalse();
    assertThat(configMap).containsKeys("executorSynchronizationInterval", "resourcePingInterval", "errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("false");
    assertThat(sut.getSparkDataSource()).isNotNull();
  }

  @Test
  void wtaDriverPluginDoesNotInitializeWithNonExistingConfigFilepath() {
    Map<String, String> configMap = createSparkConfAndInitialize("non-existing-file.json");
    assertThat(sut.isError()).isTrue();
    assertThat(configMap).containsKeys("errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("true");
    assertThat(sut.getSparkDataSource()).isNull();
    verify(sut, times(0)).initListeners();
    try {
      sut.shutdown();
    } catch (Exception ignored) {
    }
  }

  @Test
  void wtaDriverPluginDoesNotInitializeWithNegativeResourceTimer() {
    Map<String, String> configMap =
        createSparkConfAndInitialize("src/test/resources/testConfigNegativeResourcePingInterval.json");
    assertThat(sut.isError()).isTrue();
    assertThat(configMap).containsKeys("errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("true");
    verify(sut, times(0)).initListeners();
    try {
      sut.shutdown();
    } catch (Exception ignored) {
    }
  }

  @Test
  void wtaDriverPluginDoesNotInitializeWithMissingConfig() {
    Map<String, String> configMap = createSparkConfWithNoConfig();
    assertThat(configMap).containsKeys("errorStatus");
    assertThat(configMap.get("errorStatus")).isEqualTo("true");
    assertThat(sut.isError()).isTrue();
    verify(sut, times(0)).initListeners();
    try {
      sut.shutdown();
    } catch (Exception ignored) {
    }
  }

  @Test
  void receiveAddsIntoMetricStreamCorrectly() {
    createSparkConfAndInitialize("src/test/resources/config.json");
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
