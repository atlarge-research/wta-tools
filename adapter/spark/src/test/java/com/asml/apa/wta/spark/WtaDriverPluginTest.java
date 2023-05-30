package com.asml.apa.wta.spark;

import com.asml.apa.wta.core.utils.WtaUtils;
import com.asml.apa.wta.spark.driver.WtaDriverPlugin;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import static org.mockito.Mockito.*;

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

  @Test
  void wtaDriverPluginInitialized() {
    System.setProperty("configFile", "src/test/resources/config.json");
    assertThat(sut.isNoError()).isTrue();
    assertThat(sut.init(mockedSparkContext, mockedPluginContext)).isEqualTo(new HashMap<>());
    assertThat(sut.getSparkDataSource()).isNotNull();
    assertThat(sut.getParquetUtil()).isNotNull();
    assertThat(sut.isNoError()).isTrue();
    verify(sut, times(0)).shutdown();
    verify(sut, times(1)).initListeners();
  }

  @Test
  void wtaDriverPluginInitializeThrowsException() {
    System.setProperty("configFile", "non-existing-file.json");
    assertThat(sut.isNoError()).isTrue();
    sut.init(mockedSparkContext, mockedPluginContext);
    assertThat(sut.getSparkDataSource()).isNull();
    assertThat(sut.getParquetUtil()).isNull();
    assertThat(sut.isNoError()).isFalse();
    verify(sut, times(1)).shutdown();
    verify(sut, times(0)).initListeners();
  }
}
