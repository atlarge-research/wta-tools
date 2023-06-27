package com.asml.apa.wta.spark.executor.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;

import com.asml.apa.wta.spark.executor.WtaExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.Test;

class WtaExecutorPluginTest {

  private final PluginContext mockedPluginContext = mock(PluginContext.class);

  private final Map<String, String> extraConf = new HashMap<>();

  private final WtaExecutorPlugin sut = spy(new WtaExecutorPlugin());

  @Test
  void wtaExecutorPluginInitialized() {
    extraConf.put("errorStatus", "false");
    extraConf.put("resourcePingInterval", "1000");
    extraConf.put("executorSynchronizationInterval", "1000");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isFalse();
  }

  @Test
  void wtaExecutorNullExtraConfResultsInError() {
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, null));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorEmptyExtraConfResultsInError() {
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorExtraConfNoErrorStatusEntryResultsInError() {
    extraConf.put("resourcePingInterval", "1000");
    extraConf.put("executorSynchronizationInterval", "1000");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorExtraConfErrorStatusTrueResultsInError() {
    extraConf.put("errorStatus", "true");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorExtraConfErrorStatusWrongValueResultsInError() {
    extraConf.put("errorStatus", "blah");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorPluginNoResourcePingIntervalEntryResultsInError() {
    extraConf.put("errorStatus", "false");
    extraConf.put("executorSynchronizationInterval", "1000");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorPluginInvalidResourcePingIntervalValueResultsInError() {
    extraConf.put("errorStatus", "false");
    extraConf.put("resourcePingInterval", "sdcd");
    extraConf.put("executorSynchronizationInterval", "1000");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorPluginNoSynchronizationIntervalEntryResultsInError() {
    extraConf.put("errorStatus", "false");
    extraConf.put("resourcePingInterval", "1000");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }

  @Test
  void wtaExecutorPluginInvalidSynchronizationIntervalValueResultsInError() {
    extraConf.put("errorStatus", "false");
    extraConf.put("resourcePingInterval", "1000");
    extraConf.put("executorSynchronizationInterval", "blah");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
  }
}
