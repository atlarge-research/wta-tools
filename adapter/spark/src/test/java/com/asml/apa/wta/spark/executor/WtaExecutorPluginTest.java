package com.asml.apa.wta.spark.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.plugin.PluginContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class WtaExecutorPluginTest {

  @Mock
  private Logger log;

  @Mock
  private SparkSupplierExtractionEngine engine;

  private final PluginContext mockedPluginContext = mock(PluginContext.class);

  private final Map<String, String> extraConf = new HashMap<>();

  @InjectMocks
  private WtaExecutorPlugin sut = new WtaExecutorPlugin();

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
    verify(log).error(anyString());
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
    verifyNoInteractions(engine);
  }

  @Test
  void wtaExecutorPluginNoSynchronizationIntervalEntryResultsInError() {
    extraConf.put("errorStatus", "false");
    extraConf.put("resourcePingInterval", "1000");
    assertThat(sut.isError()).isFalse();
    assertDoesNotThrow(() -> sut.init(mockedPluginContext, extraConf));
    assertThat(sut.isError()).isTrue();
    verify(log).error(anyString());
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
