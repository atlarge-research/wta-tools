package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.driver.WtaDriverPlugin;
import com.asml.apa.wta.spark.executor.WtaExecutorPlugin;
import org.junit.jupiter.api.Test;

class WtaPluginTest {

  @Test
  void driverInitialisation() {
    WtaPlugin sut = new WtaPlugin();
    assertThat(sut.driverPlugin()).isInstanceOf(WtaDriverPlugin.class).isNotNull();
  }

  @Test
  void executorInitialisation() {
    WtaPlugin sut = new WtaPlugin();
    assertThat(sut.executorPlugin()).isInstanceOf(WtaExecutorPlugin.class).isNotNull();
  }
}
