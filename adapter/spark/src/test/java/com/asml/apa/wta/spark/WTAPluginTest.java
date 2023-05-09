package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.Driver.WTADriverComponent;
import com.asml.apa.wta.spark.Executor.WTAExecutorComponent;
import org.junit.jupiter.api.Test;

class WTAPluginTest {

  @Test
  void driverInitialisation() {
    WTAPlugin sut = new WTAPlugin();
    assertThat(sut.driverPlugin()).isInstanceOf(WTADriverComponent.class).isNotNull();
  }

  @Test
  void executorInitialisation() {
    WTAPlugin sut = new WTAPlugin();
    assertThat(sut.executorPlugin())
        .isInstanceOf(WTAExecutorComponent.class)
        .isNotNull();
  }
}
