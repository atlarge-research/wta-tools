package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.config.RuntimeConfig;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class RuntimeConfigTest {

  @Test
  void createsRuntimeConfigWithDefaultValues() {
    RuntimeConfig cr = RuntimeConfig.builder().build();
    assertThat(cr.getAuthors()).isNull();
    assertThat(cr.getDomain()).isNull();
    assertThat(cr.getDescription()).isEqualTo("");
    assertThat(cr.getEvents()).isEqualTo(new HashMap<String, String>());
    assertThat(cr.getLogLevel()).isEqualTo("ERROR");
  }
}
