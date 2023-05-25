package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Resource;
import org.junit.jupiter.api.Test;

class TraceObjectTest {

  @Test
  void getsCorrectSchemaVersion() {
    var traceObjectBuilder = Resource.builder();
    traceObjectBuilder = traceObjectBuilder
        .id(1L)
        .type("")
        .numResources(1.0)
        .procModel("")
        .memory(1L)
        .diskSpace(1L)
        .networkSpeed(1L)
        .os("")
        .details("");
    assertThat(traceObjectBuilder.build().getSchemaVersion()).isEqualTo("1.0");
  }
}
