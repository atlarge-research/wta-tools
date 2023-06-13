package com.asml.apa.wta.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.asml.apa.wta.core.io.ParquetSchema;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Workload;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
        .network(1L)
        .os("")
        .details("");
    assertThat(traceObjectBuilder.build().getSchemaVersion()).isEqualTo("1.0");
  }

  @Test
  void testWorkloadReturnsException() {
    ParquetSchema schemaMock = Mockito.mock(ParquetSchema.class);
    Workload sut = Mockito.spy(Workload.builder().build());
    assertThatThrownBy(() -> sut.convertToRecord(schemaMock))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Something went wrong, this method shouldn't be called!");
  }
}
