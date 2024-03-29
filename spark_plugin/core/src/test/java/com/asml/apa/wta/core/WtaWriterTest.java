package com.asml.apa.wta.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.io.DiskOutputFile;
import com.asml.apa.wta.core.io.JsonWriter;
import com.asml.apa.wta.core.model.Workload;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class WtaWriterTest {

  @Test
  void workloadWriterWasActuallyCalled() throws IOException {
    JsonWriter<Workload> workloadWriterMock = mock(JsonWriter.class);
    WtaWriter sut = spy(new WtaWriter(
        new DiskOutputFile(Path.of("wta-output")),
        "schema-1.0",
        String.valueOf(System.currentTimeMillis()),
        "spark-plugin-v3"));
    when(sut.createWorkloadWriter()).thenReturn(workloadWriterMock);
    sut.write(Workload.builder().build());
    verify(workloadWriterMock, times(1)).write(Workload.builder().build());
  }
}
