package com.asml.apa.wta.core.io;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.stream.Stream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ParquetWriterIntegrationTest {

  @Test
  void generateAndWrite() throws IOException {
    Task task = Task.builder().id(1).build();

    OutputFile file = new DiskOutputFile(Path.of("test.parquet"));
    ParquetSchema parquetSchema = new ParquetSchema(Task.class, new Stream<>(task), "tasks");

    try (ParquetWriter<Task> writer = new ParquetWriter<>(file, parquetSchema)) {
      writer.write(task);
    }

    assertThat(new File("test.parquet").exists()).isTrue();
    new File("test.parquet").delete();
  }
}
