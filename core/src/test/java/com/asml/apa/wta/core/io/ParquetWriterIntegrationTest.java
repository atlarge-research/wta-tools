package com.asml.apa.wta.core.io;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.asml.apa.wta.core.model.Task;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ParquetWriterIntegrationTest {

  @Test
  void generateAndWrite() throws IOException {
    Task task = Task.builder().id(1).build();

    OutputFile file = new DiskOutputFile(Path.of("test.parquet"));
    ParquetSchema parquetSchema = new ParquetSchema(Task.class, List.of(task), "tasks");

    ParquetWriter<Task> writer = new ParquetWriter<>(file, parquetSchema);

    writer.write(task);

    writer.close();

    assertThat(new File("test.parquet").exists()).isTrue();
    new File("test.parquet").delete();
  }
}
