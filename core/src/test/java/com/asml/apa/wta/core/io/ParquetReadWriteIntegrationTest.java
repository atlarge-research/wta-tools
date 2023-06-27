package com.asml.apa.wta.core.io;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.stream.Stream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

public class ParquetReadWriteIntegrationTest {

  @Test
  void generateAndWrite() throws IOException {
    Workflow workflow = Workflow.builder()
        .id(1)
        .domain(Domain.SCIENTIFIC)
        .nfrs("Harry Porter")
        .build();

    Path testPath = Path.of("test.parquet");
    OutputFile file = new DiskOutputFile(testPath);
    ParquetSchema parquetSchema = new ParquetSchema(Workflow.class, new Stream<>(workflow), "workflows");

    try (ParquetWriter<Workflow> writer = new ParquetWriter<>(file, parquetSchema)) {
      writer.write(workflow);
    }

    try (ParquetReader reader = new ParquetReader(new DiskParquetInputFile(testPath))) {
      GenericRecord result = reader.read();
      assertThat(result.get("id")).isEqualTo(1L);
      assertThat(result.get("domain")).isEqualTo(new Utf8("Scientific"));
      assertThat(result.get("nfrs")).isEqualTo(new Utf8("Harry Porter"));
      assertThat(result.get("critical_path_length")).isEqualTo(-1L);
    }

    assertThat(new File("test.parquet").exists()).isTrue();
    new File("test.parquet").delete();
  }
}
