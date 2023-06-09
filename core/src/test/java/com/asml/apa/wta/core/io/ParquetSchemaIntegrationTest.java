package com.asml.apa.wta.core.io;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.asml.apa.wta.core.model.Task;
import java.util.List;
import org.junit.jupiter.api.Test;

class ParquetSchemaIntegrationTest {

  @Test
  public void generateSchemaAndConvert() {
    Task task = Task.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Task.class, List.of(task), "tasks");

    assertDoesNotThrow(() -> task.convertToRecord(parquetSchema));
  }
}