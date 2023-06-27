package com.asml.apa.wta.core.io;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.stream.Stream;
import org.junit.jupiter.api.Test;

class ParquetSchemaTest {

  @Test
  public void generateSchemaAndConvertTasks() {
    Task task = Task.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Task.class, new Stream<>(task), "tasks");

    assertDoesNotThrow(() -> task.convertToRecord(parquetSchema));
  }

  @Test
  public void generateSchemaAndConvertResources() {
    Resource resource = Resource.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Resource.class, new Stream<>(resource), "resources");

    assertDoesNotThrow(() -> resource.convertToRecord(parquetSchema));
  }

  @Test
  public void generateSchemaAndConvertWorkflows() {
    Workflow workflow = Workflow.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Workflow.class, new Stream<>(workflow), "workflows");

    assertDoesNotThrow(() -> workflow.convertToRecord(parquetSchema));
  }

  @Test
  public void generateSchemaAndConvertResourceStates() {
    ResourceState resourceState = ResourceState.builder().build();

    ParquetSchema parquetSchema =
        new ParquetSchema(ResourceState.class, new Stream<>(resourceState), "resource_states");

    assertDoesNotThrow(() -> resourceState.convertToRecord(parquetSchema));
  }
}
