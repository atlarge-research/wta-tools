package com.asml.apa.wta.core.io;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import java.util.List;
import org.junit.jupiter.api.Test;

class ParquetSchemaTest {

  @Test
  public void generateSchemaAndConvertTasks() {
    Task task = Task.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Task.class, List.of(task), "tasks");

    assertDoesNotThrow(() -> task.convertToRecord(parquetSchema));
  }

  @Test
  public void generateSchemaAndConvertResources() {
    Resource resource = Resource.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Resource.class, List.of(resource), "resources");

    assertDoesNotThrow(() -> resource.convertToRecord(parquetSchema));
  }

  @Test
  public void generateSchemaAndConvertWorkflows() {
    Workflow workflow = Workflow.builder().id(1).build();

    ParquetSchema parquetSchema = new ParquetSchema(Workflow.class, List.of(workflow), "workflows");

    assertDoesNotThrow(() -> workflow.convertToRecord(parquetSchema));
  }

  @Test
  public void generateSchemaAndConvertResourceStates() {
    ResourceState resourceState = ResourceState.builder().build();

    ParquetSchema parquetSchema = new ParquetSchema(ResourceState.class, List.of(resourceState), "resource_states");

    assertDoesNotThrow(() -> resourceState.convertToRecord(parquetSchema));
  }
}
