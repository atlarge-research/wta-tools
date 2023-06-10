package com.asml.apa.wta.core;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.asml.apa.wta.core.io.DiskOutputFile;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceStore;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.TaskStore;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.WorkflowStore;
import com.asml.apa.wta.core.model.Workload;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WtaWriterIntegrationTest {

  private static WtaWriter sut;

  @BeforeAll
  static void setUp() {
    OutputFile file = new DiskOutputFile(Path.of("wta-format"));
    sut = new WtaWriter(file, "schema-1.0");
  }

  @Test
  void emptyOutputFile() {
    assertThatThrownBy(() -> new WtaWriter(null, "schema-1.0"));
  }

  @Test
  void writeWorkload() {
    Workload workload = Workload.builder().build();
    sut.write(workload);
    assertThat(new File("wta-format/workload/schema-1.0/generic_information.json").exists())
        .isTrue();
    new File("wta-format/workload/schema-1.0/generic_information.json").delete();
  }

  @Test
  void writeWorkflows() {
    Workflow workflow = Workflow.builder().build();
    WorkflowStore workflows = new WorkflowStore(List.of(workflow));
    sut.write(workflows);
    assertThat(new File("wta-format/workflows/schema-1.0/workflow.parquet").exists())
        .isTrue();
    new File("wta-format/workflows/schema-1.0/workflow.parquet").delete();
  }

  @Test
  void writeTasks() {
    Task task = Task.builder().build();
    TaskStore tasks = new TaskStore(List.of(task));
    sut.write(tasks);
    assertThat(new File("wta-format/tasks/schema-1.0/task.parquet").exists())
        .isTrue();
    new File("wta-format/tasks/schema-1.0/task.parquet").delete();
  }

  @Test
  void writeResources() {
    Resource resource = Resource.builder().build();
    ResourceStore resources = new ResourceStore(List.of(resource));
    sut.write(resources);
    assertThat(new File("wta-format/resources/schema-1.0/resource.parquet").exists())
        .isTrue();
    new File("wta-format/resources/schema-1.0/resource.parquet").delete();
  }
}
