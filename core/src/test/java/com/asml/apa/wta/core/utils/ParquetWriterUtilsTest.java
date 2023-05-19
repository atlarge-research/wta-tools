package com.asml.apa.wta.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParquetWriterUtilsTest {

  private Resource resource;
  private Task task;
  private Workflow workflow;
  private Workload workload;
  private ParquetWriterUtils utils;
  List<Resource> resources;
  List<Task> tasks;
  List<Workflow> workflows;
  List<Workload> workloads;

  @BeforeEach
  void init() {
    var resourceBuilder = Resource.builder()
        .id(1)
        .type("test")
        .os("test os")
        .details("None")
        .diskSpace(2)
        .numResources(4.0)
        .memory(8)
        .networkSpeed(16)
        .procModel("test model");
    resource = resourceBuilder.build();
    resources = new ArrayList<>();
    var taskBuilder = Task.builder();
    task = taskBuilder.build();
    tasks = new ArrayList<>();
    var workflowBuilder = Workflow.builder();
    workflow = workflowBuilder.build();
    workflows = new ArrayList<>();
    var workloadBuilder = Workload.builder();
    workload = workloadBuilder.build();
    workloads = new ArrayList<>();
    utils = new ParquetWriterUtils(new File("./src/test/resources/WTA"), "schema-1.0");
  }

  @Test
  void readResourceTest() {
    resources.add(resource);
    utils.readResource(resource);
    assertThat(resources).isEqualTo(utils.getResources());
  }

  @Test
  void readTaskTest() {
    tasks.add(task);
    utils.readTask(task);
    assertThat(tasks).isEqualTo(utils.getTasks());
  }

  @Test
  void readWorkflow() {
    workflows.add(workflow);
    utils.readWorkflow(workflow);
    assertThat(workflows).isEqualTo(utils.getWorkflows());
  }

  @Test
  void readWorkload() {
    workloads.add(workload);
    utils.readWorkload(workload);
    assertThat(workloads).isEqualTo(utils.getWorkloads());
  }

  @Test
  void writeToFileTest() {
    for (int i = 1; i < 1000; i++) {
      utils.readResource(resource);
    }
    utils.readTask(task);
    utils.readWorkflow(workflow);
    utils.readWorkload(workload);
    Assertions.assertDoesNotThrow(() -> {
      utils.writeToFile("test1", "test2", "test3", "test4");
      new File("./src/test/resources/WTA/resources/schema-1.0/test1.parquet").delete();
      new File("./src/test/resources/WTA/tasks/schema-1.0/test2.parquet").delete();
      new File("./src/test/resources/WTA/workflows/schema-1.0/test3.parquet").delete();
      new File("./src/test/resources/WTA/workloads/schema-1.0/test4.parquet").delete();
    });
  }

  @Test
  void getResources() {
    resources.add(resource);
    utils.readResource(resource);
    assertThat(resources).isEqualTo(utils.getResources());
  }

  @Test
  void getTasks() {
    tasks.add(task);
    utils.readTask(task);
    assertThat(tasks).isEqualTo(utils.getTasks());
  }

  @Test
  void getWorkflows() {
    workflows.add(workflow);
    utils.readWorkflow(workflow);
    assertThat(workflows).isEqualTo(utils.getWorkflows());
  }

  @Test
  void getWorkloads() {
    workloads.add(workload);
    utils.readWorkload(workload);
    assertThat(workloads).isEqualTo(utils.getWorkloads());
  }
}
