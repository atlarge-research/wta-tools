package com.asml.apa.wta.core.utils;

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

class ParquetWriterUtilsIntegrationTest {

  private Resource resource;
  private Resource resourceAlt;
  private Task task;
  private Task taskAlt;
  private Workflow workflow;
  private Workflow workflowAlt;
  private Workload workload;
  private ParquetWriterUtils utils;
  List<Resource> resources;
  List<Resource> resourcesAlt;
  List<Task> tasks;
  List<Task> tasksAlt;
  List<Workflow> workflows;
  List<Workflow> workflowsAlt;

  @BeforeEach
  void init() {
    resources = new ArrayList<>();
    resourcesAlt = new ArrayList<>();
    tasks = new ArrayList<>();
    workflows = new ArrayList<>();

    utils = new ParquetWriterUtils(new File("./src/test/resources/WTA"), "schema-1.0");
  }

  @Test
  void writeToFileTest() {
    Resource.ResourceBuilder resourceBuilder = Resource.builder()
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
    Task.TaskBuilder taskBuilder = Task.builder()
        .nfrs("test")
        .children(new long[1])
        .parents(new long[1])
        .type("test")
        .params("test")
        .resourceType("test");
    task = taskBuilder.build();
    Task[] tArray = new Task[1];
    tArray[0] = task;
    Workflow.WorkflowBuilder workflowBuilder = Workflow.builder()
        .applicationField("test")
        .applicationName("test")
        .scheduler("test")
        .nfrs("test")
        .tasks(tArray);
    workflow = workflowBuilder.build();
    System.out.println(workflow);
    Workload.WorkloadBuilder workloadBuilder = Workload.builder();
    workload = workloadBuilder.build();
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
      new File("./src/test/resources/WTA/workloads/schema-1.0/test4.json").delete();
    });
  }

  @Test
  void writeToFileAltTest() {
    Resource.ResourceBuilder resourceBuilderAlt =
        Resource.builder().diskSpace(-1).numResources(-1.0).memory(-1).networkSpeed(-1);
    resourceAlt = resourceBuilderAlt.build();
    Task.TaskBuilder taskBuilderAlt = Task.builder()
        .groupId(-1)
        .diskIoTime(-1)
        .energyConsumption(-1)
        .diskSpaceRequested(-1.0)
        .memoryRequested(-1.0)
        .networkIoTime(-1)
        .resourceAmountRequested(-1.0)
        .runtime(-1)
        .resourceUsed(-1)
        .submissionSite(-1)
        .submitType(-1)
        .userId(-1)
        .waitTime(-1)
        .workflowId(-1);
    taskAlt = taskBuilderAlt.build();
    Workflow.WorkflowBuilder workflowBuilderAlt = Workflow.builder()
        .criticalPathLength(-1)
        .criticalPathTaskCount(-1)
        .numberOfTasks(-1)
        .submitTime(-1)
        .maxNumberOfConcurrentTasks(-1)
        .totalDiskSpaceUsage(-1)
        .totalEnergyConsumption(-1)
        .totalMemoryUsage(-1.0)
        .totalNetworkUsage(-1)
        .totalResources(-1.0);
    workflowAlt = workflowBuilderAlt.build();
    Workload.WorkloadBuilder workloadBuilder = Workload.builder();
    workload = workloadBuilder.build();
    for (int i = 1; i < 1000; i++) {
      utils.readResource(resourceAlt);
    }
    utils.readTask(taskAlt);
    utils.readWorkflow(workflowAlt);
    utils.readWorkload(workload);
    Assertions.assertDoesNotThrow(() -> {
      utils.writeToFile("test1", "test2", "test3", "test4");
      new File("./src/test/resources/WTA/resources/schema-1.0/test1.parquet").delete();
      new File("./src/test/resources/WTA/tasks/schema-1.0/test2.parquet").delete();
      new File("./src/test/resources/WTA/workflows/schema-1.0/test3.parquet").delete();
      new File("./src/test/resources/WTA/workloads/schema-1.0/test4.json").delete();
    });
  }
}
