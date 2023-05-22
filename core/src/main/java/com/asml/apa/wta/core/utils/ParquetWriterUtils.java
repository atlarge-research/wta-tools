package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.google.gson.Gson;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

/**
 * Utility class for reading trace objects and writing them to the disk.
 * @since 1.0.0
 * @author Tianchen Qu
 */
public class ParquetWriterUtils {

  private String version;

  private File path;

  private List<Resource> resources;

  private List<Task> tasks;

  private List<Workflow> workflows;

  private Workload workload = null;

  public ParquetWriterUtils(File path, String version) {
    resources = new ArrayList<>();
    tasks = new ArrayList<>();
    workflows = new ArrayList<>();
    this.path = path;
    this.version = version;
  }

  /**
   * Getter for resources, only for tests.
   *
   * @return the resources
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public List<Resource> getResources() {
    return resources;
  }

  /**
   * Getter for tasks, only for tests.
   *
   * @return the tasks
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public List<Task> getTasks() {
    return tasks;
  }

  /**
   * Getter for workflows, only for tests.
   *
   * @return the workflows
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public List<Workflow> getWorkflows() {
    return workflows;
  }

  /**
   * Getter for workloads, only for tests.
   *
   * @return the workloads
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public Workload getWorkload() {
    return workload;
  }

  /**
   * Reads the resource object from kafka stream and feed into the writer.
   *
   * @param resource the resource
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public void readResource(Resource resource) {
    resources.add(resource);
  }

  /**
   * Reads the task object from kafka.
   *
   * @param task the task
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public void readTask(Task task) {
    tasks.add(task);
  }

  /**
   * Reads the workflow object from kafka.
   *
   * @param workflow the workflow
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public void readWorkflow(Workflow workflow) {
    workflows.add(workflow);
  }

  /**
   * Reads the workload object from kafka.
   *
   * @param workload the workload
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public void readWorkload(Workload workload) {
    this.workload = workload;
  }

  /**
   * Given the output name, output the trace.
   *
   * @param resourceFileName resource file name
   * @param taskFileName task file name
   * @param workflowFileName workflow file name
   * @param workloadFileName workload file name
   * @throws Exception possible exception due to io
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public void writeToFile(
      String resourceFileName, String taskFileName, String workflowFileName, String workloadFileName)
      throws Exception {
    writeResourceToFile(resourceFileName);
    writeTaskToFile(taskFileName);
    writeWorkflowToFile(workflowFileName);
    writeWorkloadToFile(workloadFileName);
  }

  /**
   * Writer for resource object.
   *
   * @param resourceFileName name of the resource file
   * @throws Exception possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  private void writeResourceToFile(String resourceFileName) throws Exception {
    Boolean[] checker = checkResourceDomain(resources);
    SchemaBuilder.FieldAssembler<Schema> fieldSchema = SchemaBuilder.record("resource")
        .namespace("com.asml.apa.wta.core.model")
        .fields();
    if (checker[0] == true) {
      fieldSchema = fieldSchema.name("id").type().longType().noDefault();
    }
    if (checker[1] == true) {
      fieldSchema =
          fieldSchema.name("type").type().nullable().stringType().stringDefault("test");
    }
    if (checker[2] == true) {
      fieldSchema = fieldSchema.name("numResources").type().doubleType().doubleDefault(0.0);
    }
    if (checker[3] == true) {
      fieldSchema =
          fieldSchema.name("procModel").type().nullable().stringType().stringDefault("test");
    }
    if (checker[4] == true) {
      fieldSchema = fieldSchema.name("memory").type().longType().longDefault(0);
    }
    if (checker[5] == true) {
      fieldSchema = fieldSchema.name("diskSpace").type().longType().longDefault(0);
    }
    if (checker[6] == true) {
      fieldSchema = fieldSchema.name("networkSpeed").type().longType().longDefault(0);
    }
    if (checker[7] == true) {
      fieldSchema = fieldSchema.name("os").type().nullable().stringType().stringDefault("test");
    }
    if (checker[8] == true) {
      fieldSchema =
          fieldSchema.name("details").type().nullable().stringType().stringDefault("test");
    }
    Schema schema = fieldSchema.endRecord();
    AvroUtils resourceWriter =
        new AvroUtils(schema, new File(path, "/resources/" + version + "/" + resourceFileName + ".parquet"));
    List<GenericRecord> resourceList = new ArrayList<>();
    for (Resource resource : resources) {
      resourceList.add(Resource.convertResourceToRecord(resource, checker, schema));
    }
    resourceWriter.writeRecords(resourceList);
    resourceWriter.close();
  }

  /**
   * Writer for the task object.
   *
   * @param taskFileName name of the task file
   * @throws Exception possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  private void writeTaskToFile(String taskFileName) throws Exception {
    Boolean[] checker = checkTaskDomain(tasks);
    SchemaBuilder.FieldAssembler<Schema> fieldSchema = SchemaBuilder.record("resource")
        .namespace("com.asml.apa.wta.core.model")
        .fields();
    if (checker[0] == true) {
      fieldSchema = fieldSchema.name("id").type().longType().noDefault();
    }
    if (checker[1] == true) {
      fieldSchema =
          fieldSchema.name("type").type().nullable().stringType().stringDefault("test");
    }
    if (checker[2] == true) {
      fieldSchema = fieldSchema.name("submitType").type().longType().longDefault(0);
    }
    if (checker[3] == true) {
      fieldSchema = fieldSchema.name("submissionSite").type().intType().intDefault(0);
    }
    if (checker[4] == true) {
      fieldSchema = fieldSchema.name("runtime").type().longType().longDefault(0);
    }
    if (checker[5] == true) {
      fieldSchema = fieldSchema
          .name("resourceType")
          .type()
          .nullable()
          .stringType()
          .stringDefault("test");
    }
    if (checker[6] == true) {
      fieldSchema = fieldSchema
          .name("resourceAmountRequested")
          .type()
          .doubleType()
          .doubleDefault(0.0);
    }
    if (checker[7] == true) {
      fieldSchema = fieldSchema
          .name("parents")
          .type()
          .nullable()
          .array()
          .items()
          .longType()
          .arrayDefault(new ArrayList<Long>());
    }
    if (checker[8] == true) {
      fieldSchema = fieldSchema
          .name("children")
          .type()
          .nullable()
          .array()
          .items()
          .longType()
          .arrayDefault(new ArrayList<Long>());
    }
    if (checker[9] == true) {
      fieldSchema = fieldSchema.name("userId").type().intType().intDefault(0);
    }
    if (checker[10] == true) {
      fieldSchema = fieldSchema.name("groupId").type().intType().intDefault(0);
    }
    if (checker[11] == true) {
      fieldSchema =
          fieldSchema.name("nfrs").type().nullable().stringType().stringDefault("test");
    }
    if (checker[12] == true) {
      fieldSchema = fieldSchema.name("workflowId").type().longType().longDefault(0);
    }
    if (checker[13] == true) {
      fieldSchema = fieldSchema.name("waitTime").type().longType().longDefault(0);
    }
    if (checker[14] == true) {
      fieldSchema =
          fieldSchema.name("params").type().nullable().stringType().stringDefault("test");
    }
    if (checker[15] == true) {
      fieldSchema =
          fieldSchema.name("memoryRequested").type().doubleType().doubleDefault(0.0);
    }
    if (checker[16] == true) {
      fieldSchema = fieldSchema.name("networkIoTime").type().longType().longDefault(0);
    }
    if (checker[17] == true) {
      fieldSchema = fieldSchema.name("diskIoTime").type().longType().longDefault(0);
    }
    if (checker[18] == true) {
      fieldSchema =
          fieldSchema.name("diskSpaceRequested").type().doubleType().doubleDefault(0.0);
    }
    if (checker[19] == true) {
      fieldSchema =
          fieldSchema.name("energyConsumption").type().longType().longDefault(0);
    }
    if (checker[20] == true) {
      fieldSchema = fieldSchema.name("resourceUsed").type().longType().longDefault(0);
    }
    Schema schema = fieldSchema.endRecord();
    AvroUtils taskWriter =
        new AvroUtils(schema, new File(path, "/tasks/" + version + "/" + taskFileName + ".parquet"));
    List<GenericRecord> taskList = new ArrayList<>();
    for (Task task : tasks) {
      taskList.add(Task.convertTaskToRecord(task, checker, schema));
    }
    taskWriter.writeRecords(taskList);
    taskWriter.close();
  }

  /**
   * Writer for the workflow object.
   *
   * @param workflowFileName name of the workflow file
   * @throws Exception possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  private void writeWorkflowToFile(String workflowFileName) throws Exception {
    Boolean[] checker = checkWorkflowDomain(workflows);
    SchemaBuilder.FieldAssembler<Schema> fieldSchema = SchemaBuilder.record("resource")
        .namespace("com.asml.apa.wta.core.model")
        .fields();
    if (checker[0] == true) {
      fieldSchema = fieldSchema.name("id").type().longType().noDefault();
    }
    if (checker[1] == true) {
      fieldSchema = fieldSchema.name("submitTime").type().longType().noDefault();
    }
    if (checker[2] == true) {
      fieldSchema = fieldSchema
          .name("tasks")
          .type()
          .nullable()
          .array()
          .items()
          .longType()
          .noDefault();
    }
    if (checker[3] == true) {
      fieldSchema = fieldSchema.name("numberOfTasks").type().intType().noDefault();
    }
    if (checker[4] == true) {
      fieldSchema =
          fieldSchema.name("criticalPathLength").type().intType().noDefault();
    }
    if (checker[5] == true) {
      fieldSchema =
          fieldSchema.name("criticalPathTaskCount").type().intType().noDefault();
    }
    if (checker[6] == true) {
      fieldSchema = fieldSchema
          .name("maxNumberOfConcurrentTasks")
          .type()
          .intType()
          .noDefault();
    }
    if (checker[7] == true) {
      fieldSchema =
          fieldSchema.name("nfrs").type().nullable().stringType().noDefault();
    }
    if (checker[8] == true) {
      fieldSchema =
          fieldSchema.name("scheduler").type().nullable().stringType().noDefault();
    }
    if (checker[9] == true) {
      fieldSchema =
          fieldSchema.name("domain").type().nullable().stringType().noDefault();
    }
    if (checker[10] == true) {
      fieldSchema = fieldSchema
          .name("applicationName")
          .type()
          .nullable()
          .stringType()
          .noDefault();
    }
    if (checker[11] == true) {
      fieldSchema = fieldSchema
          .name("applicationField")
          .type()
          .nullable()
          .stringType()
          .noDefault();
    }
    if (checker[12] == true) {
      fieldSchema = fieldSchema.name("totalResources").type().doubleType().noDefault();
    }
    if (checker[13] == true) {
      fieldSchema =
          fieldSchema.name("totalMemoryUsage").type().doubleType().noDefault();
    }
    if (checker[14] == true) {
      fieldSchema =
          fieldSchema.name("totalNetworkUsage").type().longType().noDefault();
    }
    if (checker[15] == true) {
      fieldSchema =
          fieldSchema.name("totalDiskSpaceUsage").type().longType().noDefault();
    }
    if (checker[16] == true) {
      fieldSchema =
          fieldSchema.name("totalEnergyConsumption").type().longType().noDefault();
    }
    Schema schema = fieldSchema.endRecord();
    AvroUtils workflowWriter =
        new AvroUtils(schema, new File(path, "/workflows/" + version + "/" + workflowFileName + ".parquet"));
    List<GenericRecord> workflowList = new ArrayList<>();
    for (Workflow workflow : workflows) {
      workflowList.add(Workflow.convertWorkflowToRecord(workflow, checker, schema));
    }
    workflowWriter.writeRecords(workflowList);
    workflowWriter.close();
  }

  /**
   * Writer for the workload object.
   *
   * @param workloadFileName name of the workload file
   * @throws Exception possible io exception
   * @since 1.0.0
   * @author Tianchen Qu
   */
  private void writeWorkloadToFile(String workloadFileName) throws Exception {
    Gson gson = new Gson();
    String workloadJson = gson.toJson(workload);
    File path = new File(this.path, "/workloads/" + version);
    path.mkdirs();
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    OutputStream dir = new FileOutputStream(new File(path, workloadFileName + ".json"));
    writer.write(workloadJson.getBytes());
    writer.writeTo(dir);
    writer.flush();
  }

  /**
   * Checks whether there are objects with uninitialized field, we will skip these columns in the output parquet file.
   * @param resources resources
   * @return a boolean array indicating what column to skip
   */
  private Boolean[] checkResourceDomain(List<Resource> resources) {
    Boolean[] flg = new Boolean[9];
    for (int i = 0; i < 9; i++) {
      flg[i] = true;
    }
    for (Resource resource : resources) {
      if (resource.getId() == -1) {
        flg[0] = false;
      }
      if (resource.getType() == null) {
        flg[1] = false;
      }
      if (resource.getNumResources() == -1.0) {
        flg[2] = false;
      }
      if (resource.getProcModel() == null) {
        flg[3] = false;
      }
      if (resource.getMemory() == -1) {
        flg[4] = false;
      }
      if (resource.getDiskSpace() == -1) {
        flg[5] = false;
      }
      if (resource.getNetworkSpeed() == -1) {
        flg[6] = false;
      }
      if (resource.getOs() == null) {
        flg[7] = false;
      }
      if (resource.getDetails() == null) {
        flg[8] = false;
      }
    }
    return flg;
  }

  /**
   * Checks whether there are objects with uninitialized field, we will skip these columns in the output parquet file.
   * @param tasks tasks
   * @return a boolean array indicating what column to skip
   */
  private Boolean[] checkTaskDomain(List<Task> tasks) {
    Boolean[] flg = new Boolean[21];
    for (int i = 0; i < 21; i++) {
      flg[i] = true;
    }
    for (Task task : tasks) {
      if (task.getId() == -1) {
        flg[0] = false;
      }
      if (task.getType() == null) {
        flg[1] = false;
      }
      if (task.getSubmitType() == -1) {
        flg[2] = false;
      }
      if (task.getSubmissionSite() == -1) {
        flg[3] = false;
      }
      if (task.getRuntime() == -1) {
        flg[4] = false;
      }
      if (task.getResourceType() == null) {
        flg[5] = false;
      }
      if (task.getResourceAmountRequested() == -1.0) {
        flg[6] = false;
      }
      if (task.getParents() == null) {
        flg[7] = false;
      }
      if (task.getChildren() == null) {
        flg[8] = false;
      }
      if (task.getUserId() == -1) {
        flg[9] = false;
      }
      if (task.getGroupId() == -1) {
        flg[10] = false;
      }
      if (task.getNfrs() == null) {
        flg[11] = false;
      }
      if (task.getWorkflowId() == -1) {
        flg[12] = false;
      }
      if (task.getWaitTime() == -1) {
        flg[13] = false;
      }
      if (task.getParams() == null) {
        flg[14] = false;
      }
      if (task.getMemoryRequested() == -1.0) {
        flg[15] = false;
      }
      if (task.getNetworkIoTime() == -1) {
        flg[16] = false;
      }
      if (task.getDiskIoTime() == -1) {
        flg[17] = false;
      }
      if (task.getDiskSpaceRequested() == -1.0) {
        flg[18] = false;
      }
      if (task.getEnergyConsumption() == -1) {
        flg[19] = false;
      }
      if (task.getResourceUsed() == -1) {
        flg[20] = false;
      }
    }
    return flg;
  }

  /**
   * Checks whether there are objects with uninitialized field, we will skip these columns in the output parquet file.
   * @param workflows workflows
   * @return a boolean array indicating what column to skip
   */
  private Boolean[] checkWorkflowDomain(List<Workflow> workflows) {
    Boolean[] flg = new Boolean[17];
    for (int i = 0; i < 17; i++) {
      flg[i] = true;
    }
    for (Workflow workflow : workflows) {
      if (workflow.getId() == -1) {
        flg[0] = false;
      }
      if (workflow.getSubmitTime() == -1) {
        flg[1] = false;
      }
      if (workflow.getTasks() == null) {
        flg[2] = false;
      }
      if (workflow.getNumberOfTasks() == -1) {
        flg[3] = false;
      }
      if (workflow.getCriticalPathLength() == -1) {
        flg[4] = false;
      }
      if (workflow.getCriticalPathTaskCount() == -1) {
        flg[5] = false;
      }
      if (workflow.getMaxNumberOfConcurrentTasks() == -1) {
        flg[6] = false;
      }
      if (workflow.getNfrs() == null) {
        flg[7] = false;
      }
      if (workflow.getScheduler() == null) {
        flg[8] = false;
      }
      if (workflow.getDomain() == null || workflow.getDomain().getValue() == null) {
        flg[9] = false;
      }
      if (workflow.getApplicationName() == null) {
        flg[10] = false;
      }
      if (workflow.getApplicationField() == null) {
        flg[11] = false;
      }
      if (workflow.getTotalResources() == -1.0) {
        flg[12] = false;
      }
      if (workflow.getTotalMemoryUsage() == -1.0) {
        flg[13] = false;
      }
      if (workflow.getTotalNetworkUsage() == -1) {
        flg[14] = false;
      }
      if (workflow.getTotalDiskSpaceUsage() == -1) {
        flg[15] = false;
      }
      if (workflow.getTotalEnergyConsumption() == -1) {
        flg[16] = false;
      }
    }
    return flg;
  }
}
