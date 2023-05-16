package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;

public class ParquetWriterUtils {

  private String version;
  private File path;
  private List<Resource> resources;
  private List<Task> tasks;
  private List<Workflow> workflows;
  private List<Workload> workloads;

  public ParquetWriterUtils(File path, String version) {
    resources = new ArrayList<>();
    tasks = new ArrayList<>();
    workflows = new ArrayList<>();
    workloads = new ArrayList<>();
    this.path = path;
    this.version = version;
  }

  /**getter for resources, only for tests.
   *
   * @return the resources
   */
  public List<Resource> getResources() {
    return resources;
  }

  /**reads the resource object from kafka stream and feed into the writer.
   *
   * @param resource the resource
   */
  public void readResource(Resource resource) {
    resources.add(resource);
  }

  /**reads the task object from kafka.
   *
   * @param task the task
   */
  public void readTask(Task task) {
    tasks.add(task);
  }

  /**reads the workflow object from kafka.
   *
   * @param workflow the workflow
   */
  public void readWorkflow(Workflow workflow) {
    workflows.add(workflow);
  }

  /**reads the workload object from kafka.
   *
   * @param workload the workload
   */
  public void readWorkload(Workload workload) {
    workloads.add(workload);
  }

  /**given the output name, output the trace.
   *
   * @param resourceFileName resource file name
   * @param taskFileName task file name
   * @param workflowFileName workflow file name
   * @param workloadFileName workload file name
   * @throws Exception possible exception due to io
   */
  public void writeToFile(
      String resourceFileName, String taskFileName, String workflowFileName, String workloadFileName)
      throws Exception {
    writeResourceToFile(resourceFileName);
    writeTaskToFile(taskFileName);
    writeWorkflowToFile(workflowFileName);
    writeWorkloadToFile(workloadFileName);
  }

  private void writeResourceToFile(String resourceFileName) throws Exception {
    AvroUtils resourceWriter = new AvroUtils(
        Resource.getResourceSchema(),
        new File(path, "/resources/" + version + "/" + resourceFileName + ".parquet"));
    List<GenericRecord> resourceList = new ArrayList<>();
    for (Resource resource : resources) {
      resourceList.add(Resource.convertResourceToRecord(resource));
    }
    resourceWriter.writeRecords(resourceList);
  }

  private void writeTaskToFile(String taskFileName) throws Exception {
    AvroUtils taskWriter = new AvroUtils(
        Task.getTaskSchema(), new File(path, "/tasks/" + version + "/" + taskFileName + ".parquet"));
    List<GenericRecord> taskList = new ArrayList<>();
    for (Task task : tasks) {
      taskList.add(Task.convertTaskToRecord(task));
    }
    taskWriter.writeRecords(taskList);
  }

  private void writeWorkflowToFile(String workflowFileName) throws Exception {
    AvroUtils workflowWriter = new AvroUtils(
        Workflow.getWorkflowSchema(),
        new File(path, "/workflows/" + version + "/" + workflowFileName + ".parquet"));
    List<GenericRecord> workflowList = new ArrayList<>();
    for (Workflow workflow : workflows) {
      workflowList.add(Workflow.convertWorkflowToRecord(workflow));
    }
    workflowWriter.writeRecords(workflowList);
  }

  private void writeWorkloadToFile(String workloadFileName) throws Exception {
    AvroUtils workloadWriter = new AvroUtils(
        Workload.getWorkloadSchema(),
        new File(path, "/workloads/" + version + "/" + workloadFileName + ".parquet"));
    List<GenericRecord> workloadList = new ArrayList<>();
    for (Workload workload : workloads) {
      workloadList.add(Workload.convertWorkloadToRecord(workload));
    }
    workloadWriter.writeRecords(workloadList);
  }
}
