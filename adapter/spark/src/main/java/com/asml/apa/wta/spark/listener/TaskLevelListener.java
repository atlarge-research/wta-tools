package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.TaskInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
public class TaskLevelListener extends TaskStageBaseListener {

  private final Map<String, Integer> executorResources = new ConcurrentHashMap<>();

  /**
   * Constructor for the task-level listener.
   *
   * @param sparkContext          The current spark context
   * @param config                Additional config specified by the user for the plugin
   * @author Henry Page
   * @since 1.0.0
   */
  public TaskLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  @Override
  public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
    super.onExecutorAdded(executorAdded);
    executorResources.put(executorAdded.executorId(),executorAdded.executorInfo().resourceProfileId());
  }

  /**
   * This method is called every time a task ends, task-level metrics should be collected here, and added.
   *
   * @param taskEnd   SparkListenerTaskEnd The object corresponding to information on task end
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    final TaskInfo curTaskInfo = taskEnd.taskInfo();
    final TaskMetrics curTaskMetrics = taskEnd.taskMetrics();
    final String executorId = curTaskInfo.executorId();
    final ResourceProfile resourceProfile = sparkContext.resourceProfileManager()
            .resourceProfileFromId(executorResources.get(executorId));

    final long taskId = curTaskInfo.taskId() + 1;
    final String type = taskEnd.taskType();
    final long submitTime = curTaskInfo.launchTime();
    final long runTime = curTaskMetrics.executorRunTime();
    final int userId = sparkContext.sparkUser().hashCode();
    final long workflowId = stageIdsToJobs.get(taskEnd.stageId() + 1);

    final double diskSpaceRequested = (double) curTaskMetrics.diskBytesSpilled()/1048576;
    String memoryString = ResourceProfile.MEMORY();
    System.out.println(memoryString);
    final double memoryRequested = Double.parseDouble(memoryString.substring(0,memoryString.length()-1));
    final TaskResourceRequest resourceRequest = resourceProfile.taskResources().values().head();
    final String resourceType = resourceRequest.resourceName();
    final double resourceAmountRequested = resourceRequest.amount();
    final long diskIoTime = curTaskMetrics.executorDeserializeTime() + curTaskMetrics.resultSerializationTime(); //unsure
    // unknown
    final int submissionSite = -1;

    final long[] parents = new long[0];
    final long[] children = new long[0];
    final String nfrs = "";
    final String params = "";
    final int groupId = -1;
    final long networkIoTime = -1L;

    final double energyConsumption = -1L;
    final long waitTime = -1L;
    final long resourceUsed = -1L;

    // TODO(#61): CALL EXTERNAL DEPENDENCIES

    processedObjects.add(Task.builder()
        .id(taskId)
        .type(type)
        .submissionSite(submissionSite)
        .submitTime(submitTime)
        .runtime(runTime)
        .resourceType(resourceType)
        .resourceAmountRequested(resourceAmountRequested)
        .parents(parents)
        .children(children)
        .userId(userId)
        .groupId(groupId)
        .nfrs(nfrs)
        .workflowId(workflowId)
        .waitTime(waitTime)
        .params(params)
        .memoryRequested(memoryRequested)
        .networkIoTime(networkIoTime)
        .diskIoTime(diskIoTime)
        .diskSpaceRequested(diskSpaceRequested)
        .energyConsumption(energyConsumption)
        .resourceUsed(resourceUsed)
        .build());
  }


}
