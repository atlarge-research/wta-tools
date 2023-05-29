package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.TaskInfo;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
public class TaskLevelListener extends AbstractListener<Task> {

  @Getter
  private final Map<Integer, Integer> stageIdsToJobs = new ConcurrentHashMap<>();

  /**
   * Constructor for the task-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @author Henry Page
   * @since 1.0.0
   */
  public TaskLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
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

    final long taskId = curTaskInfo.taskId() + 1;
    final String type = taskEnd.taskType();
    final long submitTime = curTaskInfo.launchTime();
    final long runTime = curTaskMetrics.executorRunTime();
    final int userId = sparkContext.sparkUser().hashCode();
    final long workflowId = stageIdsToJobs.get(taskEnd.stageId()) + 1;

    // unknown
    final int submissionSite = -1;
    final String resourceType = "N/A";
    final double resourceAmountRequested = -1.0;
    final long[] parents = new long[0];
    final long[] children = new long[0];
    final int groupId = -1;
    final String nfrs = "";
    final String params = "";
    final double memoryRequested = -1.0;
    final long networkIoTime = -1L;
    final long diskIoTime = -1L;
    final double diskSpaceRequested = -1.0;
    final long energyConsumption = -1L;
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

  /**
   * This method is called every time a job starts.
   * In the context of the WTA, this is a workflow.
   *
   * @param jobStart The object corresponding to information on job start.
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    // stage ids are always unique
    jobStart.stageInfos().foreach(stageInfo -> stageIdsToJobs.put(stageInfo.stageId(), jobStart.jobId()));
  }

  /**
   * Callback for when a stage ends.
   *
   * @param stageCompleted The stage completion event
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    // all tasks are guaranteed to be completed, so we can remove the stage id to reduce memory usage.
    stageIdsToJobs.remove(stageCompleted.stageInfo().stageId());
  }
}
