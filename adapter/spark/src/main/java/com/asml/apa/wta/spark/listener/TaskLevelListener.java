package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.model.Task;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
@RequiredArgsConstructor
public class TaskLevelListener extends SparkListener {

  private final SparkContext sparkContext;

  private Map<Integer, Integer> stageIdstoJobs = new ConcurrentHashMap<>();

  private final List<Task> processedTasks = new LinkedList<>();

  /**
   * This method is called every time a task ends, task-level metrics should be collected here, and added.
   *
   * @param taskEnd   SparkListenerTaskEnd
   */
  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    final var curTaskInfo = taskEnd.taskInfo();
    final var curTaskMetrics = taskEnd.taskMetrics();

    final var taskId = curTaskInfo.taskId();
    final var type = taskEnd.taskType();
    final var submitTime = curTaskInfo.launchTime();
    final Long runTime = curTaskMetrics.executorRunTime();
    final int userId = sparkContext.sparkUser().hashCode();
    var workflowId = stageIdstoJobs.get(taskEnd.stageId());

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

    processedTasks.add(Task.builder()
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
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    jobStart.stageInfos().foreach(stageInfo -> stageIdstoJobs.put(stageInfo.stageId(), jobStart.jobId()));
  }

  /**
   * Callback for when a stage ends.
   *
   * @param stageCompleted The stage completion event
   */
  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    stageIdstoJobs.remove(stageCompleted.stageInfo().stageId());
  }

  /**
   * This method gets a list of task metrics.
   *
   * @return  List of task metrics
   */
  public List<Task> getProcessedTasks() {
    return processedTasks;
  }
}
