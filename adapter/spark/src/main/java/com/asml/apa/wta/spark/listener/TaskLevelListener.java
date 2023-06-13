package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
public class TaskLevelListener extends TaskStageBaseListener {

  private final Map<Integer, List<Long>> stageToTasks = new ConcurrentHashMap<>();

  private final Map<Long, Integer> taskToStage = new ConcurrentHashMap<>();

  /**
   * Constructor for the task-level listener.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
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
   * @author Tianchen Qu
   * @since 1.0.0
   */
  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    final long taskId = taskEnd.taskInfo().taskId() + 1;
    final int stageId = taskEnd.stageId() + 1;
    taskToStage.put(taskId, stageId);

    final List<Long> tasks = stageToTasks.get(stageId);
    if (tasks == null) {
      List<Long> newTasks = new ArrayList<>();
      newTasks.add(taskId);
      stageToTasks.put(stageId, newTasks);
    } else {
      tasks.add(taskId);
    }

    final String type = taskEnd.taskType();
    final long tsSubmit = taskEnd.taskInfo().launchTime();
    final long runtime = taskEnd.taskMetrics().executorRunTime();
    final long[] parents = new long[0];
    final long[] children = new long[0];
    final int userId = sparkContext.sparkUser().hashCode();
    final long workflowId = stageIdsToJobs.get(stageId);

    // unknown
    final int submissionSite = -1;
    final String resourceType = "N/A";
    final double resourceAmountRequested = -1.0;
    final int groupId = -1;
    final String nfrs = "";
    final String params = "";
    final double memoryRequested = -1.0;
    final long networkIoTime = -1L;
    final long diskIoTime = -1L;
    final double diskSpaceRequested = -1.0;
    final double energyConsumption = -1L;
    final long waitTime = -1L;
    final long resourceUsed = -1L;

    // TODO(#61): CALL EXTERNAL DEPENDENCIES

    this.getProcessedObjects()
        .add(Task.builder()
            .id(taskId)
            .type(type)
            .submissionSite(submissionSite)
            .tsSubmit(tsSubmit)
            .runtime(runtime)
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
