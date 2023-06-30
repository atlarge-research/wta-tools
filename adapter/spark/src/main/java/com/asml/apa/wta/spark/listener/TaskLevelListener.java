package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.TaskInfo;
import scala.collection.JavaConverters;

/**
 * Task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
public class TaskLevelListener extends TaskStageBaseListener {

  private final Map<Long, List<Task>> stageToTasks = new ConcurrentHashMap<>();

  private final Map<Long, Long> taskToStage = new ConcurrentHashMap<>();

  /**
   * Constructor for the task-level listener.
   *
   * @param sparkContext          current spark context.
   * @param config                additional config specified by the user for the plugin.
   */
  public TaskLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * Fills in the maps used to determine parent-child relation of Tasks at on application end.
   *
   * @param taskId                Spark Task id.
   * @param stageId               Spark Stage id.
   * @param wtaTask               WTA Task object.
   */
  private void fillInParentChildMaps(long taskId, long stageId, Task wtaTask) {
    taskToStage.put(taskId, stageId);
    final List<Task> sparkTasks = stageToTasks.computeIfAbsent(stageId, tasks -> new ArrayList<>());
    sparkTasks.add(wtaTask);
  }

  /**
   * This method is called every time a task ends. Task-level metrics are collected, aggregated, and added here.
   *
   * @param taskEnd               SparkListenerTaskEnd object corresponding to information on task end.
   */
  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    final TaskInfo curTaskInfo = taskEnd.taskInfo();
    final TaskMetrics curTaskMetrics = taskEnd.taskMetrics();
    assert curTaskMetrics != null;

    final long taskId = curTaskInfo.taskId() + 1;
    final long stageId = taskEnd.stageId() + 1;

    final String type = taskEnd.taskType();
    final long tsSubmit = curTaskInfo.launchTime();
    final int userId = Math.abs(getSparkContext().sparkUser().hashCode());
    final long workflowId = getStageToJob().get(stageId);
    final long resourceUsed = Math.abs(curTaskInfo.executorId().hashCode());

    long runtime;
    double diskSpaceRequested;

    try {
      runtime = curTaskMetrics.executorRunTime();
      diskSpaceRequested = (double) curTaskMetrics.diskBytesSpilled()
          + curTaskMetrics.shuffleWriteMetrics().bytesWritten();
    } catch (RuntimeException e) {
      runtime = -1L;
      diskSpaceRequested = -1.0;
    }

    Task task = Task.builder()
        .id(taskId)
        .type(type)
        .tsSubmit(tsSubmit)
        .runtime(runtime)
        .userId(userId)
        .workflowId(workflowId)
        .diskSpaceRequested(diskSpaceRequested)
        .resourceUsed(resourceUsed)
        .build();

    fillInParentChildMaps(taskId, stageId, task);

    addTaskToWorkflow(workflowId, task);
    getThreadPool().execute(() -> addProcessedObject(task));
  }

  /**
   * Sets the parent, child and resource fields for Spark Tasks. This method is called on job end in
   * {@link JobLevelListener#onJobEnd(SparkListenerJobEnd)} and only sets the Tasks which are
   * affiliated to the passed jobId.
   *
   * @param stageLevelListener        stage-level listener to get ConcurrentHashMap containers.
   * @param jobId                     Spark Job id to filter Tasks by.
   */
  public void setTasks(StageLevelListener stageLevelListener, long jobId) {
    getWorkflowsToTasks().onKey(jobId).forEach(task -> {
      final Long nullableStageId = getTaskToStage().remove(task.getId());
      final long stageId = nullableStageId == null ? -1 : nullableStageId;
      final Long[] parentStages = stageLevelListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        final long[] parents = Arrays.stream(parentStages)
            .flatMap(parentStageId ->
                Arrays.stream(getStageToTasks().getOrDefault(parentStageId, new ArrayList<>()).stream()
                    .map(Task::getId)
                    .toArray(Long[]::new)))
            .mapToLong(Long::longValue)
            .toArray();
        task.setParents(parents);
      }

      List<Long> childrenStages =
          stageLevelListener.getParentStageToChildrenStages().get(stageId);
      if (childrenStages != null) {
        List<Task> children = new ArrayList<>();
        childrenStages.forEach(
            childrenStage -> children.addAll(getStageToTasks().get(childrenStage)));
        long[] childrenTaskIds = children.stream()
            .map(Task::getId)
            .mapToLong(Long::longValue)
            .toArray();
        task.setChildren(childrenTaskIds);
      }

      final int resourceProfileId =
          stageLevelListener.getStageToResource().getOrDefault(stageId, -1);
      if (resourceProfileId >= 0) {
        final ResourceProfile resourceProfile =
            getSparkContext().resourceProfileManager().resourceProfileFromId(resourceProfileId);
        final List<TaskResourceRequest> resources = JavaConverters.seqAsJavaList(
            resourceProfile.taskResources().values().toList());
        if (!resources.isEmpty()) {
          task.setResourceType(resources.get(0).resourceName());
          task.setResourceAmountRequested(resources.get(0).amount());
        }
      }
    });
  }
}
