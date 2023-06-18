package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
 * This class is a task-level listener for the Spark data source.
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
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @author Henry Page
   * @since 1.0.0
   */
  public TaskLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * Fills in the maps used to determine parent-child relation of Tasks at on application end.
   *
   * @param taskId    Spark Task id
   * @param stageId   Spark Stage id
   * @param wtaTask   WTA Task object
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private void fillInParentChildMaps(long taskId, long stageId, Task wtaTask) {
    taskToStage.put(taskId, stageId);
    final List<Task> sparkTasks = stageToTasks.computeIfAbsent(stageId, tasks -> new ArrayList<>());
    sparkTasks.add(wtaTask);
  }

  /**
   * This method is called every time a task ends. Task-level metrics are collected, aggregated, and added here.
   * <p>
   * Note:
   * peakExecutionMemory is the peak memory used by internal data structures created during shuffles, aggregations
   * and joins. The value of this accumulator should be approximately the sum of the peak sizes across all such
   * data structures created in this task. It is thus only an upper bound of the actual peak memory for the task.
   * For SQL jobs, this only tracks all unsafe operators and ExternalSort
   * <p>
   * Alternative:
   * final double memoryRequested = curTaskMetrics.peakExecutionMemory();
   *
   * @param taskEnd   SparkListenerTaskEnd object corresponding to information on task end
   * @author Henry Page
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
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
    final long runtime = curTaskMetrics.executorRunTime();
    final int userId = sparkContext.sparkUser().hashCode();
    final long workflowId = stageToJob.get(stageId);
    final long diskIoTime = -1L;
    final double diskSpaceRequested = (double) curTaskMetrics.diskBytesSpilled()
        + curTaskMetrics.shuffleWriteMetrics().bytesWritten();
    final long resourceUsed = Math.abs(curTaskInfo.executorId().hashCode());

    // dummy values
    final double resourceAmountRequested = -1.0;
    final int submissionSite = -1;
    final String resourceType = "N/A";
    final long[] parents = new long[0];
    final long[] children = new long[0];
    final int groupId = -1;
    final String nfrs = "";
    final long waitTime = -1L;
    final String params = "";
    final double memoryRequested = -1.0;
    final long networkIoTime = -1L;
    final double energyConsumption = -1L;

    Task task = Task.builder()
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
        .build();
    this.getProcessedObjects().add(task);
    fillInParentChildMaps(taskId, stageId, task);
  }

  /**
   * Sets the parent, child and resource fields for Spark Tasks. This method is called on job end in
   * {@link JobLevelListener#onJobEnd(SparkListenerJobEnd)} and only sets the Tasks which are
   * affiliated to the passed jobId.
   *
   * @param stageLevelListener    stage level listener to get ConcurrentHashMap containers
   * @param jobId                 Spark Job id to filter Tasks by
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void setTasks(StageLevelListener stageLevelListener, long jobId) {
    final List<Task> filteredTasks = this.getProcessedObjects().stream()
        .filter(t -> t.getWorkflowId() == jobId)
        .collect(Collectors.toList());
    for (Task task : filteredTasks) {
      // set parent field: all Tasks in are guaranteed to be in taskToStage
      final long stageId = this.getTaskToStage().get(task.getId());
      final Long[] parentStages = stageLevelListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        final long[] parents = Arrays.stream(parentStages)
            .flatMap(parentStageId -> Arrays.stream(
                this.getStageToTasks().getOrDefault(parentStageId, new ArrayList<>()).stream()
                    .map(Task::getId)
                    .toArray(Long[]::new)))
            .mapToLong(Long::longValue)
            .toArray();
        task.setParents(parents);
      }

      // set children field
      List<Long> childrenStages =
          stageLevelListener.getParentStageToChildrenStages().get(stageId);
      if (childrenStages != null) {
        List<Task> children = new ArrayList<>();
        childrenStages.forEach(
            childrenStage -> children.addAll(this.getStageToTasks().get(childrenStage)));
        long[] childrenTaskIds = children.stream()
            .map(Task::getId)
            .mapToLong(Long::longValue)
            .toArray();
        task.setChildren(childrenTaskIds);
      }

      // set resource related fields
      final int resourceProfileId =
          stageLevelListener.getStageToResource().getOrDefault(stageId, -1);
      final ResourceProfile resourceProfile =
          sparkContext.resourceProfileManager().resourceProfileFromId(resourceProfileId);
      final List<TaskResourceRequest> resources = JavaConverters.seqAsJavaList(
          resourceProfile.taskResources().values().toList());
      if (resources.size() > 0) {
        task.setResourceType(resources.get(0).resourceName());
        task.setResourceAmountRequested(resources.get(0).amount());
      }
    }
  }
}
