package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.TaskResourceRequest;
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

  private final Map<Integer, List<Task>> stageToTasks = new ConcurrentHashMap<>();

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
    final TaskInfo curTaskInfo = taskEnd.taskInfo();
    final TaskMetrics curTaskMetrics = taskEnd.taskMetrics();
    final long taskId = curTaskInfo.taskId() + 1;
    final String type = taskEnd.taskType();
    final long submitTime = curTaskInfo.launchTime();
    final long runTime = curTaskMetrics.executorRunTime();
    final int userId = sparkContext.sparkUser().hashCode();
    final int stageId = taskEnd.stageId();
    final long workflowId = stageIdsToJobs.get(taskEnd.stageId() + 1);

    final long[] parents = new long[0];
    final long[] children = new long[0];
    taskToStage.put(taskId, stageId);

    final double diskSpaceRequested = (double) curTaskMetrics.diskBytesSpilled()
        + curTaskMetrics.shuffleWriteMetrics().bytesWritten();
    // final double memoryRequested = curTaskMetrics.peakExecutionMemory();
    /**
     *  peakExecutionMemory is the peak memory used by internal data structures created during shuffles, aggregations
     *  and joins.
     *  The value of this accumulator should be approximately the sum of the peak sizes across all such data structures
     *  created in this task.
     *  It is thus only an upper bound of the actual peak memory for the task.
     *  For SQL jobs, this only tracks all unsafe operators and ExternalSort
     */
    final double memoryRequested = -1.0;
    final String resourceType = "N/A";
    final double resourceAmountRequested = -1.0;
    final long diskIoTime =
        curTaskMetrics.executorDeserializeTime() + curTaskMetrics.resultSerializationTime(); // unsure
    final long resourceUsed = Math.abs(curTaskInfo.executorId().hashCode());

    // unknown
    final int submissionSite = -1;
    final int groupId = -1;
    final String nfrs = "";
    final String params = "";
    final long networkIoTime = -1L;

    final double energyConsumption = -1L;
    final long waitTime = -1L;

    Task task = Task.builder()
        .id(taskId)
        .type(type)
        .submissionSite(submissionSite)
        .tsSubmit(submitTime)
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
        .build();

    final List<Task> tasks = stageToTasks.get(stageId);
    if (tasks == null) {
      List<Task> newTasks = new ArrayList<>();
      newTasks.add(task);
      stageToTasks.put(stageId, newTasks);
    } else {
      tasks.add(task);
    }
    this.getProcessedObjects().add(task);
  }

  /**
   * This method set up the parents and children of all tasks as well as the resources used by all tasks.
   *
   * @param stageListener stage level listener to get resource bindings
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public void setTasks(StageLevelListener stageListener) {
    final List<Task> tasks = this.getProcessedObjects();
    for (Task task : tasks) {
      // parent children fields
      final int stageId = this.getTaskToStage().get(task.getId());
      final Integer[] parentStages = stageListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        final Long[] parents = Arrays.stream(parentStages)
            .flatMap(parentStage ->
                Arrays.stream(stageToTasks.getOrDefault(parentStage, new ArrayList<>()).stream()
                    .map(Task::getId)
                    .toArray(Long[]::new)))
            .toArray(Long[]::new);
        task.setParents(ArrayUtils.toPrimitive(parents));
      }

      List<Integer> childrenStages = stageListener.getParentToChildren().get(stageId);
      if (childrenStages != null) {
        List<Task> children = new ArrayList<>();
        childrenStages.forEach(childrenStage -> children.addAll(stageToTasks.get(childrenStage)));
        Long[] temp = children.stream().map(Task::getId).toArray(Long[]::new);
        task.setChildren(ArrayUtils.toPrimitive(temp));
      }

      // resource related fields
      final int resourceProfileId = stageListener.getStageToResource().getOrDefault(stageId, -1);
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
