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
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import scala.collection.JavaConverters;

/**
 * This class is a stage-level listener for the Spark data source.
 *
 * @author Tianchen Qu
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Getter
public class StageLevelListener extends TaskStageBaseListener {

  private final Map<Long, Long[]> stageToParents = new ConcurrentHashMap<>();

  private final Map<Long, List<Long>> parentStageToChildrenStages = new ConcurrentHashMap<>();

  private final Map<Long, Integer> stageToResource = new ConcurrentHashMap<>();

  /**
   * Constructor for the stage-level listener from Spark datasource class.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public StageLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method will store the stage hierarchy information from the callback, depending on the isStageLevel config.
   * Only the parents of the current stage is determined here. The children are determined on application end in
   * ApplicationLevelListener.
   * @param stageId           Current stage id
   * @param task              Task object of current stage metrics
   * @param curStageInfo      Current stage info
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void fillInParentChildMaps(long stageId, Task task, StageInfo curStageInfo) {
    final long[] parentStageIds = JavaConverters.seqAsJavaList(
            curStageInfo.parentIds().toList())
        .stream()
        .mapToLong(parentId -> (Integer) parentId + 1)
        .toArray();
    if (getConfig().isStageLevel()) {
      task.setParents(parentStageIds);
    } else {
      stageToParents.put(stageId, Arrays.stream(parentStageIds).boxed().toArray(Long[]::new));
    }

    // Add current task as child to its parent stages
    for (Long parentStageId : parentStageIds) {
      List<Long> childrenStages = parentStageToChildrenStages.get(parentStageId);
      if (childrenStages == null) {
        childrenStages = new ArrayList<>();
        childrenStages.add(stageId);
        parentStageToChildrenStages.put(parentStageId, childrenStages);
      } else {
        childrenStages.add(stageId);
      }
    }
  }

  /**
   * This method is called every time a stage is completed. Stage-level metrics are collected, aggregated,
   * and added here.
   * <p>
   * Note: peakExecutionMemory is the peak memory used by internal data structures created during
   * shuffles, aggregations and joins. The value of this accumulator should be approximately the sum of
   * the peak sizes across all such data structures created in this task. It is thus only an upper
   * bound of the actual peak memory for the task. For SQL jobs, this only tracks all unsafe operators
   * and ExternalSort
   * <p>
   * Alternative:
   * final double memoryRequested = curTaskMetrics.peakExecutionMemory();
   *
   * @param stageCompleted   SparkListenerStageCompleted The object corresponding to information on stage completion
   * @author Tianchen Qu
   * @author Lohithsai Yadala Chanchu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    final StageInfo curStageInfo = stageCompleted.stageInfo();
    final TaskMetrics curStageMetrics = curStageInfo.taskMetrics();
    final long stageId = curStageInfo.stageId() + 1;
    stageToResource.put(stageId, curStageInfo.resourceProfileId());

    final long tsSubmit = curStageInfo.submissionTime().getOrElse(() -> -1L);
    final long runtime = curStageInfo.taskMetrics().executorRunTime();
    final long[] parents = new long[0];
    final long[] children = new long[0];
    final int userId = Math.abs(getSparkContext().sparkUser().hashCode());
    final Long workflowId = getStageToJob().get(stageId);
    final double diskSpaceRequested = (double) curStageMetrics.diskBytesSpilled()
        + curStageMetrics.shuffleWriteMetrics().bytesWritten();
    // final double memoryRequested = curTaskMetrics.peakExecutionMemory();

    // dummy values
    final String type = "";
    final int submissionSite = -1;
    final String resourceType = "N/A";
    final double resourceAmountRequested = -1.0;
    final int groupId = -1;
    final String nfrs = "";
    final long waitTime = -1L;
    final String params = "";
    final double memoryRequested = -1.0;
    final long networkIoTime = -1L;
    final long diskIoTime = -1L;
    final double energyConsumption = -1L;
    final long resourceUsed = -1L;

    Task task = Task.builder()
        .id(stageId)
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
    fillInParentChildMaps(stageId, task, curStageInfo);
    addProcessedObject(task);
  }

  /**
   * Sets up the stage children, and it shall be called on job end in
   * {@link JobLevelListener#onJobEnd(SparkListenerJobEnd)}. It only sets the Stages which are
   * affiliated to the passed jobId.
   *
   * @param jobId            Spark Job id to filter Stages by
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void setStages(long jobId) {
    final List<Task> filteredStages = this.getProcessedObjects()
        .filter(task -> task.getWorkflowId() == jobId)
        .toList();
    filteredStages.forEach(stage -> stage.setChildren(
        this.getParentStageToChildrenStages().getOrDefault(stage.getId(), new ArrayList<>()).stream()
            .mapToLong(Long::longValue)
            .toArray()));
  }
}
