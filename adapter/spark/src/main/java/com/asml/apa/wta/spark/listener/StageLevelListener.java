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

  private final Map<Integer, Integer[]> stageToParents = new ConcurrentHashMap<>();

  private final Map<Integer, List<Integer>> parentToChildren = new ConcurrentHashMap<>();

  private final Map<Integer, Integer> stageToResource = new ConcurrentHashMap<>();

  private final Map<Long, List<Long>> jobToStages = new ConcurrentHashMap<>();

  public StageLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method will store the stage hierarchy information from the callback.
   * This class is a stage-level listener for the Spark data source.
   *
   * @param stageCompleted   SparkListenerStageCompleted The object corresponding to information on stage completion
   * @author Tianchen Qu
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    final StageInfo curStageInfo = stageCompleted.stageInfo();
    final TaskMetrics curStageMetrics = curStageInfo.taskMetrics();

    final int stageId = curStageInfo.stageId();
    stageToResource.put(stageId, curStageInfo.resourceProfileId());
    final Long submitTime = curStageInfo.submissionTime().getOrElse(() -> -1L);
    final long runTime = curStageMetrics.executorRunTime();
    final int userId = Math.abs(sparkContext.sparkUser().hashCode());
    final long workflowId = stageIdsToJobs.get(stageId + 1);

    final Integer[] parentIds = JavaConverters.seqAsJavaList(
            curStageInfo.parentIds().toList())
        .stream()
        .map(parentId -> (Integer) parentId)
        .toArray(Integer[]::new);
    for (Integer id : parentIds) {
      List<Integer> children = parentToChildren.get(id);
      if (children == null) {
        children = new ArrayList<>();
        children.add(stageId);
        parentToChildren.put(id, children);
      } else {
        children.add(stageId);
      }
    }
    final double diskSpaceRequested = (double) curStageMetrics.diskBytesSpilled()
        + curStageMetrics.shuffleWriteMetrics().bytesWritten();
    /**
     * alternative:
     *
     * final double memoryRequested = curTaskMetrics.peakExecutionMemory();
     *
     *  peakExecutionMemory is the peak memory used by internal data structures created during shuffles,
     *  aggregations and joins.
     *  The value of this accumulator should be approximately the sum of the peak sizes across all such data structures
     *  created in this task.
     *  It is thus only an upper bound of the actual peak memory for the task.
     *  For SQL jobs, this only tracks all unsafe operators and ExternalSort
     */
    final double memoryRequested = -1.0;
    long[] parents =
        Arrays.stream(parentIds).mapToLong(parentId -> parentId + 1).toArray();
    if (!config.isStageLevel()) {
      stageToParents.put(stageId, parentIds);
    }
    final long[] children = new long[0];
    // dummy values
    final String type = "";
    final int submissionSite = -1;
    final String resourceType = "N/A";
    final double resourceAmountRequested = -1.0;

    final int groupId = -1;
    final String nfrs = "";
    final String params = "";

    final long networkIoTime = -1L;
    final long diskIoTime = -1L;

    final double energyConsumption = -1L;
    final long waitTime = -1L;
    final long resourceUsed = -1L;

    this.getProcessedObjects()
        .add(Task.builder()
            .id(stageId)
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
            .build());
    stageIdsToJobs.remove(stageCompleted.stageInfo().stageId() + 1);
  }

  /**
   * This method sets up the stage children, and it shall be called on application end.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public void setStages() {
    final List<Task> stages = this.getProcessedObjects();
    for (Task stage : stages) {
      stage.setChildren(parentToChildren.getOrDefault(Math.toIntExact(stage.getId()), new ArrayList<>()).stream()
          .mapToLong(childrenId -> childrenId + 1)
          .toArray());
    }
  }
}
