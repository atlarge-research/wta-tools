package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.ArrayList;
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

  private final Map<Integer, Long> stageToMaximumTask = new ConcurrentHashMap<>();

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
    final int userId = sparkContext.sparkUser().hashCode();
    final long workflowId = stageIdsToJobs.get(stageId + 1);

    final Integer[] parentIds = JavaConverters.seqAsJavaList(
            curStageInfo.parentIds().toList())
        .stream()
        .map(x -> (Integer) x)
        .toArray(size -> new Integer[size]);
    stageToParents.put(stageId, parentIds);
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
    // dummy values
    final String type = "";
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
    final double energyConsumption = -1L;
    final long waitTime = -1L;
    final long resourceUsed = -1L;

    // TODO(#61): CALL EXTERNAL DEPENDENCIES

    this.getProcessedObjects()
        .add(Task.builder()
            .id(stageId)
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
    stageIdsToJobs.remove(stageCompleted.stageInfo().stageId() + 1);
  }
}
