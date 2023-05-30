package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import scala.collection.JavaConverters;

/**
 * This class is a stage-level listener for the Spark data source.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public class StageLevelListener extends AbstractListener<Task> {

  @Getter
  private final Map<Integer, Integer> stageIdsToJobs = new ConcurrentHashMap<>();

  /**
   * Constructor for the stage-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public StageLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method is called every time a stage ends, stage-level metrics should be collected here, and added.
   *
   * @param stageCompleted   SparkListenerStageCompleted The object corresponding to information on stage completion
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
    final StageInfo curStageInfo = stageCompleted.stageInfo();
    final TaskMetrics curStageMetrics = curStageInfo.taskMetrics();

    final int stageId = curStageInfo.stageId();
    final Long submitTime = (Long) curStageInfo.submissionTime().getOrElse(() -> -1L);
    final long runTime = curStageMetrics.executorRunTime();
    final int userId = sparkContext.sparkUser().hashCode();
    final long workflowId = stageIdsToJobs.get(stageId);
    Long[] parentIdArrayIntermediate =
        JavaConverters.seqAsJavaListConverter(curStageInfo.parentIds()).asJava().stream()
            .map(id -> Long.parseLong(id.toString()))
            .toArray(Long[]::new);
    final long[] parentIdLongArray = Arrays.stream(parentIdArrayIntermediate)
        .mapToLong(Long::valueOf)
        .toArray();

    // unknown
    final String type = "";
    final int submissionSite = -1;
    final String resourceType = "N/A";
    final double resourceAmountRequested = -1.0;
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
        .id(stageId)
        .type(type)
        .submissionSite(submissionSite)
        .submitTime(submitTime)
        .runtime(runTime)
        .resourceType(resourceType)
        .resourceAmountRequested(resourceAmountRequested)
        .parents(parentIdLongArray)
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
    stageIdsToJobs.remove(stageCompleted.stageInfo().stageId());
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
}
