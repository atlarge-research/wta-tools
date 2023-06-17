package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * This abstract class is a base class for the task and stage level listeners.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public abstract class TaskStageBaseListener extends AbstractListener<Task> {

  @Getter
  protected final Map<Long, Long> stageToJob = new ConcurrentHashMap<>();

  /**
   * Constructor for the stage-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  protected TaskStageBaseListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method is called every time a job starts. In the context of the WTA, this is a workflow.
   *
   * @param jobStart The object corresponding to information on job start.
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    long jobId = jobStart.jobId() + 1;
    jobStart.stageInfos().foreach(stageInfo -> stageToJob.put((long) stageInfo.stageId() + 1, jobId));
  }

  /**
   * Stage to Job mapping needs to be removed in onJobEnd callback as some stages are created at job start but never
   * submitted. For ConcurrentHashMap, most thread-safe and performance-optimized way to remove entries based
   * on values is using entrySet() in combination with removeIf().
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    long jobId = jobEnd.jobId() + 1;
    stageToJob.entrySet().removeIf(entry -> entry.getValue().equals(jobId));
  }
}
