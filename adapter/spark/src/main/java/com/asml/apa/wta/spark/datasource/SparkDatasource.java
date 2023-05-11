package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.StageInfo;

/**
 * This class is a Stage data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class SparkDatasource {

  private final SparkContext sparkContext;
  private final TaskLevelListener taskLevelListener;
  private final StageLevelListener stageLevelListener;

  /**
   *
   *
   * @param sparkContext
   */
  public SparkDatasource(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
    taskLevelListener = new TaskLevelListener();
    stageLevelListener = new StageLevelListener();
  }

  /**
   * This method registers a task listener to the Spark context.
   */
  public void registerTaskListener() {
    sparkContext.addSparkListener(taskLevelListener);
  }

  /**
   * This method removes a task listener from the Spark context.
   */
  public void removeTaskListener() {
    sparkContext.removeSparkListener(taskLevelListener);
  }

  /**
   * This method registers a stage listener to the Spark context.
   */
  public void registerStageListener() {
    sparkContext.addSparkListener(stageLevelListener);
  }

  /**
   * This method removes a stage listener from the Spark context.
   */
  public void removeStageListener() {
    sparkContext.removeSparkListener(stageLevelListener);
  }

  /**
   * This method returns a list of task metrics.
   *
   * @return List<TaskMetrics>    List of task metrics
   */
  public List<TaskMetrics> getTaskMetrics() {
    return taskLevelListener.taskMetricsList;
  }

  /**
   * This method returns a list of stage information.
   *
   * @return List<StageInfo>      List of stage information
   */
  public List<StageInfo> getStageInfo() {
    return stageLevelListener.stageInfoList;
  }
}
