package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import java.util.List;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.StageInfo;

/**
 * This class is a Stage data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class SparkDataSource {

  @Getter
  private final SparkContext sparkContext;

  @Getter
  private final TaskLevelListener taskLevelListener;

  @Getter
  private final StageLevelListener stageLevelListener;

  /**
   * Constructor for the Spark data source. This requires a Spark context to ensure a Spark session
   * is available before the data source is initialized.
   *
   * @param sparkContext  SparkContext of the running Spark session
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public SparkDataSource(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
    taskLevelListener = new TaskLevelListener(sparkContext);
    stageLevelListener = new StageLevelListener();
  }

  /**
   * This method registers a task listener to the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void registerTaskListener() {
    sparkContext.addSparkListener(taskLevelListener);
  }

  /**
   * This method removes a task listener from the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void removeTaskListener() {
    sparkContext.removeSparkListener(taskLevelListener);
  }

  /**
   * This method registers a stage listener to the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void registerStageListener() {
    sparkContext.addSparkListener(stageLevelListener);
  }

  /**
   * This method removes a stage listener from the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void removeStageListener() {
    sparkContext.removeSparkListener(stageLevelListener);
  }

  /**
   * This method gets a list of TaskMetrics from the registered task listener.
   * @return List of task metrics
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public List<Task> getTaskMetrics() {
    return taskLevelListener.getProcessedTasks();
  }

  /**
   * This method gets a list of StageInfo from the registered stage listener.
   * @return List of StageInfo
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public List<StageInfo> getStageInfo() {
    return stageLevelListener.getStageInfoList();
  }
}
