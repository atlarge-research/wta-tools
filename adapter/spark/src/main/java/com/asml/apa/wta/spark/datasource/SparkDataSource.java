package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.spark.listener.ApplicationLevelListener;
import com.asml.apa.wta.spark.listener.JobLevelListener;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;

/**
 * This class is a Stage data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
@Slf4j
public class SparkDataSource {

  private final TaskLevelListener taskLevelListener;

  private final StageLevelListener stageLevelListener;

  private final JobLevelListener jobLevelListener;

  private final ApplicationLevelListener applicationLevelListener;

  private final RuntimeConfig runtimeConfig;

  /**
   * Constructor for the Spark data source. This requires a Spark context to ensure a Spark session
   * is available before the data source is initialized.
   *
   * @param sparkContext  SparkContext of the running Spark session
   * @param config Additional config specified by the user for the plugin
   * @author Pil Kyu Cho
   * @author Henry Page
   * @author Tianchen Qu
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public SparkDataSource(SparkContext sparkContext, RuntimeConfig config) {
    log.trace("Initialising Spark Data Source");
    taskLevelListener = new TaskLevelListener(sparkContext, config);
    stageLevelListener = new StageLevelListener(sparkContext, config);
    if (config.isStageLevel()) {
      log.trace("Stage level listener is enabled.");
      jobLevelListener = new JobLevelListener(sparkContext, config, stageLevelListener);
      applicationLevelListener =
          new ApplicationLevelListener(sparkContext, config, stageLevelListener, jobLevelListener);
    } else {
      log.trace("Task level listener is enabled.");
      jobLevelListener = new JobLevelListener(sparkContext, config, taskLevelListener, stageLevelListener);
      applicationLevelListener = new ApplicationLevelListener(
          sparkContext, config, taskLevelListener, stageLevelListener, jobLevelListener);
    }

    runtimeConfig = config;
  }

  /**
   * This method registers a task listener to the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void registerTaskListener() {
    log.trace("Registering task listener.");
    taskLevelListener.register();
  }

  /**
   * This method removes a task listener from the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void removeTaskListener() {
    log.trace("Removing task listener.");
    taskLevelListener.remove();
  }

  /**
   * Registers a job listener to the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void registerJobListener() {
    log.trace("Registering job listener.");
    jobLevelListener.register();
  }

  /**
   * Removes a job listener from the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void removeJobListener() {
    log.trace("Removing job listener.");
    jobLevelListener.remove();
  }

  /**
   * Registers an application listener to the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void registerApplicationListener() {
    log.trace("Registering application listener.");
    applicationLevelListener.register();
  }

  /**
   * Removes an application listener from the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void removeApplicationListener() {
    log.trace("Removing application listener.");
    applicationLevelListener.remove();
  }

  /**
   * This method registers a stage listener to the Spark context.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public void registerStageListener() {
    log.trace("Registering stage level listener.");
    stageLevelListener.register();
  }

  /**
   * This method removes a stage listener from the Spark context.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public void removeStageListener() {
    log.trace("Removing stage level listener.");
    stageLevelListener.remove();
  }
}
