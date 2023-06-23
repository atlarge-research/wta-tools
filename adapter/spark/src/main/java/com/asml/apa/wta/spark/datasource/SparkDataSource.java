package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.spark.listener.AbstractListener;
import com.asml.apa.wta.spark.listener.ApplicationLevelListener;
import com.asml.apa.wta.spark.listener.JobLevelListener;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
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
@Slf4j
@Getter
public class SparkDataSource {

  private final TaskLevelListener taskLevelListener;

  private final StageLevelListener stageLevelListener;

  private final JobLevelListener jobLevelListener;

  private final ApplicationLevelListener applicationLevelListener;

  private final RuntimeConfig runtimeConfig;

  /**
   * Joins the thread pools.
   *
   * @throws InterruptedException when an {@link InterruptedException} occurs while joining the threads
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void join() throws InterruptedException {
    AbstractListener.getThreadPool().shutdown();
  }

  /**
   * Constructor for the Spark data source. This requires a Spark context to ensure a Spark session
   * is available before the data source is initialized.
   *
   * @param sparkContext  SparkContext of the running Spark session
   * @param config Additional config specified by the user for the plugin
   * @param metricStreamingEngine the driver's {@link MetricStreamingEngine} to inject
   * @param wtaWriter the {@link WtaWriter} to write to
   * @author Pil Kyu Cho
   * @author Henry Page
   * @author Tianchen Qu
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public SparkDataSource(
      SparkContext sparkContext,
      RuntimeConfig config,
      MetricStreamingEngine metricStreamingEngine,
      WtaWriter wtaWriter) {
    taskLevelListener = new TaskLevelListener(sparkContext, config);
    stageLevelListener = new StageLevelListener(sparkContext, config);
    if (config.isStageLevel()) {
      jobLevelListener = new JobLevelListener(sparkContext, config, stageLevelListener);
      applicationLevelListener = new ApplicationLevelListener(
          sparkContext, config, stageLevelListener, jobLevelListener, this, metricStreamingEngine, wtaWriter);
    } else {
      jobLevelListener = new JobLevelListener(sparkContext, config, taskLevelListener, stageLevelListener);
      applicationLevelListener = new ApplicationLevelListener(
          sparkContext,
          config,
          taskLevelListener,
          stageLevelListener,
          jobLevelListener,
          this,
          metricStreamingEngine,
          wtaWriter);
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
    taskLevelListener.register();
  }

  /**
   * This method removes a task listener from the Spark context.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void removeTaskListener() {
    taskLevelListener.remove();
  }

  /**
   * Registers a job listener to the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void registerJobListener() {
    jobLevelListener.register();
  }

  /**
   * Removes a job listener from the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void removeJobListener() {
    jobLevelListener.remove();
  }

  /**
   * Registers an application listener to the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void registerApplicationListener() {
    applicationLevelListener.register();
  }

  /**
   * Removes an application listener from the Spark context.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  public void removeApplicationListener() {
    applicationLevelListener.remove();
  }

  /**
   * This method registers a stage listener to the Spark context.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public void registerStageListener() {
    stageLevelListener.register();
  }

  /**
   * This method removes a stage listener from the Spark context.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public void removeStageListener() {
    stageLevelListener.remove();
  }
}
