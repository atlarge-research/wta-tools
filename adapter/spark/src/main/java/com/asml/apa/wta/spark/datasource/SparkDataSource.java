package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.spark.listener.AbstractListener;
import com.asml.apa.wta.spark.listener.ApplicationLevelListener;
import com.asml.apa.wta.spark.listener.JobLevelListener;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;

/**
 * Spark data source class for the WTA plugin.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Atour Mousavi Gourabi
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
   * Awaits the thread pool.
   *
   * @param awaitSeconds      amount of seconds to wait for.
   */
  public void awaitAndShutdownThreadPool(int awaitSeconds) {
    AbstractListener.getThreadPool().shutdown();
    try {
      if (!AbstractListener.getThreadPool().awaitTermination(awaitSeconds, TimeUnit.SECONDS)) {
        log.error("Could not await the thread pool because of a {} second timeout.", awaitSeconds);
      }
    } catch (InterruptedException e) {
      log.error("Could not await the thread pool because InterruptedException {}.", e.getMessage());
    }
  }

  /**
   * Constructor for the Spark data source. This requires a Spark context to ensure a Spark session
   * is available before the data source is initialized.
   *
   * @param sparkContext              SparkContext of the running Spark session.
   * @param config                    additional config specified by the user for the plugin.
   * @param metricStreamingEngine     driver's {@link MetricStreamingEngine} to inject.
   * @param wtaWriter                 {@link WtaWriter} to write to.
   */
  public SparkDataSource(
      SparkContext sparkContext,
      RuntimeConfig config,
      MetricStreamingEngine metricStreamingEngine,
      WtaWriter wtaWriter) {
    log.trace("Initialising Spark Data Source");
    taskLevelListener = new TaskLevelListener(sparkContext, config);
    stageLevelListener = new StageLevelListener(sparkContext, config);
    if (config.isStageLevel()) {
      log.trace("Stage level listener is enabled.");
      jobLevelListener = new JobLevelListener(sparkContext, config, stageLevelListener);
      applicationLevelListener = new ApplicationLevelListener(
          sparkContext, config, stageLevelListener, jobLevelListener, this, metricStreamingEngine, wtaWriter);
    } else {
      log.trace("Task level listener is enabled.");
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
   * Registers a task listener to the Spark context.
   */
  public void registerTaskListener() {
    log.debug("Registering task listener.");
    taskLevelListener.register();
  }

  /**
   * Registers a job listener to the Spark context.
   */
  public void registerJobListener() {
    log.debug("Registering job listener.");
    jobLevelListener.register();
  }

  /**
   * Registers an application listener to the Spark context.
   */
  public void registerApplicationListener() {
    log.debug("Registering application listener.");
    applicationLevelListener.register();
  }

  /**
   * Registers a stage listener to the Spark context.
   */
  public void registerStageListener() {
    log.debug("Registering stage level listener.");
    stageLevelListener.register();
  }

  /**
   * Removes the listeners from the Spark context.
   */
  public void removeListeners() {
    log.debug("Removing the listeners from the Spark context.");
    taskLevelListener.remove();
    stageLevelListener.remove();
    jobLevelListener.remove();
    applicationLevelListener.remove();
  }
}
