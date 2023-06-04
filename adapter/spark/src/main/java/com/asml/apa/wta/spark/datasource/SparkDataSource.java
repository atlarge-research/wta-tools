package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.utils.CollectorInterface;
import com.asml.apa.wta.core.utils.WtaUtils;
import com.asml.apa.wta.spark.listener.AbstractListener;
import com.asml.apa.wta.spark.listener.ApplicationLevelListener;
import com.asml.apa.wta.spark.listener.JobLevelListener;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import lombok.Getter;
import org.apache.spark.SparkContext;

/**
 * This class is a Stage data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
public class SparkDataSource implements CollectorInterface {

  private final AbstractListener<Task> taskLevelListener;

  private final AbstractListener<Workflow> jobLevelListener;

  private final AbstractListener<Workload> applicationLevelListener;

  private final AbstractListener<Task> stageLevelListener;

  private final RuntimeConfig runtimeConfig;

  /**
   * Constructor for the Spark data source. This requires a Spark context to ensure a Spark session
   * is available before the data source is initialized.
   *
   * @param sparkContext  SparkContext of the running Spark session
   * @param config Additional config specified by the user for the plugin
   * @author Pil Kyu Cho
   * @author Henry Page
   * @since 1.0.0
   */
  public SparkDataSource(SparkContext sparkContext, RuntimeConfig config) {
    taskLevelListener = new TaskLevelListener(sparkContext, config, executorLevelListener);
    stageLevelListener = new StageLevelListener(sparkContext, config);
    if (config.isStageLevel()) {
      jobLevelListener = new JobLevelListener(sparkContext, config, stageLevelListener);
    } else {
      jobLevelListener = new JobLevelListener(sparkContext, config, taskLevelListener);
    }
    applicationLevelListener = new ApplicationLevelListener(sparkContext, config, jobLevelListener);
    runtimeConfig = config;
  }

  /**
   * Alternate constructor which requires the context and gets the config from
   * the default directory.
   *
   * @param sparkContext The current spark context
   * @author Henry Page
   * @since 1.0.0
   */
  public SparkDataSource(SparkContext sparkContext) {
    this(sparkContext, WtaUtils.readConfig());
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
