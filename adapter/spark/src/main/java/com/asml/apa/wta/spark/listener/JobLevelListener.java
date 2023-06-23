package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.streams.Stream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * This class is a job-level listener for the Spark data source.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
public class JobLevelListener extends AbstractListener<Workflow> {

  private final TaskStageBaseListener wtaTaskListener;

  private final StageLevelListener stageLevelListener;

  private final Map<Long, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  private final Map<Long, Integer> criticalPathTasks = new ConcurrentHashMap<>();

  /**
   * Constructor for the job-level listener.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @param wtaTaskListener       The task-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public JobLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      TaskStageBaseListener wtaTaskListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.wtaTaskListener = wtaTaskListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Constructor for the job-level listener.
   * This constructor is for stage-level plugin.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @param stageLevelListener The stage-level listener
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public JobLevelListener(SparkContext sparkContext, RuntimeConfig config, StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.wtaTaskListener = stageLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Callback for job start event, tracks the submit time of the job.
   *
   * @param jobStart The SparkListenerJobStart event object containing information upon job start.
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    jobSubmitTimes.put((long) jobStart.jobId() + 1, jobStart.time());
    criticalPathTasks.put(jobStart.jobId() + 1L, jobStart.stageIds().length());
  }

  /**
   * When a job is finished, processes the workflow and puts it into an object. In addition, it sets the
   * parent, child, and resource information for the affiliated Spark tasks or Spark stages to this job,
   * depending on the config setting of isStageLevel. This needs to happen here as the clean-up of
   * ConcurrentHashMap containers must also happen at the very end when a job finishes.
   *
   * @param jobEnd The job end event object containing information upon job end
   * @author Henry Page
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final long jobId = jobEnd.jobId() + 1;
    final Long tsSubmit = jobSubmitTimes.get(jobId);
    final Stream<Task> tasks = wtaTaskListener.getProcessedObjects().filter(task -> task.getWorkflowId() == jobId);
    final long criticalPathLength = -1L;
    final int criticalPathTaskCount = criticalPathTasks.get(jobId);
    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";

    // we can also get the mode from the config, if that's what the user wants?
    final String scheduler = getSparkContext().getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = getConfig().getDomain();
    final String appName = getSparkContext().appName();
    final String applicationField = "ETL";
    final double totalMemoryUsage = computeSum(tasks.copy().map(Task::getMemoryRequested));
    final long totalNetworkUsage = (long) computeSum(tasks.copy().map(task -> (double) task.getNetworkIoTime()));
    final double totalDiskSpaceUsage = computeSum(tasks.copy().map(Task::getDiskSpaceRequested));
    final double totalEnergyConsumption = computeSum(tasks.copy().map(Task::getEnergyConsumption));
    final double totalResources = tasks.copy()
        .map(Task::getResourceAmountRequested)
        .filter(resourceAmount -> resourceAmount >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0);

    getThreadPool()
        .execute(() -> addProcessedObject(Workflow.builder()
            .id(jobId)
            .tsSubmit(tsSubmit)
            .taskIds(tasks.copy().map(Task::getId).toArray(Long[]::new))
            .taskCount(tasks.count())
            .criticalPathLength(criticalPathLength)
            .criticalPathTaskCount(criticalPathTaskCount)
            .maxConcurrentTasks(maxNumberOfConcurrentTasks)
            .nfrs(nfrs)
            .scheduler(scheduler)
            .domain(domain)
            .applicationName(appName)
            .applicationField(applicationField)
            .totalResources(totalResources)
            .totalMemoryUsage(totalMemoryUsage)
            .totalNetworkUsage(totalNetworkUsage)
            .totalDiskSpaceUsage(totalDiskSpaceUsage)
            .totalEnergyConsumption(totalEnergyConsumption)
            .build()));

    if (getConfig().isStageLevel()) {
      stageLevelListener.setStages(jobId);
    } else {
      TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
      taskLevelListener.setTasks(stageLevelListener, jobId);
    }
    cleanUpContainers(jobEnd);
  }

  /**
   * To prevent memory overload, clean up the containers in all listeners after a Spark job finishes.
   * Spark jobs are designed to be independent and run in parallel, thus the clean-up of containers
   * after a job finishes should not affect other jobs. This is also necessary as some stages are created at
   * job start but never submitted.
   * <p>
   * For ConcurrentHashMap, remove() is thread-safe when removing based on key. For removing based on value,
   * most thread-safe and is to use entrySet() in combination with removeIf().
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private void cleanUpContainers(SparkListenerJobEnd jobEnd) {
    long jobId = jobEnd.jobId() + 1;
    jobSubmitTimes.remove(jobId);
    criticalPathTasks.remove(jobId);

    Stream<Long> stageIds = new Stream<>();

    for (Map.Entry<Long, Long> e : stageLevelListener.getStageToJob().entrySet()) {
      if (e.getValue().equals(jobId)) {
        stageIds.addToStream(e.getKey());
      }
    }

    stageIds.forEach(stageId -> {
      stageLevelListener.getStageToJob().remove(stageId);
      stageLevelListener.getStageToParents().remove(stageId);
      stageLevelListener.getParentStageToChildrenStages().remove(stageId);
      stageLevelListener.getStageToResource().remove(stageId);
      if (!getConfig().isStageLevel()) {
        // additional clean up for task-level plugin
        TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
        taskLevelListener.getStageToTasks().remove(stageId);
        taskLevelListener.getTaskToStage().entrySet().removeIf(entry -> entry.getValue()
            .equals(stageId));
      }
    });
  }

  /**
   * Summation for Double stream for positive terms.
   *
   * @param data              stream of data
   * @return                  summation of positive values from data
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeSum(Stream<Double> data) {
    return data.filter(task -> task >= 0.0).reduce(Double::sum).orElse(-1.0);
  }
}
