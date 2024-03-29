package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.stream.Stream;
import com.asml.apa.wta.spark.util.DagSolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import scala.collection.JavaConverters;

/**
 * Job-level listener for the Spark data source.
 *
 * @author Henry Page
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
public class JobLevelListener extends AbstractListener<Workflow> {

  private final TaskStageBaseListener wtaTaskListener;

  private final StageLevelListener stageLevelListener;

  private final Map<Long, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  private final Map<Long, List<Task>> jobToStages = new ConcurrentHashMap<>();

  /**
   * Constructor for the job-level listener when collecting task-level metrics.
   *
   * @param sparkContext          current spark context
   * @param config                additional config specified by the user for the plugin
   * @param wtaTaskListener       task-level listener to be used by this listener
   * @param stageLevelListener    stage-level listener
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
   * Constructor for the job-level listener when collecting stage-level metrics.
   *
   * @param sparkContext          current spark context
   * @param config                additional config specified by the user for the plugin
   * @param stageLevelListener    stage-level listener
   * @since 1.0.0
   */
  public JobLevelListener(SparkContext sparkContext, RuntimeConfig config, StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.wtaTaskListener = stageLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Callback for job start event, tracks the submit time of the Spark Job and Spark Stages and its parents in the
   * Spark Job.
   *
   * @param jobStart              SparkListenerJobStart event object containing information upon job start
   * @since 1.0.0
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    jobSubmitTimes.put(jobStart.jobId() + 1L, jobStart.time());
    List<Task> stages = new ArrayList<>();
    JavaConverters.seqAsJavaList(jobStart.stageInfos())
        .forEach(stageInfo -> stages.add(Task.builder()
            .id((long) stageInfo.stageId() + 1)
            .parents(JavaConverters.seqAsJavaList(stageInfo.parentIds()).stream()
                .mapToInt(parentId -> (int) parentId + 1)
                .mapToLong(parentId -> (long) parentId)
                .toArray())
            .build()));
    jobToStages.put((long) jobStart.jobId() + 1, stages);
  }

  /**
   * When a job is finished, processes the workflow and puts it into an object. In addition, it sets the
   * parent, child, and resource information for the affiliated Spark tasks or Spark stages to this job,
   * depending on the config setting of isStageLevel. This needs to happen here as the clean-up of
   * ConcurrentHashMap containers must also happen at the very end when a job finishes.
   *
   * @param jobEnd                SparkListenerJobEnd event object containing information upon job end
   * @since 1.0.0
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final long jobId = jobEnd.jobId() + 1L;
    final long tsSubmit = jobSubmitTimes.get(jobId);
    final Stream<Task> tasks = wtaTaskListener.getWorkflowsToTasks().onKey(jobId);

    final String scheduler = getSparkContext().getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = getConfig().getDomain();
    final String appName = getSparkContext().appName();
    final double totalMemoryUsage = computeSum(tasks.copy().map(Task::getMemoryRequested));
    final long totalNetworkUsage = (long) computeSum(tasks.copy().map(task -> (double) task.getNetworkIoTime()));
    final double totalDiskSpaceUsage = computeSum(tasks.copy().map(Task::getDiskSpaceRequested));
    final double totalEnergyConsumption = computeSum(tasks.copy().map(Task::getEnergyConsumption));
    final double totalResources = tasks.copy()
        .map(Task::getResourceAmountRequested)
        .filter(resourceAmount -> resourceAmount >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0);

    final long criticalPathLength;
    final long criticalPathTaskCount;
    if (getConfig().isStageLevel()) {
      stageLevelListener.setStages(jobId);
      criticalPathLength = -1L;
      criticalPathTaskCount = -1;
    } else {
      TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
      taskLevelListener.setTasks(stageLevelListener, jobId);

      List<Task> jobStages =
          stageLevelListener.getWorkflowsToTasks().onKey(jobId).toList();
      jobStages.addAll(jobToStages.get(jobId).stream()
          .filter(stage -> !jobStages.stream()
              .map(Task::getId)
              .collect(Collectors.toList())
              .contains(stage.getId()))
          .collect(Collectors.toList()));

      final List<Task> criticalPath = solveCriticalPath(jobStages);
      final Map<Long, List<Task>> stageToTasks = taskLevelListener.getStageToTasks();
      criticalPathTaskCount = criticalPath.isEmpty() ? -1L : criticalPath.size();
      criticalPathLength = criticalPath.stream()
          .map(stage -> stageToTasks.getOrDefault(stage.getId(), new ArrayList<>()).stream()
              .map(Task::getRuntime)
              .reduce(Long::max)
              .orElse(0L))
          .reduce(Long::sum)
          .orElse(-1L);
    }

    getThreadPool()
        .execute(() -> addProcessedObject(Workflow.builder()
            .id(jobId)
            .tsSubmit(tsSubmit)
            .taskIds(tasks.copy().map(Task::getId).toArray(Long[]::new))
            .taskCount(tasks.count())
            .criticalPathLength(criticalPathLength)
            .criticalPathTaskCount(criticalPathTaskCount)
            .scheduler(scheduler)
            .domain(domain)
            .applicationName(appName)
            .totalResources(totalResources)
            .totalMemoryUsage(totalMemoryUsage)
            .totalNetworkUsage(totalNetworkUsage)
            .totalDiskSpaceUsage(totalDiskSpaceUsage)
            .totalEnergyConsumption(totalEnergyConsumption)
            .build()));

    cleanUpContainers(jobId);
  }

  /**
   * To prevent memory overload, clean up the containers in all listeners after a Spark job finishes.
   * Spark jobs are designed to be independent and run in parallel, thus the clean-up of containers
   * after a job finishes should not affect other jobs. This is also necessary as some stages are created at
   * job start but never submitted.
   * <p>
   * For ConcurrentHashMap, remove() is thread-safe when removing based on key.
   *
   * @since 1.0.0
   */
  private void cleanUpContainers(long jobId) {
    jobSubmitTimes.remove(jobId);
    jobToStages.remove(jobId);

    Stream<Long> stageIds = new Stream<>();
    stageLevelListener.getStageToJob().entrySet().stream()
        .filter((e) -> e.getValue().equals(jobId))
        .forEach((e) -> stageIds.addToStream(e.getKey()));

    stageLevelListener.getWorkflowsToTasks().dropKey(jobId);
    wtaTaskListener.getWorkflowsToTasks().dropKey(jobId);

    stageIds.forEach(stageId -> {
      stageLevelListener.getStageToJob().remove(stageId);
      stageLevelListener.getStageToParents().remove(stageId);
      stageLevelListener.getParentStageToChildrenStages().remove(stageId);
      stageLevelListener.getStageToResource().remove(stageId);
      wtaTaskListener.getStageToJob().remove(stageId);
      if (!getConfig().isStageLevel()) {
        TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
        taskLevelListener.getStageToTasks().remove(stageId);
      }
    });
  }

  /**
   * Summation for Double stream for positive terms.
   *
   * @param data              stream of data
   * @return                  summation of positive values from data
   * @since 1.0.0
   */
  private double computeSum(Stream<Double> data) {
    return data.filter(task -> task >= 0.0).reduce(Double::sum).orElse(-1.0);
  }

  /**
   * This method takes the stages inside this Spark Job and return the critical path.
   *
   * @param stages            all completed stages in the Spark Job
   * @return                  critical path for this Spark Job
   * @since 1.0.0
   */
  public List<Task> solveCriticalPath(List<Task> stages) {
    try {
      DagSolver dag = new DagSolver(stages, (TaskLevelListener) wtaTaskListener);
      return dag.longestPath().stream()
          .filter(stage -> stage.getRuntime() != 0)
          .collect(Collectors.toList());
    } catch (Exception e) {
      return new ArrayList<>();
    }
  }
}
