package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.enums.Domain;
import dagsolver.DagSolver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import scala.collection.JavaConverters;

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

  private final Map<Long, List<Task>> jobToStages = new ConcurrentHashMap<>();

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
   * Also tracks the stages and their parents in the job.
   *
   * @param jobStart The SparkListenerJobStart event object containing information upon job start.
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    jobSubmitTimes.put((long) jobStart.jobId() + 1, jobStart.time());
    List<Task> stages = new ArrayList<>();
    JavaConverters.seqAsJavaList(jobStart.stageInfos()).forEach(stageInfo -> {
      stages.add(Task.builder()
          .id((long) stageInfo.stageId() + 1)
          .parents(JavaConverters.seqAsJavaList(stageInfo.parentIds()).stream()
              .mapToInt(x -> (int) x + 1)
              .mapToLong(x -> (long) x)
              .toArray())
          .build());
    });
    jobToStages.put((long) jobStart.jobId() + 1, stages);
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
    final long tsSubmit = jobSubmitTimes.get(jobId);
    final Task[] tasks = wtaTaskListener
        .getWithCondition(task -> task.getWorkflowId() == jobId)
        .toArray(Task[]::new);
    final int taskCount = tasks.length;
    long criticalPathLength = -1L;
    int criticalPathTaskCount = -1;
    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";

    // we can also get the mode from the config, if that's what the user wants?
    final String scheduler = sparkContext.getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = config.getDomain();
    final String appName = sparkContext.appName();
    final String applicationField = "ETL";
    final double totalResources = -1.0;
    final double totalMemoryUsage = computeSum(Arrays.stream(tasks).map(Task::getMemoryRequested));
    final long totalNetworkUsage =
        (long) computeSum(Arrays.stream(tasks).map(task -> (double) task.getNetworkIoTime()));
    final double totalDiskSpaceUsage = computeSum(Arrays.stream(tasks).map(Task::getDiskSpaceRequested));
    final double totalEnergyConsumption = computeSum(Arrays.stream(tasks).map(Task::getEnergyConsumption));

    if (config.isStageLevel()) {
      stageLevelListener.setStages(jobId);
    } else {
      TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
      taskLevelListener.setTasks(stageLevelListener, jobId);

      List<Task> jobStages = stageLevelListener.getProcessedObjects().stream()
          .filter(stage -> stage.getWorkflowId() == jobId)
          .collect(Collectors.toList());
      jobStages.addAll(jobToStages.get(jobId).stream()
          .filter(stage -> !jobStages.stream()
              .map(Task::getId)
              .collect(Collectors.toList())
              .contains(stage.getId()))
          .collect(Collectors.toList()));

      final List<Task> criticalPath = solveCriticalPath(jobStages);
      TaskLevelListener listener = (TaskLevelListener) wtaTaskListener;

      final Map<Long, List<Task>> stageToTasks = listener.getStageToTasks();
      criticalPathTaskCount = criticalPath.stream()
          .map(stage -> stageToTasks.get(stage.getId()).size())
          .reduce(Integer::sum)
          .orElse(-1);
      criticalPathLength = criticalPath.stream()
          .map(stage -> stageToTasks.getOrDefault(stage.getId(), new ArrayList<>()).stream()
              .map(Task::getRuntime)
              .reduce(Long::max)
              .orElse(0L))
          .reduce(Long::sum)
          .orElse(-1L);
    }

    this.getProcessedObjects()
        .add(Workflow.builder()
            .id(jobId)
            .tsSubmit(tsSubmit)
            .tasks(tasks)
            .taskCount(taskCount)
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
            .build());
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

    Stream<Long> stageIds = stageLevelListener.getStageToJob().entrySet().stream()
        .filter(e -> e.getValue().equals(jobId))
        .map(Map.Entry::getKey);
    stageIds.forEach(stageId -> {
      stageLevelListener.getStageToJob().remove(stageId);
      stageLevelListener.getStageToParents().remove(stageId);
      stageLevelListener.getParentStageToChildrenStages().remove(stageId);
      stageLevelListener.getStageToResource().remove(stageId);
      if (!config.isStageLevel()) {
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

  /**
   * This is a method called on application end. it sets up the resources used in the spark workflow.
   * It also calculated the critical path for the job.
   *
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void setWorkflows() {
    final List<Workflow> workflows = this.getProcessedObjects();
    workflows.forEach(workflow -> {
      workflow.setTotalResources(Arrays.stream(workflow.getTasks())
          .map(Task::getResourceAmountRequested)
          .filter(resourceAmount -> resourceAmount >= 0.0)
          .reduce(Double::sum)
          .orElse(-1.0));
    });
  }

  /**
   * This method takes the stages inside this job and return the critical path.
   * it will filter out all dummy caches nodes.
   *
   * @param stages all stages in the job(including the cached ones)
   * @return critical path
   * @author Tianchen Qu
   * @since 1.0.0
   */
  List<Task> solveCriticalPath(List<Task> stages) {
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
