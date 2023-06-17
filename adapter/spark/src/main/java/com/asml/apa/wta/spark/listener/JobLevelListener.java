package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.ArrayList;
import java.util.Arrays;
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

  private int criticalPathTaskCount = -1;

  private long jobStartTime = -1L;

  private List<Long> stageIds = new ArrayList<>();

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
   * Computes the critical path length.
   * @return    Critical path length
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private long computeCriticalPathLength() {
    final long jobRunTime = System.currentTimeMillis() - jobStartTime;
    final long driverTime = jobRunTime
        - stageLevelListener.getProcessedObjects().stream()
            .filter(wtaTask -> stageIds.contains(wtaTask.getId()))
            .map(Task::getRuntime)
            .reduce(Long::sum)
            .orElse(0L);

    long criticalPathLength = -1L;
    if (!config.isStageLevel()) {
      TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
      final Map<Long, List<Task>> stageToTasks = taskLevelListener.getStageToTasks();
      List<Long> stageMaximumTaskTime = new ArrayList<>();
      stageIds.stream()
          .filter(stageToTasks::containsKey)
          .forEach(stageId -> stageMaximumTaskTime.add(stageToTasks.get(stageId).stream()
              .map(Task::getRuntime)
              .max(Long::compareTo)
              .orElse(0L)));
      criticalPathLength =
          driverTime + stageMaximumTaskTime.stream().reduce(Long::sum).orElse(0L);
    }
    return criticalPathLength;
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
    jobStartTime = System.currentTimeMillis();
    stageIds = JavaConverters.seqAsJavaList(jobStart.stageIds()).stream()
        .map(obj -> ((Long) obj) + 1)
        .collect(Collectors.toList());
    criticalPathTaskCount = jobStart.stageIds().length();
  }

  /**
   * Processes the workflow and puts it into an object.
   *
   * @param jobEnd The job end event object containing information upon job end
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final long jobId = jobEnd.jobId() + 1;
    final long tsSubmit = jobSubmitTimes.remove(jobId);
    final Task[] tasks = wtaTaskListener
        .getWithCondition(task -> task.getWorkflowId() == jobId)
        .toArray(Task[]::new);
    final int taskCount = tasks.length;

    long criticalPathLength = computeCriticalPathLength();

    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";
    // we can also get the mode from the config, if that's what the user wants?
    final String scheduler = sparkContext.getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = config.getDomain();
    final String appName = sparkContext.appName();
    final String applicationField = "ETL";
    final double totalResources = -1.0;
    final double totalMemoryUsage = Arrays.stream(tasks)
        .map(Task::getMemoryRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::sum)
        .orElseGet(() -> -1.0);
    final long totalNetworkUsage = Arrays.stream(tasks)
        .map(Task::getNetworkIoTime)
        .filter(x -> x >= 0)
        .reduce(Long::sum)
        .orElse(-1L);
    final double totalDiskSpaceUsage = Arrays.stream(tasks)
        .map(Task::getDiskSpaceRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::sum)
        .orElseGet(() -> -1.0);
    final double totalEnergyConsumption = Arrays.stream(tasks)
        .map(Task::getEnergyConsumption)
        .filter(x -> x >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0);

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
  }
}
