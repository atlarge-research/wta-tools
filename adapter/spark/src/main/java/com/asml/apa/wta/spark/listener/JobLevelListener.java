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

  private final TaskStageBaseListener taskListener;

  private final StageLevelListener stageLevelListener;

  private final Map<Integer, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  private int criticalPathTasks = -1;

  private long jobStartTime = -1L;

  private List<Object> jobStages;

  /**
   * Constructor for the job-level listener.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @param taskListener       The task-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public JobLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      TaskStageBaseListener taskListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.taskListener = taskListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Callback for job start event, tracks the submit time of the job.
   *
   * @param jobStart The jobstart event object containing information upon job start.
   * @author Henry Page
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    jobSubmitTimes.put(jobStart.jobId() + 1, jobStart.time());
    criticalPathTasks = jobStart.stageIds().length();
    jobStartTime = System.currentTimeMillis();
    jobStages = JavaConverters.seqAsJavaList(jobStart.stageIds());
  }

  /**
   * Processes the workflow and puts it into an object.
   *
   * @param jobEnd The job end event object containing information upon job end
   * @author Henry Page
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final int jobId = jobEnd.jobId() + 1;
    final long submitTime = jobSubmitTimes.get(jobId);
    final Task[] tasks = taskListener
        .getWithCondition(task -> task.getWorkflowId() == jobId)
        .toArray(Task[]::new);
    final int numTasks = tasks.length;
    // we can also get the mode from the config, if that's what the user wants?
    final String scheduler = "DAGScheduler";
    final Domain domain = config.getDomain();
    final String appName = sparkContext.appName();
    final int criticalPathTaskCount = criticalPathTasks;
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
    final long jobRunTime = System.currentTimeMillis() - jobStartTime;
    final long driverTime = jobRunTime
        - stageLevelListener.getProcessedObjects().stream()
            .filter(x -> jobStages.contains(Math.toIntExact(x.getId())))
            .map(Task::getRuntime)
            .reduce(Long::sum)
            .orElse(0L);
    long criticalPathLength = -1L;
    if (!config.isStageLevel()) {
      TaskLevelListener listener = (TaskLevelListener) taskListener;
      final Map<Integer, List<Task>> stageToTasks = listener.getStageToTasks();
      List<Long> stageMaximumTaskTime = new ArrayList<>();
      for (Object id : jobStages) {
        List<Task> stageTasks = stageToTasks.get((Integer) id);
        if (stageTasks != null) {
          stageMaximumTaskTime.add(stageTasks.stream()
              .map(Task::getRuntime)
              .reduce(Long::max)
              .orElse(0L));
        } else {
          stageMaximumTaskTime.add(0L);
        }
      }
      criticalPathLength =
          driverTime + stageMaximumTaskTime.stream().reduce(Long::sum).orElse(0L);
    }
    // unknown

    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";
    final String applicationField = "ETL";

    this.getProcessedObjects()
        .add(Workflow.builder()
            .id(jobId)
            .submitTime(submitTime)
            .tasks(tasks)
            .numberOfTasks(numTasks)
            .criticalPathLength(criticalPathLength)
            .criticalPathTaskCount(criticalPathTaskCount)
            .maxNumberOfConcurrentTasks(maxNumberOfConcurrentTasks)
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

    jobSubmitTimes.remove(jobId);
  }
}
