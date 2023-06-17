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

  private final TaskStageBaseListener taskListener;

  private final StageLevelListener stageLevelListener;

  private final Map<Integer, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  private final Map<Integer, Integer> criticalPathTasks = new ConcurrentHashMap<>();

  private List<Integer> jobStages;

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
    this.taskListener = stageLevelListener;
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
    criticalPathTasks.put(jobStart.jobId() + 1, jobStart.stageIds().length());
    jobStages = JavaConverters.seqAsJavaList(jobStart.stageIds()).stream()
        .map(stageId -> (int) stageId)
        .collect(Collectors.toList());
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
    final String scheduler = sparkContext.getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = config.getDomain();
    final String appName = sparkContext.appName();
    final int criticalPathTaskCount = criticalPathTasks.remove(jobId);
    final double totalResources = -1.0;
    final double totalMemoryUsage = sumDouble(Arrays.stream(tasks).map(Task::getMemoryRequested));
    final long totalNetworkUsage = sumLong(Arrays.stream(tasks).map(Task::getNetworkIoTime));
    final double totalDiskSpaceUsage = sumDouble(Arrays.stream(tasks).map(Task::getDiskSpaceRequested));
    final double totalEnergyConsumption = sumDouble(Arrays.stream(tasks).map(Task::getEnergyConsumption));
    final long jobRunTime = jobEnd.time() - jobSubmitTimes.get(jobId);
    final long driverTime = jobRunTime
        - stageLevelListener.getProcessedObjects().stream()
            .filter(stage -> jobStages.contains(Math.toIntExact(stage.getId())))
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
            .tsSubmit(submitTime)
            .tasks(tasks)
            .taskCount(numTasks)
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

    jobSubmitTimes.remove(jobId);
  }

  /**
   * Summation for Double stream for positive terms.
   *
   * @param data data stream
   * @return summation
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private double sumDouble(Stream<Double> data) {
    return data.filter(task -> task >= 0.0).reduce(Double::sum).orElse(-1.0);
  }

  /**
   * Summation for Long stream for positive terms.
   *
   * @param data data stream
   * @return summation
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private long sumLong(Stream<Long> data) {
    return data.filter(task -> task >= 0).reduce(Long::sum).orElse(-1L);
  }

  /**
   * This is a method called on application end. it sets up the resources used in the spark workflow.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public void setWorkflows() {
    final List<Workflow> workflows = getProcessedObjects();
    for (Workflow workflow : workflows) {
      workflow.setTotalResources(Arrays.stream(workflow.getTasks())
          .map(Task::getResourceAmountRequested)
          .filter(resourceAmount -> resourceAmount >= 0.0)
          .reduce(Double::sum)
          .orElse(-1.0));
    }
  }
}
