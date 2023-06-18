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

  private final TaskStageBaseListener wtaTaskListener;

  private final StageLevelListener stageLevelListener;

  private final Map<Long, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  private final Map<Long, Integer> criticalPathTasks = new ConcurrentHashMap<>();

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
   * Constructor for the job-level listener.
   * This constructor is for stage-level plugin.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @param stageLevelListener The stage-level listener
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public JobLevelListener(SparkContext sparkContext, RuntimeConfig config,
                          StageLevelListener stageLevelListener) {
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
    stageIds = JavaConverters.seqAsJavaList(jobStart.stageIds()).stream()
        .map(obj -> ((Long) obj) + 1)
        .collect(Collectors.toList());
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
    long criticalPathLength = -1L;
    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";

    // we can also get the mode from the config, if that's what the user wants?
    final String scheduler = sparkContext.getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = config.getDomain();
    final String appName = sparkContext.appName();
    final String applicationField = "ETL";
    final int criticalPathTaskCount = criticalPathTasks.remove(jobId);
    final double totalResources = -1.0;
    final double totalMemoryUsage = computeSum(Arrays.stream(tasks).map(Task::getMemoryRequested));
    final long totalNetworkUsage = (long) computeSum(Arrays.stream(tasks).map(task -> (double) task.getNetworkIoTime()));
    final double totalDiskSpaceUsage = computeSum(Arrays.stream(tasks).map(Task::getDiskSpaceRequested));
    final double totalEnergyConsumption = computeSum(Arrays.stream(tasks).map(Task::getEnergyConsumption));

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
   *
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void setWorkflows() {
    final List<Workflow> workflows = this.getProcessedObjects();
    workflows.forEach(workflow -> workflow.setTotalResources(Arrays.stream(workflow.getTasks())
            .map(Task::getResourceAmountRequested)
            .filter(resourceAmount -> resourceAmount >= 0.0)
            .reduce(Double::sum)
            .orElse(-1.0)));
  }
}
