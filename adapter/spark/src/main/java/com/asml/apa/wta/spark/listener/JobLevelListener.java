package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.enums.Domain;
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

  private final AbstractListener<Task> taskListener;

  private final Map<Integer, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  /**
   * Constructor for the job-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param taskListener The task-level listener to be used by this listener
   * @author Henry Page
   * @since 1.0.0
   */
  public JobLevelListener(SparkContext sparkContext, RuntimeConfig config, AbstractListener<Task> taskListener) {
    super(sparkContext, config);
    this.taskListener = taskListener;
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

    // unknown
    final int criticalPathLength = -1;
    final int criticalPathTaskCount = -1;
    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";
    final String applicationField = "ETL";
    final double totalResources = -1.0;
    final double totalMemoryUsage = -1.0;
    final long totalNetworkUsage = -1L;
    final long totalDiskSpaceUsage = -1L;
    final long totalEnergyConsumption = -1L;
    processedObjects.add(Workflow.builder()
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
