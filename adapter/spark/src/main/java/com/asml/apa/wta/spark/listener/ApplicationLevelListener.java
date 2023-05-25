package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

/**
 * This class is an application-level listener for the Spark data source.
 * It's important that one does not override {@link org.apache.spark.scheduler.SparkListenerInterface#onApplicationStart(SparkListenerApplicationStart)} here, as the event is already sent
 * before the listener is registered unless the listener is explicitly registered in the Spark configuration as per <a href="https://stackoverflow.com/questions/36401238/spark-onapplicationstart-is-never-gets-called">SO</a>
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
public class ApplicationLevelListener extends AbstractListener<Workload> {

  private final AbstractListener<Workflow> jobLevelListener;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param jobLevelListener The job-level listener to be used by this listener
   * @author Henry Page
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext, RuntimeConfig config, AbstractListener<Workflow> jobLevelListener) {
    super(sparkContext, config);
    this.jobLevelListener = jobLevelListener;
  }

  /**
   * Callback function that is called right at the end of the application. Further experimentation
   * is needed to determine if applicationEnd is called first or shutdown.
   *
   * @param applicationEnd The event corresponding to the end of the application
   */
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {

    // we should enver enter this branch, this is a guard since an application
    // only terminates once.
    if (!processedObjects.isEmpty()) {
      return;
    }

    final Workflow[] workflows = jobLevelListener.getProcessedObjects().toArray(new Workflow[0]);
    final int numWorkflows = workflows.length;
    final int totalTasks =
        Arrays.stream(workflows).mapToInt(Workflow::getNumberOfTasks).sum();
    final Domain domain = config.getDomain();
    final long startDate = sparkContext.startTime();
    final long endDate = applicationEnd.time();
    final String[] authors = config.getAuthors();
    final String workloadDescription = config.getDescription();

    // unknown
    final long numSites = -1L;
    final long numResources = -1L;
    final long numUsers = -1L;
    final long numGroups = -1L;
    final double totalResourceSeconds = -1.0;
    // all statistics (stdev, mean, etc.) are unknown

    processedObjects.add(Workload.builder()
        .workflows(workflows)
        .total_workflows(numWorkflows)
        .total_tasks(totalTasks)
        .domain(domain)
        .date_start(startDate)
        .date_end(endDate)
        .authors(authors)
        .workload_description(workloadDescription)
        .num_sites(numSites)
        .num_resources(numResources)
        .num_users(numUsers)
        .num_groups(numGroups)
        .total_resource_seconds(totalResourceSeconds)
        .build());
  }
}
