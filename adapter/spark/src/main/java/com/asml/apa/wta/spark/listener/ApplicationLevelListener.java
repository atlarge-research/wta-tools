package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.asml.apa.wta.core.streams.Stream;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Triple;
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

   private final JobLevelListener jobLevelListener;

   private final TaskLevelListener taskLevelListener;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext      The current spark context
   * @param config            Additional config specified by the user for the plugin
   * @param jobLevelListener  The job-level listener to be used by this listener
   * @param taskLevelListener
   * @author Henry Page
   * @since 1.0.0
   */
  public ApplicationLevelListener(
          SparkContext sparkContext, RuntimeConfig config, JobLevelListener jobLevelListener, TaskLevelListener taskLevelListener) {
    super(sparkContext, config);
    this.jobLevelListener = jobLevelListener;
    this.taskLevelListener = taskLevelListener;
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
    final List<Task> tasks = taskLevelListener.processedObjects;

    final long numSites = tasks.stream().filter(x -> x.getSubmissionSite()!=-1).count();
    final long numResources = (long) tasks.stream().map(Task::getResourceAmountRequested).reduce(Double::sum).get();
    final long numUsers = tasks.stream().filter(x -> x.getUserId()!=-1).count();
    final long numGroups = tasks.stream().filter(x -> x.getGroupId()!=-1).count();
    final double totalResourceSeconds = tasks.stream().filter(x -> x.getGroupId()!=-1).map(x -> x.getResourceAmountRequested()*x.getRuntime()).reduce(Double::sum).orElseGet(() -> -1.0);
    // unknown

    // all statistics (stdev, mean, etc.) are unknown
     final double minResourceTask = tasks.stream().map(Task::getResourceAmountRequested).reduce(Double::min).get();

     final double maxResourceTask = tasks.stream().map(Task::getResourceAmountRequested).reduce(Double::max).get();

     final double meanResourceTask = tasks.stream().map(Task::getResourceAmountRequested).reduce(Double::sum).get()/tasks.size();;

     final double stdResourceTask = standardDeviation(tasks.stream().map(Task::getResourceAmountRequested), meanResourceTask);

     final Double[] resourceStats = medianAndQuatiles(tasks.stream().map(Task::getResourceAmountRequested).collect(Collectors.toList()));

     final double medianResourceTask = resourceStats[0];

     final double firstQuartileResourceTask = resourceStats[1];

     final double thirdQuartileResourceTask = resourceStats[2];

     final double covResourceTask = stdResourceTask/meanResourceTask;

     final double minMemory = tasks.stream().map(Task::getMemoryRequested).reduce(Double::min).get();

     final double maxMemory = tasks.stream().map(Task::getMemoryRequested).reduce(Double::max).get();

     final double meanMemory = tasks.stream().map(Task::getMemoryRequested).reduce(Double::sum).get()/tasks.size();

     final double stdMemory = standardDeviation(tasks.stream().map(Task::getMemoryRequested), meanMemory);

     final Double[] memoryStats = medianAndQuatiles(tasks.stream().map(Task::getMemoryRequested).collect(Collectors.toList()));

     final double medianMemory = memoryStats[0];

     final double firstQuartileMemory = memoryStats[1];

     final double thirdQuartileMemory = memoryStats[2];

     final double covMemory = stdMemory/meanMemory;

     final long minNetworkUsage = tasks.stream().map(Task::getNetworkIoTime).reduce(Long::min).get();

     final long maxNetworkUsage = tasks.stream().map(Task::getNetworkIoTime).reduce(Long::max).get();

     final double meanNetworkUsage = (double) tasks.stream().map(Task::getNetworkIoTime).reduce(Long::sum).get()/tasks.size();;

     final double stdNetworkUsage = standardDeviation(tasks.stream().map(Task::getNetworkIoTime).map(Long::doubleValue), meanNetworkUsage);

     final Long[] networkStats = medianAndQuatiles(tasks.stream().map(Task::getNetworkIoTime).collect(Collectors.toList()));

     final long medianNetworkUsage = networkStats[0];

     final long firstQuartileNetworkUsage = networkStats[1];

     final long thirdQuartileNetworkUsage = networkStats[2];

     final double covNetworkUsage = stdNetworkUsage/meanNetworkUsage;

     final double minDiskSpaceUsage = tasks.stream().map(Task::getDiskSpaceRequested).reduce(Double::min).get();

     final double maxDiskSpaceUsage = tasks.stream().map(Task::getDiskSpaceRequested).reduce(Double::max).get();

     final double meanDiskSpaceUsage = tasks.stream().map(Task::getDiskSpaceRequested).reduce(Double::sum).get()/tasks.size();

     final double stdDiskSpaceUsage = standardDeviation(tasks.stream().map(Task::getDiskSpaceRequested), meanDiskSpaceUsage);

     final Double[] diskSpaceStats = medianAndQuatiles(tasks.stream().map(Task::getDiskSpaceRequested).collect(Collectors.toList()));

     final double medianDiskSpaceUsage = diskSpaceStats[0];

     final double firstQuartileDiskSpaceUsage = diskSpaceStats[1];

     final double thirdQuartileDiskSpaceUsage = diskSpaceStats[2];

     final double covDiskSpaceUsage = stdDiskSpaceUsage/meanDiskSpaceUsage;

     final double minEnergy = tasks.stream().map(Task::getEnergyConsumption).reduce(Double::min).get();

     final double maxEnergy = tasks.stream().map(Task::getEnergyConsumption).reduce(Double::max).get();

     final double meanEnergy = (double) tasks.stream().map(Task::getEnergyConsumption).reduce(Double::sum).get()/tasks.size();;

     final double stdEnergy = standardDeviation(tasks.stream().map(Task::getEnergyConsumption), meanEnergy);

      final Double[] energyStats = medianAndQuatiles(tasks.stream().map(Task::getEnergyConsumption).collect(Collectors.toList()));

     final double medianEnergy = energyStats[0];

     final double firstQuartileEnergy = energyStats[1];

     final double thirdQuartileEnergy = energyStats[2];

     final double covEnergy = stdEnergy/meanEnergy;

    processedObjects.add(Workload.builder()
        .totalWorkflows(numWorkflows)
        .totalTasks(totalTasks)
        .domain(domain)
        .dateStart(startDate)
        .dateEnd(endDate)
        .authors(authors)
        .workloadDescription(workloadDescription)
        .numSites(numSites)
        .numResources(numResources)
        .numUsers(numUsers)
        .numGroups(numGroups)
        .totalResourceSeconds(totalResourceSeconds)
                    .meanNetworkUsage(meanNetworkUsage).covNetworkUsage(covNetworkUsage).firstQuartileNetworkUsage(firstQuartileNetworkUsage).maxNetworkUsage(maxNetworkUsage).medianNetworkUsage(medianNetworkUsage).minNetworkUsage(minNetworkUsage).stdNetworkUsage(stdNetworkUsage).thirdQuartileNetworkUsage(thirdQuartileNetworkUsage)
                    .maxEnergy(maxEnergy).covEnergy(covEnergy).firstQuartileEnergy(firstQuartileEnergy).meanEnergy(meanEnergy).medianEnergy(medianEnergy).minEnergy(minEnergy).stdEnergy(stdEnergy).thirdQuartileEnergy(thirdQuartileEnergy)
                    .covResourceTask(covResourceTask).meanResourceTask(meanResourceTask).stdResourceTask(stdResourceTask).maxResourceTask(maxResourceTask).minResourceTask(minResourceTask).medianResourceTask(medianResourceTask).firstQuartileResourceTask(firstQuartileResourceTask).thirdQuartileResourceTask(thirdQuartileResourceTask)
                    .covMemory(covMemory).meanMemory(meanMemory).stdMemory(stdMemory).maxMemory(maxMemory).minMemory(minMemory).medianMemory(medianMemory).firstQuartileMemory(firstQuartileMemory).thirdQuartileMemory(thirdQuartileMemory)
                    .covDiskSpaceUsage(covDiskSpaceUsage).stdDiskSpaceUsage(stdDiskSpaceUsage).meanDiskSpaceUsage(meanDiskSpaceUsage).firstQuartileDiskSpaceUsage(firstQuartileDiskSpaceUsage).thirdQuartileDiskSpaceUsage(thirdQuartileDiskSpaceUsage).medianDiskSpaceUsage(medianDiskSpaceUsage).maxDiskSpaceUsage(maxDiskSpaceUsage).minDiskSpaceUsage(minDiskSpaceUsage)
        .build());
  }

  private double standardDeviation(java.util.stream.Stream<Double> data, Double mean){
    double temp = data.map(x -> x*x).reduce(Double::sum).orElseGet(() -> -1.0);
    if (temp == -1.0){
      return -1.0;
    }else {
      return Math.pow(temp-mean*mean,0.5);
    }
  }

  private <T extends Comparable> T[] medianAndQuatiles(List<T> data){
    Collections.sort(data);
    T[] triple = (T[]) new Object[3];
    triple[0] = data.get(data.size()/2);
    triple[1] = data.get(data.size()/4);
    triple[2] = data.get(data.size()*3/4);
    return triple;
  }
}
