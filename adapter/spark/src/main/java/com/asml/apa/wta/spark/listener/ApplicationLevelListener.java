package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.Workload.WorkloadBuilder;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

/**
 * This class is an application-level listener for the Spark data source.
 * It's important that one does not override {@link org.apache.spark.scheduler.SparkListenerInterface#onApplicationStart(SparkListenerApplicationStart)}
 * here, as the event is already sent before the listener is registered unless the listener is explicitly registered in
 * the Spark configuration as per <a href="https://stackoverflow.com/questions/36401238/spark-onapplicationstart-is-never-gets-called">SO</a>
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
@Slf4j
public class ApplicationLevelListener extends AbstractListener<Workload> {

  private final TaskStageBaseListener wtaTaskListener;

  private final StageLevelListener stageLevelListener;

  private final JobLevelListener jobLevelListener;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param wtaTaskListener The task-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener to be used by this listener
   * @param jobLevelListener The job-level listener to be used by this listener
   * @author Henry Page
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      TaskStageBaseListener wtaTaskListener,
      StageLevelListener stageLevelListener,
      JobLevelListener jobLevelListener) {
    super(sparkContext, config);
    this.wtaTaskListener = wtaTaskListener;
    this.stageLevelListener = stageLevelListener;
    this.jobLevelListener = jobLevelListener;
  }

  /**
   * Constructor for the application-level listener at stage level.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param stageLevelListener The stage-level listener to be used by this listener
   * @param jobLevelListener The job-level listener to be used by this listener
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      StageLevelListener stageLevelListener,
      JobLevelListener jobLevelListener) {
    super(sparkContext, config);
    this.wtaTaskListener = stageLevelListener;
    this.stageLevelListener = stageLevelListener;
    this.jobLevelListener = jobLevelListener;
  }

  /**
   * Setters for the general fields of the Workload.
   *
   * @param dateEnd     End date of the application
   * @param builder     WorkloadBuilder to be used to build the Workload
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private void setGeneralFields(long dateEnd, WorkloadBuilder builder) {
    final Domain domain = config.getDomain();
    final long dateStart = sparkContext.startTime();
    final String[] authors = config.getAuthors();
    final String workloadDescription = config.getDescription();
    builder.authors(authors)
        .workloadDescription(workloadDescription)
        .domain(domain)
        .dateStart(dateStart)
        .dateEnd(dateEnd);
  }

  /**
   * Setters for the count fields of the Workload.
   *
   * @param tasks       List of WTA Task objects
   * @param builder     WorkloadBuilder to be used to build the Workload
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private void setCountFields(List<Task> tasks, WorkloadBuilder builder) {
    final Workflow[] workflows = jobLevelListener.getProcessedObjects().toArray(new Workflow[0]);
    final int totalWorkflows = workflows.length;
    final int totalTasks =
        Arrays.stream(workflows).mapToInt(Workflow::getTaskCount).sum();
    final long numSites =
        tasks.stream().filter(task -> task.getSubmissionSite() != -1).count();
    final long numResources = tasks.stream()
        .map(Task::getResourceAmountRequested)
        .filter(task -> task >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0)
        .longValue();
    final long numUsers =
        tasks.stream().filter(task -> task.getUserId() != -1).count();
    final long numGroups =
        tasks.stream().filter(task -> task.getGroupId() != -1).count();
    final double totalResourceSeconds = tasks.stream()
        .filter(task -> task.getRuntime() >= 0 && task.getResourceAmountRequested() >= 0.0)
        .map(task -> task.getResourceAmountRequested() * task.getRuntime())
        .reduce(Double::sum)
        .orElse(-1.0);
    builder.totalWorkflows(totalWorkflows)
        .totalTasks(totalTasks)
        .numSites(numSites)
        .numResources(numResources)
        .numUsers(numUsers)
        .numGroups(numGroups)
        .totalResourceSeconds(totalResourceSeconds);
  }

  /**
   * Private enum to represent the different types of resources for resource fields.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private enum ResourceType {
    RESOURCE(),
    MEMORY(),
    DISK(),
    NETWORK(),
    ENERGY();

    ResourceType() {}
  }

  /**
   * Setters for the statistical resource fields of the Workload.
   *
   * @param tasks           List of WTA Task objects
   * @param function        Function to be applied to the tasks to get the resource field
   * @param resourceType    Type of resource to be set
   * @param builder         WorkloadBuilder to be used to build the Workload
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @SuppressWarnings("CyclomaticComplexity")
  private void setResourceStatisticsFields(
      List<Task> tasks, Function<Task, Double> function, ResourceType resourceType, WorkloadBuilder builder) {
    List<Double> positiveList =
        tasks.stream().map(function).filter(x -> x >= 0.0).collect(Collectors.toList());
    final double meanField = computeMean(tasks.stream().map(function), positiveList.size());
    final double stdField =
        computeStd(tasks.stream().map(function).filter(x -> x >= 0.0), meanField, positiveList.size());

    switch (resourceType) {
      case RESOURCE:
        builder.minResourceTask(computeMin(tasks.stream().map(function)))
            .maxResourceTask(computeMax(tasks.stream().map(function)))
            .meanResourceTask(meanField)
            .stdResourceTask(stdField)
            .covResourceTask(computeCov(meanField, stdField))
            .medianResourceTask(positiveList.isEmpty() ? -1.0 : computeMedian(positiveList))
            .firstQuartileResourceTask(positiveList.isEmpty() ? -1.0 : computeFirstQuantile(positiveList))
            .thirdQuartileResourceTask(positiveList.isEmpty() ? -1.0 : computeThirdQuantile(positiveList));
        break;
      case MEMORY:
        builder.minMemory(computeMin(tasks.stream().map(function)))
            .maxMemory(computeMax(tasks.stream().map(function)))
            .meanMemory(meanField)
            .stdMemory(stdField)
            .covMemory(computeCov(meanField, stdField))
            .medianMemory(positiveList.isEmpty() ? -1.0 : computeMedian(positiveList))
            .firstQuartileMemory(positiveList.isEmpty() ? -1.0 : computeFirstQuantile(positiveList))
            .thirdQuartileMemory(positiveList.isEmpty() ? -1.0 : computeThirdQuantile(positiveList));
        break;
      case NETWORK:
        builder.minNetworkUsage((long) computeMin(tasks.stream().map(function)))
            .maxNetworkUsage((long) computeMax(tasks.stream().map(function)))
            .meanNetworkUsage(meanField)
            .stdNetworkUsage(stdField)
            .covNetworkUsage(computeCov(meanField, stdField))
            .medianNetworkUsage(positiveList.isEmpty() ? -1L : (long) computeMedian(positiveList))
            .firstQuartileNetworkUsage(
                positiveList.isEmpty() ? -1L : (long) computeFirstQuantile(positiveList))
            .thirdQuartileNetworkUsage(
                positiveList.isEmpty() ? -1L : (long) computeThirdQuantile(positiveList));
        break;
      case DISK:
        builder.minDiskSpaceUsage(computeMin(tasks.stream().map(function)))
            .maxDiskSpaceUsage(computeMax(tasks.stream().map(function)))
            .meanDiskSpaceUsage(meanField)
            .stdDiskSpaceUsage(stdField)
            .covDiskSpaceUsage(computeCov(meanField, stdField))
            .medianDiskSpaceUsage(positiveList.isEmpty() ? -1.0 : computeMedian(positiveList))
            .firstQuartileDiskSpaceUsage(positiveList.isEmpty() ? -1.0 : computeFirstQuantile(positiveList))
            .thirdQuartileDiskSpaceUsage(
                positiveList.isEmpty() ? -1.0 : computeThirdQuantile(positiveList));
        break;
      case ENERGY:
        builder.minEnergy(computeMin(tasks.stream().map(function)))
            .maxEnergy(computeMax(tasks.stream().map(function)))
            .meanEnergy(meanField)
            .stdEnergy(stdField)
            .covEnergy(computeCov(meanField, stdField))
            .medianEnergy(positiveList.isEmpty() ? -1.0 : computeMedian(positiveList))
            .firstQuartileEnergy(positiveList.isEmpty() ? -1.0 : computeFirstQuantile(positiveList))
            .thirdQuartileEnergy(positiveList.isEmpty() ? -1.0 : computeThirdQuantile(positiveList));
    }
  }

  /**
   * Callback function that is called right at the end of the application. Further experimentation
   * is needed to determine if applicationEnd is called first or shutdown.
   *
   * @param applicationEnd    The event corresponding to the end of the application
   * @author Henry Page
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    // we should never enter this branch, this is a guard since an application only terminates once.
    if (!this.getProcessedObjects().isEmpty()) {
      log.debug("Application end called twice, this should never happen");
      return;
    }

    if (config.isStageLevel()) {
      stageLevelListener.setStages();
    } else {
      TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
      taskLevelListener.setTasks(stageLevelListener);
    }
    jobLevelListener.setWorkflows();

    WorkloadBuilder workloadBuilder = Workload.builder();
    final List<Task> tasks = wtaTaskListener.getProcessedObjects();
    Function<Task, Long> networkFunction = Task::getNetworkIoTime;

    setGeneralFields(applicationEnd.time(), workloadBuilder);
    setCountFields(tasks, workloadBuilder);
    setResourceStatisticsFields(tasks, Task::getResourceAmountRequested, ResourceType.RESOURCE, workloadBuilder);
    setResourceStatisticsFields(tasks, Task::getMemoryRequested, ResourceType.MEMORY, workloadBuilder);
    setResourceStatisticsFields(
        tasks, networkFunction.andThen(Long::doubleValue), ResourceType.NETWORK, workloadBuilder);
    setResourceStatisticsFields(tasks, Task::getDiskSpaceRequested, ResourceType.DISK, workloadBuilder);
    setResourceStatisticsFields(tasks, Task::getEnergyConsumption, ResourceType.ENERGY, workloadBuilder);
    this.getProcessedObjects().add(workloadBuilder.build());
  }

  /**
   * Finds the maximum element inside the double valued stream.
   *
   * @param data              stream of data
   * @return                  double maximum value from data
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeMax(Stream<Double> data) {
    return data.filter(x -> x >= 0.0).reduce(Double::max).orElse(-1.0);
  }

  /**
   * Finds the minimum element of the double valued stream.
   *
   * @param data              stream of data
   * @return                  double minimum value from data
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeMin(Stream<Double> data) {
    return data.filter(x -> x >= 0.0).reduce(Double::min).orElse(-1.0);
  }

  /**
   * Mean value for the double type data stream with invalid stream handling.
   *
   * @param data              stream of data
   * @param size              size of the stream
   * @return                  mean value from data or -1.0
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeMean(Stream<Double> data, long size) {
    if (size == 0) {
      return -1.0;
    }
    return data.filter(x -> x >= 0.0).reduce(Double::sum).orElse((double) -size) / size;
  }

  /**
   * Standard deviation value for data stream with invalid stream handling.
   *
   * @param data              stream of data
   * @param mean              mean value from {@link #computeMean(Stream, long)}
   * @param size              size from data
   * @return                  standard deviation value from data or -1.0
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeStd(Stream<Double> data, double mean, long size) {
    if (size == 0 || mean == -1.0) {
      return -1.0;
    }
    double numerator =
        data.map(x -> Math.pow(x - mean, 2)).reduce(Double::sum).orElse(-1.0);
    return Math.pow(numerator / size, 0.5);
  }

  /**
   * Normalized deviation value for data stream with invalid stream handling.
   *
   * @param mean              mean value from {@link #computeMean(Stream, long)}
   * @param std               standard deviation from {@link #computeStd(Stream, double, long)}
   * @return                  normalized standard deviation value from data or -1.0
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeCov(double mean, double std) {
    if (mean == 0.0 || mean == -1.0) {
      return -1.0;
    }
    return std / mean;
  }

  /**
   * Median value for data stream. Assumes that data is not empty.
   *
   * @param data            stream of data
   * return                 median value of the data
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeMedian(List<Double> data) {
    return data.get(data.size() / 2);
  }

  /**
   * First quantile value for data stream. Assumes that data is not empty.
   *
   * @param data            stream of data
   * return                 first quantile value of the data
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeFirstQuantile(List<Double> data) {
    return data.get(data.size() / 4);
  }

  /**
   * Third quantile value for data stream. Assumes that data is not empty.
   *
   * @param data            stream of data
   * return                 third quantile value of the data
   * @author Tianchen Qu
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private double computeThirdQuantile(List<Double> data) {
    return data.get(data.size() * 3 / 4);
  }
}
