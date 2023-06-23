package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.Workload.WorkloadBuilder;
import com.asml.apa.wta.core.streams.Stream;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  private final MetricStreamingEngine metricStreamingEngine;

  private final SparkDataSource sparkDataSource;

  private final WtaWriter wtaWriter;

  private final TaskStageBaseListener taskLevelListener;

  private final StageLevelListener stageLevelListener;

  private final JobLevelListener jobLevelListener;

  @Getter
  private Workload workload;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param wtaTaskLevelListener The task-level listener to be used by this listener
   * @param wtaStageLevelListener The stage-level listener to be used by this listener
   * @param wtaJobLevelListener The job-level listener to be used by this listener
   * @param dataSource the {@link SparkDataSource} to inject
   * @param streamingEngine the driver's {@link MetricStreamingEngine} to use
   * @param traceWriter the {@link WtaWriter} to write the traces with
   * @author Henry Page
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      TaskStageBaseListener wtaTaskLevelListener,
      StageLevelListener wtaStageLevelListener,
      JobLevelListener wtaJobLevelListener,
      SparkDataSource dataSource,
      MetricStreamingEngine streamingEngine,
      WtaWriter traceWriter) {
    super(sparkContext, config);
    taskLevelListener = wtaTaskLevelListener;
    stageLevelListener = wtaStageLevelListener;
    jobLevelListener = wtaJobLevelListener;
    sparkDataSource = dataSource;
    metricStreamingEngine = streamingEngine;
    wtaWriter = traceWriter;
  }

  /**
   * Constructor for the application-level listener at stage level.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param wtaStageLevelListener The stage-level listener to be used by this listener
   * @param wtaJobLevelListener The job-level listener to be used by this listener
   * @param dataSource the {@link SparkDataSource} to inject
   * @param streamingEngine the driver's {@link MetricStreamingEngine} to use
   * @param traceWriter the {@link WtaWriter} to write the traces with
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      StageLevelListener wtaStageLevelListener,
      JobLevelListener wtaJobLevelListener,
      SparkDataSource dataSource,
      MetricStreamingEngine streamingEngine,
      WtaWriter traceWriter) {
    super(sparkContext, config);
    taskLevelListener = wtaStageLevelListener;
    stageLevelListener = wtaStageLevelListener;
    jobLevelListener = wtaJobLevelListener;
    sparkDataSource = dataSource;
    metricStreamingEngine = streamingEngine;
    wtaWriter = traceWriter;
  }

  /**
   * Writes the trace to file.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void writeTrace() {
    List<ResourceAndStateWrapper> resourceAndStateWrappers = metricStreamingEngine.collectResourceInformation();
    Stream<Resource> resources = new Stream<>();
    resourceAndStateWrappers.stream()
        .map(ResourceAndStateWrapper::getResource)
        .forEach(resources::addToStream);
    Stream<ResourceState> resourceStates = new Stream<>();
    resourceAndStateWrappers.forEach(rs -> rs.getStates().forEach(resourceStates::addToStream));
    Stream<Task> tasks = sparkDataSource.getRuntimeConfig().isStageLevel()
        ? sparkDataSource.getStageLevelListener().getProcessedObjects()
        : sparkDataSource.getTaskLevelListener().getProcessedObjects();
    joinThreadPool();
    wtaWriter.write(Task.class, tasks);
    wtaWriter.write(Resource.class, resources);
    wtaWriter.write(Workflow.class, sparkDataSource.getJobLevelListener().getProcessedObjects());
    wtaWriter.write(ResourceState.class, resourceStates);
    wtaWriter.write(workload);

    Stream.deleteAllSerializedFiles();
  }

  /**
   * Shuts down the thread pool and joins all of its threads.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void joinThreadPool() {
    try {
      sparkDataSource.join();
    } catch (InterruptedException e) {
      log.error("Could not join the threads because of {}.", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Removes the listeners from the Spark context on application end.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void removeListeners() {
    sparkDataSource.removeTaskListener();
    sparkDataSource.removeStageListener();
    sparkDataSource.removeJobListener();
    sparkDataSource.removeApplicationListener();
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
    final Domain domain = getConfig().getDomain();
    final long dateStart = getSparkContext().startTime();
    final String[] authors = getConfig().getAuthors();
    final String workloadDescription = getConfig().getDescription();
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
  private void setCountFields(Stream<Task> tasks, WorkloadBuilder builder) {
    final Stream<Workflow> workflows = jobLevelListener.getProcessedObjects();
    final long totalWorkflows = workflows.copy().count();
    final long totalTasks =
        workflows.map(Workflow::getTaskCount).reduce(Long::sum).orElse(0L);
    long numSites =
        tasks.copy().filter(task -> task.getSubmissionSite() >= 0).count();
    numSites = numSites < 1 ? -1 : numSites;
    final long numResources = tasks.copy()
        .map(Task::getResourceAmountRequested)
        .filter(task -> task >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0)
        .longValue();
    long numUsers = tasks.copy().filter(task -> task.getUserId() >= 0).count();
    numUsers = numUsers < 1 ? -1 : numUsers;
    long numGroups = tasks.copy().filter(task -> task.getGroupId() >= 0).count();
    numGroups = numGroups < 1 ? -1 : numGroups;
    final double totalResourceSeconds = tasks.copy()
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
    RESOURCE,
    MEMORY,
    DISK,
    NETWORK,
    ENERGY
  }

  /**
   * Setters for the statistical resource fields of the Workload.
   *
   * @param tasks           List of WTA Task objects
   * @param resourceType    Type of resource to be set
   * @param builder         WorkloadBuilder to be used to build the Workload
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @SuppressWarnings("CyclomaticComplexity")
  private void setResourceStatisticsFields(List<Double> tasks, ResourceType resourceType, WorkloadBuilder builder) {
    List<Double> sortedPositiveList =
        tasks.stream().filter(x -> x >= 0.0).sorted().collect(Collectors.toList());
    final double meanField = computeMean(new Stream<>(tasks), sortedPositiveList.size());
    final double stdField = computeStd(
        new Stream<>(tasks.stream().filter(x -> x >= 0.0).collect(Collectors.toList())),
        meanField,
        sortedPositiveList.size());

    switch (resourceType) {
      case RESOURCE:
        builder.minResourceTask(computeMin(new Stream<>(tasks)))
            .maxResourceTask(computeMax(new Stream<>(tasks)))
            .meanResourceTask(meanField)
            .stdResourceTask(stdField)
            .covResourceTask(computeCov(meanField, stdField))
            .medianResourceTask(sortedPositiveList.isEmpty() ? -1.0 : computeMedian(sortedPositiveList))
            .firstQuartileResourceTask(
                sortedPositiveList.isEmpty() ? -1.0 : computeFirstQuantile(sortedPositiveList))
            .thirdQuartileResourceTask(
                sortedPositiveList.isEmpty() ? -1.0 : computeThirdQuantile(sortedPositiveList));
        break;
      case MEMORY:
        builder.minMemory(computeMin(new Stream<>(tasks)))
            .maxMemory(computeMax(new Stream<>(tasks)))
            .meanMemory(meanField)
            .stdMemory(stdField)
            .covMemory(computeCov(meanField, stdField))
            .medianMemory(sortedPositiveList.isEmpty() ? -1.0 : computeMedian(sortedPositiveList))
            .firstQuartileMemory(
                sortedPositiveList.isEmpty() ? -1.0 : computeFirstQuantile(sortedPositiveList))
            .thirdQuartileMemory(
                sortedPositiveList.isEmpty() ? -1.0 : computeThirdQuantile(sortedPositiveList));
        break;
      case NETWORK:
        builder.minNetworkUsage((long) computeMin(new Stream<>(tasks)))
            .maxNetworkUsage((long) computeMax(new Stream<>(tasks)))
            .meanNetworkUsage(meanField)
            .stdNetworkUsage(stdField)
            .covNetworkUsage(computeCov(meanField, stdField))
            .medianNetworkUsage(
                sortedPositiveList.isEmpty() ? -1L : (long) computeMedian(sortedPositiveList))
            .firstQuartileNetworkUsage(
                sortedPositiveList.isEmpty() ? -1L : (long) computeFirstQuantile(sortedPositiveList))
            .thirdQuartileNetworkUsage(
                sortedPositiveList.isEmpty() ? -1L : (long) computeThirdQuantile(sortedPositiveList));
        break;
      case DISK:
        builder.minDiskSpaceUsage(computeMin(new Stream<>(tasks)))
            .maxDiskSpaceUsage(computeMax(new Stream<>(tasks)))
            .meanDiskSpaceUsage(meanField)
            .stdDiskSpaceUsage(stdField)
            .covDiskSpaceUsage(computeCov(meanField, stdField))
            .medianDiskSpaceUsage(sortedPositiveList.isEmpty() ? -1.0 : computeMedian(sortedPositiveList))
            .firstQuartileDiskSpaceUsage(
                sortedPositiveList.isEmpty() ? -1.0 : computeFirstQuantile(sortedPositiveList))
            .thirdQuartileDiskSpaceUsage(
                sortedPositiveList.isEmpty() ? -1.0 : computeThirdQuantile(sortedPositiveList));
        break;
      case ENERGY:
        builder.minEnergy(computeMin(new Stream<>(tasks)))
            .maxEnergy(computeMax(new Stream<>(tasks)))
            .meanEnergy(meanField)
            .stdEnergy(stdField)
            .covEnergy(computeCov(meanField, stdField))
            .medianEnergy(sortedPositiveList.isEmpty() ? -1.0 : computeMedian(sortedPositiveList))
            .firstQuartileEnergy(
                sortedPositiveList.isEmpty() ? -1.0 : computeFirstQuantile(sortedPositiveList))
            .thirdQuartileEnergy(
                sortedPositiveList.isEmpty() ? -1.0 : computeThirdQuantile(sortedPositiveList));
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
    if (containsProcessedObjects()) {
      log.debug("Application end called twice, this should never happen.");
      return;
    }

    WorkloadBuilder workloadBuilder = Workload.builder();
    final Stream<Task> tasks = taskLevelListener.getProcessedObjects();
    Function<Task, Long> networkFunction = Task::getNetworkIoTime;

    setGeneralFields(applicationEnd.time(), workloadBuilder);
    setCountFields(tasks.copy(), workloadBuilder);
    setResourceStatisticsFields(
        tasks.copy().map(Task::getResourceAmountRequested).toList(), ResourceType.RESOURCE, workloadBuilder);
    setResourceStatisticsFields(
        tasks.copy().map(Task::getMemoryRequested).toList(), ResourceType.MEMORY, workloadBuilder);
    setResourceStatisticsFields(
        tasks.copy().map(networkFunction.andThen(Long::doubleValue)).toList(),
        ResourceType.NETWORK,
        workloadBuilder);
    setResourceStatisticsFields(
        tasks.copy().map(Task::getDiskSpaceRequested).toList(), ResourceType.DISK, workloadBuilder);
    setResourceStatisticsFields(
        tasks.copy().map(Task::getEnergyConsumption).toList(), ResourceType.ENERGY, workloadBuilder);

    removeListeners();

    workload = workloadBuilder.build();
    writeTrace();
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
    return data.reduce(Double::max).orElse(-1.0);
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
   * Standard deviation value for data stream with invalid stream handling. Assumes the data has
   * positive elements only.
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
    double numerator = data.map(x -> x * x).reduce(Double::sum).orElse(-1.0);
    if (numerator == -1.0) {
      return -1.0;
    }
    return Math.pow(numerator / size - mean * mean, 0.5);
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
    if (mean == 0.0 || mean == -1.0 || std == -1.0) {
      return -1.0;
    }
    return std / mean;
  }

  /**
   * Median value for data stream. Assumes that data is not empty, sorted, and positive elements only.
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
   * First quantile value for data stream. Assumes that data is not empty, sorted, and positive elements only.
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
   * Third quantile value for data stream. Assumes that data is not empty, sorted, and positive elements only.
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
