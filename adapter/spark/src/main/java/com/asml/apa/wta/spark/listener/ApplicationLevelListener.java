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
import com.asml.apa.wta.core.stream.Stream;
import com.asml.apa.wta.core.util.KthSmallest;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

/**
 * Application-level listener for the Spark data source. It's important that one does not override
 * {@link org.apache.spark.scheduler.SparkListenerInterface#onApplicationStart(SparkListenerApplicationStart)}
 * here, as the event is already sent before the listener is registered unless the listener is explicitly registered in
 * the Spark configuration as per <a href="https://stackoverflow.com/questions/36401238/spark-onapplicationstart-is-never-gets-called">SO</a>
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Tianchen Qu
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
@Getter
public class ApplicationLevelListener extends AbstractListener<Workload> {

  private static final int maxAwaitInSeconds = 600;

  private final MetricStreamingEngine metricStreamingEngine;

  private final SparkDataSource sparkDataSource;

  private final WtaWriter wtaWriter;

  private final TaskStageBaseListener wtaTaskListener;

  private final StageLevelListener stageLevelListener;

  private final JobLevelListener jobLevelListener;

  @Getter
  private Workload workload;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext                current spark context
   * @param config                      additional config specified by the user for the plugin
   * @param wtaTaskLevelListener        task-level listener to be used by this listener
   * @param wtaStageLevelListener       stage-level listener to be used by this listener
   * @param wtaJobLevelListener         job-level listener to be used by this listener
   * @param dataSource                  {@link SparkDataSource} to inject
   * @param streamingEngine             driver's {@link MetricStreamingEngine} to use
   * @param traceWriter                 {@link WtaWriter} to write the traces with
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
    wtaTaskListener = wtaTaskLevelListener;
    stageLevelListener = wtaStageLevelListener;
    jobLevelListener = wtaJobLevelListener;
    sparkDataSource = dataSource;
    metricStreamingEngine = streamingEngine;
    wtaWriter = traceWriter;
  }

  /**
   * Constructor for the application-level listener at stage level.
   *
   * @param sparkContext                current spark context
   * @param config                      additional config specified by the user for the plugin
   * @param wtaStageLevelListener       stage-level listener to be used by this listener
   * @param wtaJobLevelListener         job-level listener to be used by this listener
   * @param dataSource                  {@link SparkDataSource} to inject
   * @param streamingEngine             driver's {@link MetricStreamingEngine} to use
   * @param traceWriter                 {@link WtaWriter} to write the traces with
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
    wtaTaskListener = wtaStageLevelListener;
    stageLevelListener = wtaStageLevelListener;
    jobLevelListener = wtaJobLevelListener;
    sparkDataSource = dataSource;
    metricStreamingEngine = streamingEngine;
    wtaWriter = traceWriter;
  }

  /**
   * Writes the trace to file.
   *
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

    sparkDataSource.awaitAndShutdownThreadPool(maxAwaitInSeconds);

    wtaWriter.write(Task.class, tasks);
    wtaWriter.write(Resource.class, resources);
    wtaWriter.write(Workflow.class, sparkDataSource.getJobLevelListener().getProcessedObjects());
    wtaWriter.write(ResourceState.class, resourceStates);
    wtaWriter.write(workload);

    Stream.deleteAllSerializedFiles();
  }

  /**
   * Setters for the general fields of the Workload.
   *
   * @param dateEnd         end-date of the application
   * @param builder         WorkloadBuilder to be used to build the Workload
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
   * @param tasks           list of WTA Task objects
   * @param builder         WorkloadBuilder to be used to build the Workload
   * @since 1.0.0
   */
  private void setCountFields(Stream<Task> tasks, WorkloadBuilder builder) {
    final Stream<Workflow> workflows = jobLevelListener.getProcessedObjects();
    final long totalWorkflows = workflows.copy().count();
    final long totalTasks =
        workflows.map(Workflow::getTaskCount).reduce(Long::sum).orElse(0L);
    long numSites = tasks.copy().countFilter(task -> task.getSubmissionSite() >= 0);
    numSites = numSites < 1 ? -1 : numSites;
    final long numResources = tasks.copy()
        .map(Task::getResourceAmountRequested)
        .filter(task -> task >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0)
        .longValue();
    long numUsers = tasks.copy().countFilter(task -> task.getUserId() >= 0);
    numUsers = numUsers < 1 ? -1 : numUsers;
    long numGroups = tasks.copy().countFilter(task -> task.getGroupId() >= 0);
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
   * @param metrics             {@link List} of WTA Task objects
   * @param resourceType        type of resource to be set
   * @param builder             WorkloadBuilder to be used to build the Workload
   * @since 1.0.0
   */
  @SuppressWarnings("CyclomaticComplexity")
  private void setResourceStatisticsFields(
      Stream<Double> metrics, ResourceType resourceType, WorkloadBuilder builder) {
    Stream<Double> positiveStream = metrics.copy().filter(x -> x >= 0.0);
    final double meanField =
        computeMean(metrics.copy(), positiveStream.copy().count());
    final double stdField = computeStd(
        metrics.filter(x -> x >= 0.0), meanField, positiveStream.copy().count());

    switch (resourceType) {
      case RESOURCE:
        builder.minResourceTask(computeMin(metrics.copy()))
            .maxResourceTask(computeMax(metrics.copy()))
            .meanResourceTask(meanField)
            .stdResourceTask(stdField)
            .covResourceTask(computeCov(meanField, stdField))
            .medianResourceTask(positiveStream.isEmpty() ? -1.0 : computeMedian(positiveStream))
            .firstQuartileResourceTask(
                positiveStream.isEmpty() ? -1.0 : computeFirstQuantile(positiveStream))
            .thirdQuartileResourceTask(
                positiveStream.isEmpty() ? -1.0 : computeThirdQuantile(positiveStream));
        break;
      case MEMORY:
        builder.minMemory(computeMin(metrics.copy()))
            .maxMemory(computeMax(metrics.copy()))
            .meanMemory(meanField)
            .stdMemory(stdField)
            .covMemory(computeCov(meanField, stdField))
            .medianMemory(positiveStream.isEmpty() ? -1.0 : computeMedian(positiveStream))
            .firstQuartileMemory(positiveStream.isEmpty() ? -1.0 : computeFirstQuantile(positiveStream))
            .thirdQuartileMemory(positiveStream.isEmpty() ? -1.0 : computeThirdQuantile(positiveStream));
        break;
      case NETWORK:
        builder.minNetworkUsage((long) computeMin(metrics.copy()))
            .maxNetworkUsage((long) computeMax(metrics.copy()))
            .meanNetworkUsage(meanField)
            .stdNetworkUsage(stdField)
            .covNetworkUsage(computeCov(meanField, stdField))
            .medianNetworkUsage(positiveStream.isEmpty() ? -1L : (long) computeMedian(positiveStream))
            .firstQuartileNetworkUsage(
                positiveStream.isEmpty() ? -1L : (long) computeFirstQuantile(positiveStream))
            .thirdQuartileNetworkUsage(
                positiveStream.isEmpty() ? -1L : (long) computeThirdQuantile(positiveStream));
        break;
      case DISK:
        builder.minDiskSpaceUsage(computeMin(metrics.copy()))
            .maxDiskSpaceUsage(computeMax(metrics.copy()))
            .meanDiskSpaceUsage(meanField)
            .stdDiskSpaceUsage(stdField)
            .covDiskSpaceUsage(computeCov(meanField, stdField))
            .medianDiskSpaceUsage(positiveStream.isEmpty() ? -1.0 : computeMedian(positiveStream))
            .firstQuartileDiskSpaceUsage(
                positiveStream.isEmpty() ? -1.0 : computeFirstQuantile(positiveStream))
            .thirdQuartileDiskSpaceUsage(
                positiveStream.isEmpty() ? -1.0 : computeThirdQuantile(positiveStream));
        break;
      case ENERGY:
        builder.minEnergy(computeMin(metrics.copy()))
            .maxEnergy(computeMax(metrics.copy()))
            .meanEnergy(meanField)
            .stdEnergy(stdField)
            .covEnergy(computeCov(meanField, stdField))
            .medianEnergy(positiveStream.isEmpty() ? -1.0 : computeMedian(positiveStream))
            .firstQuartileEnergy(positiveStream.isEmpty() ? -1.0 : computeFirstQuantile(positiveStream))
            .thirdQuartileEnergy(positiveStream.isEmpty() ? -1.0 : computeThirdQuantile(positiveStream));
    }
  }

  /**
   * Callback function that is called right at the end of the application. Further experimentation
   * is needed to determine if applicationEnd is called first or shutdown.
   *
   * @param applicationEnd        event corresponding to the end of the application
   * @since 1.0.0
   */
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    if (workload != null) {
      log.debug("Application end called twice, this should never happen.");
      return;
    }

    WorkloadBuilder workloadBuilder = Workload.builder();
    final Stream<Task> tasks = wtaTaskListener.getProcessedObjects();

    setGeneralFields(applicationEnd.time(), workloadBuilder);
    setCountFields(tasks.copy(), workloadBuilder);
    if (getConfig().isAggregateMetrics()) {
      Function<Task, Long> networkFunction = Task::getNetworkIoTime;
      setResourceStatisticsFields(
          tasks.copy().map(Task::getResourceAmountRequested), ResourceType.RESOURCE, workloadBuilder);
      setResourceStatisticsFields(
          tasks.copy().map(Task::getMemoryRequested), ResourceType.MEMORY, workloadBuilder);
      setResourceStatisticsFields(
          tasks.copy().map(networkFunction.andThen(Long::doubleValue)),
          ResourceType.NETWORK,
          workloadBuilder);
      setResourceStatisticsFields(
          tasks.copy().map(Task::getDiskSpaceRequested), ResourceType.DISK, workloadBuilder);
      setResourceStatisticsFields(
          tasks.copy().map(Task::getEnergyConsumption), ResourceType.ENERGY, workloadBuilder);
    }

    sparkDataSource.removeListeners();
    workload = workloadBuilder.build();
    writeTrace();
  }

  /**
   * Finds the maximum element inside the double valued stream.
   *
   * @param data              stream of data
   * @return                  double maximum value from data
   * @since 1.0.0
   */
  public double computeMax(Stream<Double> data) {
    return data.reduce(Double::max).orElse(-1.0);
  }

  /**
   * Finds the minimum element of the double valued stream.
   *
   * @param data              stream of data
   * @return                  double minimum value from data
   * @since 1.0.0
   */
  public double computeMin(Stream<Double> data) {
    return data.filter(x -> x >= 0.0).reduce(Double::min).orElse(-1.0);
  }

  /**
   * Mean value for the double type data stream with invalid stream handling.
   *
   * @param data              stream of data
   * @param size              size of the stream
   * @return                  mean value from data or -1.0
   * @since 1.0.0
   */
  public double computeMean(Stream<Double> data, long size) {
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
   * @param data              {@link Stream} of data
   * @return                  median value of the data
   * @since 1.0.0
   */
  private double computeMedian(Stream<Double> data) {
    return new KthSmallest().find(data.copy(), data.copy().count() / 2);
  }

  /**
   * First quantile value for data stream. Assumes that data is not empty, sorted, and positive elements only.
   *
   * @param data              {@link Stream} of data
   * @return                  first quantile value of the data
   * @since 1.0.0
   */
  private double computeFirstQuantile(Stream<Double> data) {
    return new KthSmallest().find(data.copy(), data.copy().count() / 4);
  }

  /**
   * Third quantile value for data stream. Assumes that data is not empty, sorted, and positive elements only.
   *
   * @param data              {@link Stream} of data
   * @return                  third quantile value of the data
   * @since 1.0.0
   */
  private double computeThirdQuantile(Stream<Double> data) {
    return new KthSmallest().find(data.copy(), data.copy().count() * 3 / 4);
  }
}
