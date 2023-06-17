package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import java.util.Collections;
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
 * It's important that one does not override {@link org.apache.spark.scheduler.SparkListenerInterface#onApplicationStart(SparkListenerApplicationStart)} here, as the event is already sent
 * before the listener is registered unless the listener is explicitly registered in the Spark configuration as per <a href="https://stackoverflow.com/questions/36401238/spark-onapplicationstart-is-never-gets-called">SO</a>
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
@Slf4j
public class ApplicationLevelListener extends AbstractListener<Workload> {

  private final JobLevelListener jobLevelListener;

  private final TaskStageBaseListener taskLevelListener;

  private final StageLevelListener stageLevelListener;

  /**
   * Constructor for the application-level listener.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param jobLevelListener The job-level listener to be used by this listener
   * @param taskLevelListener The task-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener to be used by this listener
   * @author Henry Page
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      JobLevelListener jobLevelListener,
      TaskStageBaseListener taskLevelListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.jobLevelListener = jobLevelListener;
    this.taskLevelListener = taskLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Constructor for the application-level listener at stage level.
   *
   * @param sparkContext The current spark context
   * @param config Additional config specified by the user for the plugin
   * @param jobLevelListener The job-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener to be used by this listener
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public ApplicationLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      JobLevelListener jobLevelListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.jobLevelListener = jobLevelListener;
    this.taskLevelListener = stageLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Callback function that is called right at the end of the application. Further experimentation
   * is needed to determine if applicationEnd is called first or shutdown.
   *
   * @param applicationEnd The event corresponding to the end of the application
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    Workload.WorkloadBuilder builder = Workload.builder();
    if (config.isStageLevel()) {
      stageLevelListener.setStages();
    } else {
      TaskLevelListener listener = (TaskLevelListener) taskLevelListener;
      listener.setTasks(stageLevelListener);
    }
    jobLevelListener.setWorkflows();
    final List<Task> tasks = taskLevelListener.getProcessedObjects();
    List<Workload> processedObjects = this.getProcessedObjects();

    if (!processedObjects.isEmpty()) {
      log.debug("Application end called twice, this should never happen");
      return;
    }

    final Workflow[] workflows = jobLevelListener.getProcessedObjects().toArray(new Workflow[0]);
    final int numWorkflows = workflows.length;
    final int totalTasks =
        Arrays.stream(workflows).mapToInt(Workflow::getTaskCount).sum();
    final Domain domain = config.getDomain();
    final long startDate = sparkContext.startTime();
    final long endDate = applicationEnd.time();
    final String[] authors = config.getAuthors();
    final String workloadDescription = config.getDescription();

    setCounters(tasks, builder);
    setMinMax(tasks, builder);
    setMeanStdCov(tasks, builder);
    setMedianAndQuartiles(tasks, builder);
    processedObjects.add(builder.totalWorkflows(numWorkflows)
        .totalTasks(totalTasks)
        .domain(domain)
        .dateStart(startDate)
        .dateEnd(endDate)
        .authors(authors)
        .workloadDescription(workloadDescription)
        .build());
  }

  /**
   * This method sets all statistics about accumulators of the workload.
   *
   * @param tasks all tasks
   * @param builder builder for workload
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void setCounters(List<Task> tasks, Workload.WorkloadBuilder builder) {
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

    builder.numSites(numSites)
        .numResources(numResources)
        .numUsers(numUsers)
        .numGroups(numGroups)
        .totalResourceSeconds(totalResourceSeconds);
  }

  /**
   * This method sets all statistics about minimum value and maximum value.
   *
   * @param tasks all tasks
   * @param builder builder for workload
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void setMinMax(List<Task> tasks, Workload.WorkloadBuilder builder) {
    min(tasks.stream().map(Task::getResourceAmountRequested), builder::minResourceTask);

    max(tasks.stream().map(Task::getResourceAmountRequested), builder::maxResourceTask);

    min(tasks.stream().map(Task::getMemoryRequested), builder::minMemory);

    max(tasks.stream().map(Task::getMemoryRequested), builder::maxMemory);

    minLong(tasks.stream().map(Task::getNetworkIoTime), builder::minNetworkUsage);

    maxLong(tasks.stream().map(Task::getNetworkIoTime), builder::maxNetworkUsage);

    min(tasks.stream().map(Task::getDiskSpaceRequested), builder::minDiskSpaceUsage);

    max(tasks.stream().map(Task::getDiskSpaceRequested), builder::maxDiskSpaceUsage);

    min(tasks.stream().map(Task::getEnergyConsumption), builder::minEnergy);

    max(tasks.stream().map(Task::getEnergyConsumption), builder::maxEnergy);
  }

  /**
   * This method sets all statistics about mean, standard deviation and normalized standard deviation.
   *
   * @param tasks all tasks
   * @param builder builder for workload
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void setMeanStdCov(List<Task> tasks, Workload.WorkloadBuilder builder) {
    final long resourceTaskSize = validSize(tasks.stream().map(Task::getResourceAmountRequested));

    final double meanResourceTask =
        mean(tasks.stream().map(Task::getResourceAmountRequested), resourceTaskSize, builder::meanResourceTask);

    stdAndCov(
        tasks.stream().map(Task::getResourceAmountRequested).filter(resourceAmount -> resourceAmount >= 0.0),
        meanResourceTask,
        resourceTaskSize,
        builder::stdResourceTask,
        builder::covResourceTask);
    long memorySize = validSize(tasks.stream().map(Task::getMemoryRequested));

    final double meanMemory = mean(tasks.stream().map(Task::getMemoryRequested), memorySize, builder::meanMemory);

    stdAndCov(
        tasks.stream().map(Task::getMemoryRequested).filter(memory -> memory >= 0.0),
        meanMemory,
        memorySize,
        builder::stdMemory,
        builder::covMemory);
    final long diskSpaceSize = validSize(tasks.stream().map(Task::getDiskSpaceRequested));
    final double meanDiskSpaceUsage =
        mean(tasks.stream().map(Task::getDiskSpaceRequested), diskSpaceSize, builder::meanDiskSpaceUsage);

    stdAndCov(
        tasks.stream().map(Task::getDiskSpaceRequested).filter(diskSpace -> diskSpace >= 0.0),
        meanDiskSpaceUsage,
        diskSpaceSize,
        builder::stdDiskSpaceUsage,
        builder::covDiskSpaceUsage);
    final long networkUsageSize =
        validSize(tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed());
    final double meanNetworkUsage = mean(
        tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed(),
        networkUsageSize,
        builder::meanNetworkUsage);

    stdAndCov(
        tasks.stream()
            .map(Task::getNetworkIoTime)
            .filter(networkIo -> networkIo >= 0.0)
            .map(Long::doubleValue),
        meanNetworkUsage,
        networkUsageSize,
        builder::stdNetworkUsage,
        builder::covNetworkUsage);
    final long energySize = validSize(tasks.stream().map(Task::getEnergyConsumption));
    final double meanEnergy = mean(tasks.stream().map(Task::getEnergyConsumption), energySize, builder::meanEnergy);

    stdAndCov(
        tasks.stream().map(Task::getEnergyConsumption).filter(energy -> energy >= 0.0),
        meanEnergy,
        energySize,
        builder::stdEnergy,
        builder::covEnergy);
  }

  /**
   * This method sets all statistics about median and Quartiles.
   *
   * @param tasks all tasks
   * @param builder builder for workload
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void setMedianAndQuartiles(List<Task> tasks, Workload.WorkloadBuilder builder) {
    medianAndQuartiles(
        tasks.stream()
            .map(Task::getResourceAmountRequested)
            .filter(resourceAmount -> resourceAmount >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianResourceTask,
        builder::firstQuartileResourceTask,
        builder::thirdQuartileResourceTask);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getMemoryRequested)
            .filter(memory -> memory >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianMemory,
        builder::firstQuartileMemory,
        builder::thirdQuartileMemory);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getNetworkIoTime)
            .filter(networkIo -> networkIo >= 0)
            .collect(Collectors.toList()),
        -1L,
        builder::medianNetworkUsage,
        builder::firstQuartileNetworkUsage,
        builder::thirdQuartileNetworkUsage);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getDiskSpaceRequested)
            .filter(diskSpace -> diskSpace >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianDiskSpaceUsage,
        builder::firstQuartileDiskSpaceUsage,
        builder::thirdQuartileDiskSpaceUsage);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getEnergyConsumption)
            .filter(energy -> energy >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianEnergy,
        builder::firstQuartileEnergy,
        builder::thirdQuartileEnergy);
  }

  /**
   * This return the size of the data after filtering for positive ones.
   * It is needed for calculation of mean and standard deviation.
   *
   * @param data stream of data
   * @return the size of positive elements
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private long validSize(Stream<Double> data) {
    return data.filter(x -> x >= 0.0).count();
  }

  /**
   * Find the maximum element inside the double valued stream and give it to the field of the builder.
   *
   * @param data data
   * @param builder builder
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void max(Stream<Double> data, Function<Double, Workload.WorkloadBuilder> builder) {
    builder.apply(data.filter(x -> x >= 0.0).reduce(Double::max).orElse(-1.0));
  }

  /**
   * Find the minimum element of the double valued stream and set the corresponding field in the builder as that value.
   *
   * @param data data stream
   * @param builder builder
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void min(Stream<Double> data, Function<Double, Workload.WorkloadBuilder> builder) {
    builder.apply(data.filter(x -> x >= 0.0).reduce(Double::min).orElse(-1.0));
  }

  /**
   * Maximum function for long value.
   *
   * @param data data stream
   * @param builder builder
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void maxLong(Stream<Long> data, Function<Long, Workload.WorkloadBuilder> builder) {
    builder.apply(data.filter(x -> x >= 0).reduce(Long::max).orElse(-1L));
  }

  /**
   * Minimum function for long value.
   *
   * @param data data stream
   * @param builder builder
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void minLong(Stream<Long> data, Function<Long, Workload.WorkloadBuilder> builder) {
    builder.apply(data.filter(x -> x >= 0).reduce(Long::min).orElse(-1L));
  }

  /**
   * Mean value for the data stream with invalid stream handling.
   * Will set up the corresponding field with the builder parameter that is passed.
   *
   * @param data data stream
   * @param validSize size of the stream
   * @param builder builder
   * @return the mean value
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private double mean(Stream<Double> data, long validSize, Function<Double, Workload.WorkloadBuilder> builder) {
    double mean = -1.0;
    if (validSize != 0) {
      mean = data.filter(x -> x >= 0.0).reduce(Double::sum).orElse((double) -validSize) / validSize;
    }
    builder.apply(mean);
    return mean;
  }

  /**
   *  Standard deviation value and normalized standard deviation value for the data stream with invalid stream handling.
   *
   * @param data data stream
   * @param mean mean value from previous method
   * @param validSize size from previous method
   * @param std the builder for the standard deviation field
   * @param cov the builder for the normalized standard deviation field
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void stdAndCov(
      Stream<Double> data,
      Double mean,
      long validSize,
      Function<Double, Workload.WorkloadBuilder> std,
      Function<Double, Workload.WorkloadBuilder> cov) {
    if (validSize == 0 || mean == -1.0) {
      std.apply(-1.0);
      cov.apply(-1.0);
    } else {
      double temp = data.map(x -> x * x).reduce(Double::sum).orElse(-1.0);
      temp = Math.pow(temp / validSize - mean * mean, 0.5);
      std.apply(temp);
      if (mean == 0.0) {
        cov.apply(-1.0);
      } else {
        cov.apply(temp / mean);
      }
    }
  }

  /**
   * This method will set the median, first quartile and third quartile elements for the given data list.
   *
   * @param data data list
   * @param defaultValue the default median,first quartile, third quartile value given invalid list
   * @param median builder for median value
   * @param firstQuartile builder for first quartile value
   * @param thirdQuartile builder for third quartile value
   * @param <T> the type of the list, needs to be comparable to sort the list
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private <T extends Comparable<T>> void medianAndQuartiles(
      List<T> data,
      T defaultValue,
      Function<T, Workload.WorkloadBuilder> median,
      Function<T, Workload.WorkloadBuilder> firstQuartile,
      Function<T, Workload.WorkloadBuilder> thirdQuartile) {
    if (data.size() == 0) {
      median.apply(defaultValue);
      firstQuartile.apply(defaultValue);
      thirdQuartile.apply(defaultValue);
    } else {
      Collections.sort(data);
      median.apply(data.get(data.size() / 2));
      firstQuartile.apply(data.get(data.size() / 4));
      thirdQuartile.apply(data.get(data.size() * 3 / 4));
    }
  }
}
