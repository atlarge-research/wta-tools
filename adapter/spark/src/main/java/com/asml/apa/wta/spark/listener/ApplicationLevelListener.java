package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import scala.collection.JavaConverters;

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

  private final TaskLevelListener taskLevelListener;

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
      TaskLevelListener taskLevelListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.jobLevelListener = jobLevelListener;
    this.taskLevelListener = taskLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  private void setTasks(TaskStageBaseListener taskListener, StageLevelListener stageListener) {
    TaskLevelListener listener = (TaskLevelListener) taskListener;
    final List<Task> tasks = taskListener.getProcessedObjects();
    for (Task task : tasks) {
      // parent children fields
      final int stageId = listener.getTaskToStage().get(task.getId());
      final Integer[] parentStages = stageListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        final Long[] parents = Arrays.stream(parentStages)
            .flatMap(x ->
                Arrays.stream(listener.getStageToTasks().getOrDefault(x, new ArrayList<>()).stream()
                    .map(Task::getId)
                    .toArray(Long[]::new)))
            .toArray(Long[]::new);
        task.setParents(ArrayUtils.toPrimitive(parents));
      }

      List<Integer> childrenStages =
          stageLevelListener.getParentStageToChildrenStages().get(stageId);
      if (childrenStages != null) {
        List<Task> children = new ArrayList<>();
        childrenStages.forEach(
            x -> children.addAll(listener.getStageToTasks().get(x)));
        Long[] temp = children.stream().map(Task::getId).toArray(Long[]::new);
        task.setChildren(ArrayUtils.toPrimitive(temp));
      }

      // resource related fields
      final int resourceProfileId =
          stageLevelListener.getStageToResource().getOrDefault(stageId, -1);
      final ResourceProfile resourceProfile =
          sparkContext.resourceProfileManager().resourceProfileFromId(resourceProfileId);
      final List<TaskResourceRequest> resources = JavaConverters.seqAsJavaList(
          resourceProfile.taskResources().values().toList());
      if (resources.size() > 0) {
        task.setResourceType(resources.get(0).resourceName());
        task.setResourceAmountRequested(resources.get(0).amount());
      }
    }
  }

  private void setStages(StageLevelListener stageListener) {
    final List<Task> stages = stageListener.getProcessedObjects();
    Map<Integer, List<Integer>> parentToChildren = stageListener.getParentStageToChildrenStages();
    for (Task stage : stages) {
      stage.setChildren(parentToChildren.getOrDefault(Math.toIntExact(stage.getId()), new ArrayList<>()).stream()
          .mapToLong(x -> x + 1)
          .toArray());
    }
  }

  private void setWorkflows(JobLevelListener jobListener) {
    final List<Workflow> workflows = jobListener.getProcessedObjects();
    for (Workflow workflow : workflows) {
      workflow.setTotalResources(Arrays.stream(workflow.getTasks())
          .map(Task::getResourceAmountRequested)
          .filter(x -> x >= 0.0)
          .reduce(Double::sum)
          .orElse(-1.0));
    }
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
  @SuppressWarnings("MethodLength")
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    Workload.WorkloadBuilder builder = Workload.builder();
    if (config.isStageLevel()) {
      setStages(stageLevelListener);
    } else {
      setTasks(taskLevelListener, stageLevelListener);
    }
    setWorkflows(jobLevelListener);
    final List<Task> tasks = taskLevelListener.getProcessedObjects();
    List<Workload> processedObjects = this.getProcessedObjects();
    // we should enver enter this branch, this is a guard since an application
    // only terminates once.
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

    final long numSites =
        tasks.stream().filter(x -> x.getSubmissionSite() != -1).count();
    final long numResources = tasks.stream()
        .map(Task::getResourceAmountRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::sum)
        .orElseGet(() -> -1.0)
        .longValue();
    final long numUsers = tasks.stream().filter(x -> x.getUserId() != -1).count();
    final long numGroups = tasks.stream().filter(x -> x.getGroupId() != -1).count();
    final double totalResourceSeconds = tasks.stream()
        .filter(x -> x.getRuntime() >= 0 && x.getResourceAmountRequested() >= 0.0)
        .map(x -> x.getResourceAmountRequested() * x.getRuntime())
        .reduce(Double::sum)
        .orElse(-1.0);

    min(tasks.stream().map(Task::getResourceAmountRequested), builder::minResourceTask);
    max(tasks.stream().map(Task::getResourceAmountRequested), builder::maxResourceTask);

    final long resourceTaskSize = size(tasks.stream().map(Task::getResourceAmountRequested));

    final double meanResourceTask =
        mean(tasks.stream().map(Task::getResourceAmountRequested), resourceTaskSize, builder::meanResourceTask);

    stdAndCov(
        tasks.stream().map(Task::getResourceAmountRequested).filter(x -> x >= 0.0),
        meanResourceTask,
        resourceTaskSize,
        builder::stdResourceTask,
        builder::covResourceTask);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getResourceAmountRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianResourceTask,
        builder::firstQuartileResourceTask,
        builder::thirdQuartileResourceTask);

    min(tasks.stream().map(Task::getMemoryRequested), builder::minMemory);

    max(tasks.stream().map(Task::getMemoryRequested), builder::maxMemory);

    long memorySize = size(tasks.stream().map(Task::getMemoryRequested));

    final double meanMemory = mean(tasks.stream().map(Task::getMemoryRequested), memorySize, builder::meanMemory);

    stdAndCov(
        tasks.stream().map(Task::getMemoryRequested).filter(x -> x >= 0.0),
        meanMemory,
        memorySize,
        builder::stdMemory,
        builder::covMemory);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getMemoryRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianMemory,
        builder::firstQuartileMemory,
        builder::thirdQuartileMemory);

    minLong(tasks.stream().map(Task::getNetworkIoTime), builder::minNetworkUsage);

    maxLong(tasks.stream().map(Task::getNetworkIoTime), builder::maxNetworkUsage);

    final long networkUsageSize =
        size(tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed());
    final double meanNetworkUsage = mean(
        tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed(),
        networkUsageSize,
        builder::meanNetworkUsage);

    stdAndCov(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0.0).map(Long::doubleValue),
        meanNetworkUsage,
        networkUsageSize,
        builder::stdNetworkUsage,
        builder::covNetworkUsage);

    medianAndQuartiles(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0).collect(Collectors.toList()),
        -1L,
        builder::medianNetworkUsage,
        builder::firstQuartileNetworkUsage,
        builder::thirdQuartileNetworkUsage);

    min(tasks.stream().map(Task::getDiskSpaceRequested), builder::minDiskSpaceUsage);

    max(tasks.stream().map(Task::getDiskSpaceRequested), builder::maxDiskSpaceUsage);

    final long diskSpaceSize = size(tasks.stream().map(Task::getDiskSpaceRequested));
    final double meanDiskSpaceUsage =
        mean(tasks.stream().map(Task::getDiskSpaceRequested), diskSpaceSize, builder::meanDiskSpaceUsage);

    stdAndCov(
        tasks.stream().map(Task::getDiskSpaceRequested).filter(x -> x >= 0.0),
        meanDiskSpaceUsage,
        diskSpaceSize,
        builder::stdDiskSpaceUsage,
        builder::covDiskSpaceUsage);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getDiskSpaceRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianDiskSpaceUsage,
        builder::firstQuartileDiskSpaceUsage,
        builder::thirdQuartileDiskSpaceUsage);

    min(tasks.stream().map(Task::getEnergyConsumption), builder::minEnergy);
    max(tasks.stream().map(Task::getEnergyConsumption), builder::maxEnergy);
    final long energySize = size(tasks.stream().map(Task::getEnergyConsumption));
    final double meanEnergy = mean(tasks.stream().map(Task::getEnergyConsumption), energySize, builder::meanEnergy);

    stdAndCov(
        tasks.stream().map(Task::getEnergyConsumption).filter(x -> x >= 0.0),
        meanEnergy,
        energySize,
        builder::stdEnergy,
        builder::covEnergy);

    medianAndQuartiles(
        tasks.stream()
            .map(Task::getEnergyConsumption)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        builder::medianEnergy,
        builder::firstQuartileEnergy,
        builder::thirdQuartileEnergy);

    processedObjects.add(builder.totalWorkflows(numWorkflows)
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
        .build());
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
  private long size(Stream<Double> data) {
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
   * @param size size of the stream
   * @param builder builder
   * @return the mean value
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private double mean(Stream<Double> data, long size, Function<Double, Workload.WorkloadBuilder> builder) {
    double mean = -1.0;
    if (size != 0) {
      mean = data.filter(x -> x >= 0.0).reduce(Double::sum).orElse((double) -size) / size;
    }
    builder.apply(mean);
    return mean;
  }

  /**
   *  Standard deviation value and normalized standard deviation value for the data stream with invalid stream handling.
   *
   * @param data data stream
   * @param mean mean value from previous method
   * @param size size from previous method
   * @param std the builder for the standard deviation field
   * @param cov the builder for the normalized standard deviation field
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void stdAndCov(
      Stream<Double> data,
      Double mean,
      long size,
      Function<Double, Workload.WorkloadBuilder> std,
      Function<Double, Workload.WorkloadBuilder> cov) {
    if (size == 0 || mean == -1.0) {
      std.apply(-1.0);
      cov.apply(-1.0);
    } else {
      double temp = data.map(x -> x * x).reduce(Double::sum).orElse(-1.0);
      temp = Math.pow(temp / size - mean * mean, 0.5);
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
