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
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.resource.TaskResourceRequest;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import scala.collection.JavaConverters;

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

  private void setTaskFields(TaskStageBaseListener wtaTaskListener, StageLevelListener stageListener) {
    TaskLevelListener taskLevelListener = (TaskLevelListener) wtaTaskListener;
    final List<Task> tasks = taskLevelListener.getProcessedObjects();
    for (Task task : tasks) {
      // set parent field
      final long stageId = taskLevelListener.getTaskToStage().get(task.getId());
      final Long[] parentStages = stageListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        final Long[] parents = Arrays.stream(parentStages)
            .flatMap(x ->
                Arrays.stream(taskLevelListener.getStageToTasks().getOrDefault(x, new ArrayList<>()).stream()
                    .map(Task::getId)
                    .toArray(Long[]::new)))
            .toArray(Long[]::new);
        task.setParents(ArrayUtils.toPrimitive(parents));
      }

      // set children field
      List<Long> childrenStages =
          stageLevelListener.getParentStageToChildrenStages().get(stageId);
      if (childrenStages != null) {
        List<Task> children = new ArrayList<>();
        childrenStages.forEach(
            x -> children.addAll(taskLevelListener.getStageToTasks().get(x)));
        Long[] temp = children.stream().map(Task::getId).toArray(Long[]::new);
        task.setChildren(ArrayUtils.toPrimitive(temp));
      }

      // set resource related fields
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

  private void setStageFields(StageLevelListener stageListener) {
    Map<Long, List<Long>> parentToChildren = stageListener.getParentStageToChildrenStages();
    final List<Task> stages = stageListener.getProcessedObjects();
    stages.forEach(stage -> stage.setChildren(
            parentToChildren.getOrDefault(stage.getId(), new ArrayList<>()).stream()
                    .mapToLong(x -> x + 1)
                    .toArray()));
  }

  private void setWorkflowFields(JobLevelListener jobListener) {
    final List<Workflow> workflows = jobListener.getProcessedObjects();
    workflows.forEach(workflow -> workflow.setTotalResources(Arrays.stream(workflow.getTasks())
        .map(Task::getResourceAmountRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::sum)
        .orElse(-1.0)));
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
    // we should never enter this branch, this is a guard since an application only terminates once.
    if (!this.getProcessedObjects().isEmpty()) {
      log.debug("Application end called twice, this should never happen");
      return;
    }

    if (config.isStageLevel()) {
      setStageFields(stageLevelListener);
    } else {
      setTaskFields(wtaTaskListener, stageLevelListener);
    }
    setWorkflowFields(jobLevelListener);



    Workload.WorkloadBuilder workloadBuilder = Workload.builder();
    final List<Task> tasks = wtaTaskListener.getProcessedObjects();

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
        .filter(task -> task.getRuntime() >= 0 && task.getResourceAmountRequested() >= 0.0)
        .map(task -> task.getResourceAmountRequested() * task.getRuntime())
        .reduce(Double::sum)
        .orElse(-1.0);

    computeDoubleMin(tasks.stream().map(Task::getResourceAmountRequested), workloadBuilder::minResourceTask);
    computeDoubleMax(tasks.stream().map(Task::getResourceAmountRequested), workloadBuilder::maxResourceTask);

    final long resourceTaskSize = positiveDataSize(tasks.stream().map(Task::getResourceAmountRequested));

    final double meanResourceTask =
        computeMean(tasks.stream().map(Task::getResourceAmountRequested), resourceTaskSize, workloadBuilder::meanResourceTask);

    computeStdAndCov(
        tasks.stream().map(Task::getResourceAmountRequested).filter(x -> x >= 0.0),
        meanResourceTask,
        resourceTaskSize,
        workloadBuilder::stdResourceTask,
        workloadBuilder::covResourceTask);

    computeMedianAndQuantile(
        tasks.stream()
            .map(Task::getResourceAmountRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        workloadBuilder::medianResourceTask,
        workloadBuilder::firstQuartileResourceTask,
        workloadBuilder::thirdQuartileResourceTask);

    computeDoubleMin(tasks.stream().map(Task::getMemoryRequested), workloadBuilder::minMemory);
    computeDoubleMax(tasks.stream().map(Task::getMemoryRequested), workloadBuilder::maxMemory);

    long memorySize = positiveDataSize(tasks.stream().map(Task::getMemoryRequested));

    final double meanMemory = computeMean(tasks.stream().map(Task::getMemoryRequested), memorySize, workloadBuilder::meanMemory);

    computeStdAndCov(
        tasks.stream().map(Task::getMemoryRequested).filter(x -> x >= 0.0),
        meanMemory,
        memorySize,
        workloadBuilder::stdMemory,
        workloadBuilder::covMemory);

    computeMedianAndQuantile(
        tasks.stream()
            .map(Task::getMemoryRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        workloadBuilder::medianMemory,
        workloadBuilder::firstQuartileMemory,
        workloadBuilder::thirdQuartileMemory);

    computeLongMin(tasks.stream().map(Task::getNetworkIoTime), workloadBuilder::minNetworkUsage);
    computeLongMax(tasks.stream().map(Task::getNetworkIoTime), workloadBuilder::maxNetworkUsage);

    final long networkUsageSize =
        positiveDataSize(tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed());
    final double meanNetworkUsage = computeMean(
        tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed(),
        networkUsageSize,
        workloadBuilder::meanNetworkUsage);

    computeStdAndCov(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0.0).map(Long::doubleValue),
        meanNetworkUsage,
        networkUsageSize,
        workloadBuilder::stdNetworkUsage,
        workloadBuilder::covNetworkUsage);

    computeMedianAndQuantile(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0).collect(Collectors.toList()),
        -1L,
        workloadBuilder::medianNetworkUsage,
        workloadBuilder::firstQuartileNetworkUsage,
        workloadBuilder::thirdQuartileNetworkUsage);

    computeDoubleMin(tasks.stream().map(Task::getDiskSpaceRequested), workloadBuilder::minDiskSpaceUsage);
    computeDoubleMax(tasks.stream().map(Task::getDiskSpaceRequested), workloadBuilder::maxDiskSpaceUsage);

    final long diskSpaceSize = positiveDataSize(tasks.stream().map(Task::getDiskSpaceRequested));
    final double meanDiskSpaceUsage =
        computeMean(tasks.stream().map(Task::getDiskSpaceRequested), diskSpaceSize, workloadBuilder::meanDiskSpaceUsage);

    computeStdAndCov(
        tasks.stream().map(Task::getDiskSpaceRequested).filter(x -> x >= 0.0),
        meanDiskSpaceUsage,
        diskSpaceSize,
        workloadBuilder::stdDiskSpaceUsage,
        workloadBuilder::covDiskSpaceUsage);

    computeMedianAndQuantile(
        tasks.stream()
            .map(Task::getDiskSpaceRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        workloadBuilder::medianDiskSpaceUsage,
        workloadBuilder::firstQuartileDiskSpaceUsage,
        workloadBuilder::thirdQuartileDiskSpaceUsage);

    computeDoubleMin(tasks.stream().map(Task::getEnergyConsumption), workloadBuilder::minEnergy);
    computeDoubleMax(tasks.stream().map(Task::getEnergyConsumption), workloadBuilder::maxEnergy);
    final long energySize = positiveDataSize(tasks.stream().map(Task::getEnergyConsumption));
    final double meanEnergy = computeMean(tasks.stream().map(Task::getEnergyConsumption), energySize, workloadBuilder::meanEnergy);

    computeStdAndCov(
        tasks.stream().map(Task::getEnergyConsumption).filter(x -> x >= 0.0),
        meanEnergy,
        energySize,
        workloadBuilder::stdEnergy,
        workloadBuilder::covEnergy);

    computeMedianAndQuantile(
        tasks.stream()
            .map(Task::getEnergyConsumption)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()),
        -1.0,
        workloadBuilder::medianEnergy,
        workloadBuilder::firstQuartileEnergy,
        workloadBuilder::thirdQuartileEnergy);

    this.getProcessedObjects().add(workloadBuilder.totalWorkflows(numWorkflows)
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
   * @param data              stream of data
   * @return                  size of positive elements
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private long positiveDataSize(Stream<Double> data) {
    return data.filter(x -> x >= 0.0).count();
  }

  /**
   * Find the maximum element inside the double valued stream and give it to the field of the workloadBuilder.
   *
   * @param data              stream of data
   * @param maxFunction       Function object that is applied to the given data
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void computeDoubleMax(Stream<Double> data, Function<Double, Workload.WorkloadBuilder> maxFunction) {
    maxFunction.apply(data.filter(x -> x >= 0.0).reduce(Double::max).orElse(-1.0));
  }

  /**
   * Find the minimum element of the double valued stream and set the corresponding field in the workloadBuilder as that value.
   *
   * @param data              stream of data
   * @param minFunction       Function object that is applied to the given data
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void computeDoubleMin(Stream<Double> data, Function<Double, Workload.WorkloadBuilder> minFunction) {
    minFunction.apply(data.filter(x -> x >= 0.0).reduce(Double::min).orElse(-1.0));
  }

  /**
   * Maximum function for long value.
   *
   * @param data              stream of data
   * @param maxFunction       Function object that is applied to the given data
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void computeLongMax(Stream<Long> data, Function<Long, Workload.WorkloadBuilder> maxFunction) {
    maxFunction.apply(data.filter(x -> x >= 0).reduce(Long::max).orElse(-1L));
  }

  /**
   * Minimum function for long value.
   *
   * @param data              stream of data
   * @param minFunction       Function object that is applied to the given data
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void computeLongMin(Stream<Long> data, Function<Long, Workload.WorkloadBuilder> minFunction) {
    minFunction.apply(data.filter(x -> x >= 0).reduce(Long::min).orElse(-1L));
  }

  /**
   * Mean value for the data stream with invalid stream handling.
   * Will set up the corresponding field with the builder parameter that is passed.
   *
   * @param data              stream of data
   * @param size              size of the stream
   * @param meanFunction      Function object that is applied to the given data
   * @return                  mean value
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private double computeMean(Stream<Double> data, long size, Function<Double, Workload.WorkloadBuilder> meanFunction) {
    double mean = -1.0;
    if (size != 0) {
      mean = data.filter(x -> x >= 0.0).reduce(Double::sum).orElse((double) -size) / size;
    }
    meanFunction.apply(mean);
    return mean;
  }

  /**
   * Standard deviation value and normalized standard deviation value for the data stream with invalid stream handling.
   *
   * @param data              stream of data
   * @param mean              mean value from {@link #computeMean(Stream, long, Function)}
   * @param size              size from {@link #positiveDataSize(Stream)}
   * @param stdFunction       standard deviation Function object that is applied to the given data
   * @param covFunction       covariance Function object that is applied to the given data
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void computeStdAndCov(
      Stream<Double> data,
      Double mean,
      long size,
      Function<Double, Workload.WorkloadBuilder> stdFunction,
      Function<Double, Workload.WorkloadBuilder> covFunction) {
    if (size == 0 || mean == -1.0) {
      stdFunction.apply(-1.0);
      covFunction.apply(-1.0);
    } else {
      double temp = data.map(x -> x * x).reduce(Double::sum).orElse(-1.0);
      temp = Math.pow(temp / size - mean * mean, 0.5);
      stdFunction.apply(temp);
      if (mean == 0.0) {
        covFunction.apply(-1.0);
      } else {
        covFunction.apply(temp / mean);
      }
    }
  }

  /**
   * This method will set the median, first quartile and third quartile elements for the given data list.
   *
   * @param data                    list of metrics
   * @param defaultValue            default median, first quartile, third quartile value given invalid list
   * @param medianFunction          median Function object that is applied to the given data
   * @param firstQuantileFunction   first quantile Function object that is applied to the given data
   * @param thirdQuantileFunction   third quantile Function object that is applied to the given data
   * @param <T>                     type of the list, needs to be comparable for sorting
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private <T extends Comparable<T>> void computeMedianAndQuantile(
      List<T> data,
      T defaultValue,
      Function<T, Workload.WorkloadBuilder> medianFunction,
      Function<T, Workload.WorkloadBuilder> firstQuantileFunction,
      Function<T, Workload.WorkloadBuilder> thirdQuantileFunction) {
    if (data.size() == 0) {
      medianFunction.apply(defaultValue);
      firstQuantileFunction.apply(defaultValue);
      thirdQuantileFunction.apply(defaultValue);
    } else {
      Collections.sort(data);
      medianFunction.apply(data.get(data.size() / 2));
      firstQuantileFunction.apply(data.get(data.size() / 4));
      thirdQuantileFunction.apply(data.get(data.size() * 3 / 4));
    }
  }
}
