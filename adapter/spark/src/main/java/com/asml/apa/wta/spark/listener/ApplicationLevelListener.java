package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    final List<Task> stages = stageListener.getProcessedObjects();
    stages.forEach(stage -> stage.setChildren(
            stageListener.getParentStageToChildrenStages().getOrDefault(stage.getId(), new ArrayList<>())
                    .stream()
                    .mapToLong(Long::longValue)
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

    final Workflow[] workflows = jobLevelListener.getProcessedObjects().toArray(new Workflow[0]);
    final int numWorkflows = workflows.length;
    final int totalTasks = Arrays.stream(workflows).mapToInt(Workflow::getTaskCount).sum();
    final Domain domain = config.getDomain();
    final long startDate = sparkContext.startTime();
    final long endDate = applicationEnd.time();
    final String[] authors = config.getAuthors();
    final String workloadDescription = config.getDescription();
    final List<Task> tasks = wtaTaskListener.getProcessedObjects();
    final long numSites = tasks.stream().filter(x -> x.getSubmissionSite() != -1).count();
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

    // resource
    List<Double> positiveResourceList = tasks.stream().map(Task::getResourceAmountRequested).filter(x -> x >= 0.0).collect(Collectors.toList());
    final double meanResourceTask = computeMean(tasks.stream().map(Task::getResourceAmountRequested), positiveResourceList.size());
    final double stdResourceTask = computeStd(tasks.stream().map(Task::getResourceAmountRequested).filter(x -> x >= 0.0), meanResourceTask, positiveResourceList.size());

    // memory
    List<Double> positiveMemoryList = tasks.stream().map(Task::getMemoryRequested).filter(x -> x >= 0.0).collect(Collectors.toList());
    final double meanMemory = computeMean(tasks.stream().map(Task::getMemoryRequested), positiveMemoryList.size());
    final double stdMemory = computeStd(tasks.stream().map(Task::getMemoryRequested).filter(x -> x >= 0.0), meanMemory, positiveMemoryList.size());

    // network io
    List<Double> positiveNetworkIoList = tasks.stream().map(task -> (double) task.getNetworkIoTime()).filter(x -> x >= 0.0).collect(Collectors.toList());
    final double meanNetworkIo = computeMean(tasks.stream().map(task -> (double) task.getNetworkIoTime()), positiveNetworkIoList.size());
    final double stdNetworkIo = computeStd(tasks.stream().map(task -> (double) task.getNetworkIoTime()).filter(x -> x >= 0.0), meanNetworkIo, positiveNetworkIoList.size());

    // disk space
    List<Double> positiveDiskSpaceList = tasks.stream().map(Task::getDiskSpaceRequested).filter(x -> x >= 0.0).collect(Collectors.toList());
    final double meanDiskSpace = computeMean(tasks.stream().map(Task::getDiskSpaceRequested), positiveDiskSpaceList.size());
    final double stdDiskSpace = computeStd(tasks.stream().map(Task::getDiskSpaceRequested).filter(x -> x >= 0.0), meanDiskSpace, positiveDiskSpaceList.size());

    // energy
    List<Double> positiveEnergyList = tasks.stream().map(Task::getEnergyConsumption).filter(x -> x >= 0.0).collect(Collectors.toList());
    final double meanEnergy = computeMean(tasks.stream().map(Task::getEnergyConsumption), positiveEnergyList.size());
    final double stdEnergy = computeStd(tasks.stream().map(Task::getEnergyConsumption).filter(x -> x >= 0.0), meanEnergy, positiveEnergyList.size());

    this.getProcessedObjects().add(Workload.builder()
                    .totalWorkflows(numWorkflows)
                    .totalTasks(totalTasks)
                    .domain(domain)
                    .dateStart(startDate)
                    .dateEnd(endDate)
                    .numSites(numSites)
                    .numResources(numResources)
                    .numUsers(numUsers)
                    .numGroups(numGroups)
                    .totalResourceSeconds(totalResourceSeconds)
                    .authors(authors)
                    .workloadDescription(workloadDescription)

                    // resources
                    .minResourceTask(computeMin(tasks.stream().map(Task::getResourceAmountRequested)))
                    .maxResourceTask(computeMax(tasks.stream().map(Task::getResourceAmountRequested)))
                    .meanResourceTask(meanResourceTask)
                    .stdResourceTask(stdResourceTask)
                    .covResourceTask(computeCov(meanResourceTask, stdResourceTask))
                    .medianResourceTask(positiveResourceList.isEmpty() ? -1.0 : computeMedian(positiveResourceList))
                    .firstQuartileResourceTask(positiveResourceList.isEmpty() ? -1.0 : computeFirstQuantile(positiveResourceList))
                    .thirdQuartileResourceTask(positiveResourceList.isEmpty() ? -1.0 : computeThirdQuantile(positiveResourceList))

                    // memory
                    .minMemory(computeMin(tasks.stream().map(Task::getMemoryRequested)))
                    .maxMemory(computeMax(tasks.stream().map(Task::getMemoryRequested)))
                    .meanMemory(meanMemory)
                    .stdMemory(stdMemory)
                    .covMemory(computeCov(meanMemory, stdMemory))
                    .medianMemory(positiveMemoryList.isEmpty() ? -1.0 : computeMedian(positiveMemoryList))
                    .firstQuartileMemory(positiveMemoryList.isEmpty() ? -1.0 : computeFirstQuantile(positiveMemoryList))
                    .thirdQuartileMemory(positiveMemoryList.isEmpty() ? -1.0 : computeThirdQuantile(positiveMemoryList))

                    // network io
                    .minNetworkUsage((long) computeMin(tasks.stream().map(task -> (double) task.getNetworkIoTime())))
                    .maxNetworkUsage((long) computeMax(tasks.stream().map(task -> (double) task.getNetworkIoTime())))
                    .meanNetworkUsage(meanNetworkIo)
                    .stdNetworkUsage(stdNetworkIo)
                    .covNetworkUsage(computeCov(meanMemory, stdMemory))
                    .medianNetworkUsage(positiveNetworkIoList.isEmpty() ? -1L : (long) computeMedian(positiveNetworkIoList))
                    .firstQuartileNetworkUsage(positiveNetworkIoList.isEmpty() ? -1L : (long) computeFirstQuantile(positiveNetworkIoList))
                    .thirdQuartileNetworkUsage(positiveNetworkIoList.isEmpty() ? -1L : (long) computeThirdQuantile(positiveNetworkIoList))

                    // disk space
                    .minDiskSpaceUsage(computeMin(tasks.stream().map(Task::getDiskSpaceRequested)))
                    .maxDiskSpaceUsage(computeMax(tasks.stream().map(Task::getDiskSpaceRequested)))
                    .meanDiskSpaceUsage(meanDiskSpace)
                    .stdDiskSpaceUsage(stdDiskSpace)
                    .covDiskSpaceUsage(computeCov(meanDiskSpace, stdDiskSpace))
                    .medianDiskSpaceUsage(positiveDiskSpaceList.isEmpty() ? -1.0 : computeMedian(positiveDiskSpaceList))
                    .firstQuartileDiskSpaceUsage(positiveDiskSpaceList.isEmpty() ? -1.0 : computeFirstQuantile(positiveDiskSpaceList))
                    .thirdQuartileDiskSpaceUsage(positiveDiskSpaceList.isEmpty() ? -1.0 : computeThirdQuantile(positiveDiskSpaceList))

                    // energy
                    .minEnergy(computeMin(tasks.stream().map(Task::getEnergyConsumption)))
                    .maxEnergy(computeMax(tasks.stream().map(Task::getEnergyConsumption)))
                    .meanEnergy(meanEnergy)
                    .stdEnergy(stdEnergy)
                    .covEnergy(computeCov(meanEnergy, stdEnergy))
                    .medianEnergy(positiveEnergyList.isEmpty() ? -1.0 : computeMedian(positiveEnergyList))
                    .firstQuartileEnergy(positiveEnergyList.isEmpty() ? -1.0 : computeFirstQuantile(positiveEnergyList))
                    .thirdQuartileEnergy(positiveEnergyList.isEmpty() ? -1.0 : computeThirdQuantile(positiveEnergyList))
                    .build());
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
  private double computeStd(
      Stream<Double> data,
      double mean,
      long size) {
    if (size == 0 || mean == -1.0) {
      return -1.0;
    }
    double numerator = data.map(x -> Math.pow(x - mean, 2)).reduce(Double::sum).orElse(-1.0);
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
  private double computeCov(
          double mean,
          double std) {
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