package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.model.enums.Domain;

import java.util.*;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
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

  private void setTasks(TaskStageBaseListener taskLevelListener, StageLevelListener stageLevelListener){
    TaskLevelListener listener = (TaskLevelListener) taskLevelListener;
    final List<Task> tasks = taskLevelListener.getProcessedObjects();
    for (Task task : tasks) {
      // parent children fields
      final int stageId = listener.getTaskToStage().get(task.getId());
      final Integer[] parentStages =
              stageLevelListener.getStageToParents().get(stageId);
      if (parentStages != null) {
        final Long[] parents = Arrays.stream(parentStages)
                .flatMap(x -> Arrays.stream(listener.getStageToTasks().get(x).stream()
                        .map(Task::getId)
                        .toArray(size -> new Long[size])))
                .toArray(size -> new Long[size]);
        task.setParents(ArrayUtils.toPrimitive(parents));
      }

      List<Integer> childrenStages =
              stageLevelListener.getParentToChildren().get(stageId);
      if (childrenStages != null) {
        List<Task> children = new ArrayList<>();
        childrenStages.forEach(
                x -> children.addAll(listener.getStageToTasks().get(x)));
        Long[] temp = children.stream().map(Task::getId).toArray(size -> new Long[size]);
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
        task.setResourceUsed(resourceProfileId);
      }
    }
  }

  private void setStages(StageLevelListener stageLevelListener){
    final List<Task> stages = stageLevelListener.getProcessedObjects();
    Map<Integer, List<Integer>> parentToChildren = stageLevelListener.getParentToChildren();
    for (Task stage : stages){
      stage.setChildren(parentToChildren.getOrDefault(Math.toIntExact(stage.getId()),new ArrayList<>()).stream().mapToLong(x -> x).toArray());
    }
  }

  private void setWorkflows(JobLevelListener jobLevelListener){
    final List<Workflow> workflows = jobLevelListener.getProcessedObjects();
    for (Workflow workflow : workflows) {
      workflow.setTotalResources(Arrays.stream(workflow.getTasks())
              .map(Task::getResourceAmountRequested)
              .filter(x -> x >= 0.0)
              .reduce(Double::sum)
              .orElseGet(() -> -1.0));
    }
  }

  /**
   * Callback function that is called right at the end of the application. Further experimentation
   * is needed to determine if applicationEnd is called first or shutdown.
   *
   * @param applicationEnd The event corresponding to the end of the application
   */
  @SuppressWarnings({"CyclomaticComplexity", "MethodLength"})
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {

    // we should enver enter this branch, this is a guard since an application
    // only terminates once.
    if (config.isStageLevel()){
      setStages(stageLevelListener);
    }else {
      setTasks(taskLevelListener,stageLevelListener);
    }
    setWorkflows(jobLevelListener);
    final List<Task> tasks = taskLevelListener.getProcessedObjects();
    List<Workload> processedObjects = this.getProcessedObjects();
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

    final double minResourceTask = tasks.stream()
        .map(Task::getResourceAmountRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::min)
        .orElse(-1.0);

    final double maxResourceTask = tasks.stream()
        .map(Task::getResourceAmountRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::max)
        .orElse(-1.0);

    double meanResourceTask = -1.0;
    long resourceTaskSize = (int) tasks.stream()
        .filter(x -> x.getResourceAmountRequested() >= 0.0)
        .count();
    if (resourceTaskSize != 0) {
      meanResourceTask = tasks.stream()
              .map(Task::getResourceAmountRequested)
              .filter(x -> x >= 0.0)
              .reduce(Double::sum)
              .get()
          / resourceTaskSize;
    }

    final double stdResourceTask = standardDeviation(
        tasks.stream().map(Task::getResourceAmountRequested).filter(x -> x >= 0.0),
        meanResourceTask,
        resourceTaskSize);

    final Optional<Object[]> resourceStats = medianAndQuatiles(tasks.stream()
        .map(Task::getResourceAmountRequested)
        .filter(x -> x >= 0.0)
        .collect(Collectors.toList()));

    final double medianResourceTask = (Double) resourceStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[0];

    final double firstQuartileResourceTask =
        (Double) resourceStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[1];

    final double thirdQuartileResourceTask =
        (Double) resourceStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[2];

    double covResourceTask = -1.0;
    if (meanResourceTask != 0 && medianResourceTask != -1.0) {
      covResourceTask = stdResourceTask / meanResourceTask;
    }

    final double minMemory = tasks.stream()
        .map(Task::getMemoryRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::min)
        .orElse(-1.0);

    final double maxMemory = tasks.stream()
        .map(Task::getMemoryRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::max)
        .orElse(-1.0);

    double meanMemory = -1.0;
    long memorySize = tasks.stream()
        .map(Task::getMemoryRequested)
        .filter(x -> x >= 0.0)
        .count();
    if (memorySize != 0) {
      meanMemory = tasks.stream()
              .map(Task::getMemoryRequested)
              .filter(x -> x >= 0.0)
              .reduce(Double::sum)
              .get()
          / memorySize;
    }

    final double stdMemory = standardDeviation(
        tasks.stream().map(Task::getMemoryRequested).filter(x -> x >= 0.0), meanMemory, memorySize);

    final Optional<Object[]> memoryStats = medianAndQuatiles(tasks.stream()
        .map(Task::getMemoryRequested)
        .filter(x -> x >= 0.0)
        .collect(Collectors.toList()));

    final double medianMemory = (Double) memoryStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[0];

    final double firstQuartileMemory = (Double) memoryStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[1];

    final double thirdQuartileMemory = (Double) memoryStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[2];

    double covMemory = -1.0;
    if (meanMemory != 0 && meanMemory != -1.0) {
      covMemory = stdMemory / meanMemory;
    }

    final long minNetworkUsage = tasks.stream()
        .map(Task::getNetworkIoTime)
        .filter(x -> x >= 0)
        .reduce(Long::min)
        .orElseGet(() -> -1L);

    final long maxNetworkUsage = tasks.stream()
        .map(Task::getNetworkIoTime)
        .filter(x -> x >= 0)
        .reduce(Long::max)
        .orElse(-1L);

    double meanNetworkUsage = -1.0;
    long networkUsageSize =
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0).count();
    if (networkUsageSize != 0) {
      meanNetworkUsage = (double) tasks.stream()
              .map(Task::getNetworkIoTime)
              .filter(x -> x >= 0)
              .reduce(Long::sum)
              .get()
          / networkUsageSize;
    }

    final double stdNetworkUsage = standardDeviation(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0.0).map(Long::doubleValue),
        meanNetworkUsage,
        networkUsageSize);

    final Optional<Object[]> networkStats = medianAndQuatiles(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0).collect(Collectors.toList()));

    final long medianNetworkUsage = (Long) networkStats.orElseGet(() -> new Long[] {-1L, -1L, -1L})[0];

    final long firstQuartileNetworkUsage = (Long) networkStats.orElseGet(() -> new Long[] {-1L, -1L, -1L})[1];

    final long thirdQuartileNetworkUsage = (Long) networkStats.orElseGet(() -> new Long[] {-1L, -1L, -1L})[2];

    double covNetworkUsage = -1.0;
    if (meanNetworkUsage != 0 && meanNetworkUsage != -1.0) {
      covNetworkUsage = stdNetworkUsage / meanNetworkUsage;
    }

    final double minDiskSpaceUsage = tasks.stream()
        .map(Task::getDiskSpaceRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::min)
        .orElse(-1.0);

    final double maxDiskSpaceUsage = tasks.stream()
        .map(Task::getDiskSpaceRequested)
        .filter(x -> x >= 0.0)
        .reduce(Double::max)
        .orElse(-1.0);

    double meanDiskSpaceUsage = -1.0;
    long diskSpaceSize = tasks.stream()
        .map(Task::getDiskSpaceRequested)
        .filter(x -> x >= 0.0)
        .count();
    if (diskSpaceSize != 0) {
      meanDiskSpaceUsage = tasks.stream()
              .map(Task::getDiskSpaceRequested)
              .filter(x -> x >= 0.0)
              .reduce(Double::sum)
              .get()
          / diskSpaceSize;
    }

    final double stdDiskSpaceUsage = standardDeviation(
        tasks.stream().map(Task::getDiskSpaceRequested).filter(x -> x >= 0.0),
        meanDiskSpaceUsage,
        diskSpaceSize);

    final Optional<Object[]> diskSpaceStats = medianAndQuatiles(tasks.stream()
        .map(Task::getDiskSpaceRequested)
        .filter(x -> x >= 0.0)
        .collect(Collectors.toList()));

    final double medianDiskSpaceUsage = (Double) diskSpaceStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[0];

    final double firstQuartileDiskSpaceUsage =
        (Double) diskSpaceStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[1];

    final double thirdQuartileDiskSpaceUsage =
        (Double) diskSpaceStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[2];

    double covDiskSpaceUsage = -1.0;
    if (meanDiskSpaceUsage != 0 && meanNetworkUsage != -1.0) {
      covDiskSpaceUsage = stdDiskSpaceUsage / meanDiskSpaceUsage;
    }

    final double minEnergy = tasks.stream()
        .map(Task::getEnergyConsumption)
        .filter(x -> x >= 0.0)
        .reduce(Double::min)
        .orElse(-1.0);

    final double maxEnergy = tasks.stream()
        .map(Task::getEnergyConsumption)
        .filter(x -> x >= 0.0)
        .reduce(Double::max)
        .orElse(-1.0);

    double meanEnergy = -1.0;
    long energySize = tasks.stream()
        .map(Task::getEnergyConsumption)
        .filter(x -> x >= 0.0)
        .count();
    if (energySize != 0) {
      meanEnergy = tasks.stream()
              .map(Task::getEnergyConsumption)
              .filter(x -> x >= 0.0)
              .reduce(Double::sum)
              .get()
          / energySize;
    }

    final double stdEnergy = standardDeviation(
        tasks.stream().map(Task::getEnergyConsumption).filter(x -> x >= 0.0), meanEnergy, energySize);

    final Optional<Object[]> energyStats = medianAndQuatiles(tasks.stream()
        .map(Task::getEnergyConsumption)
        .filter(x -> x >= 0.0)
        .collect(Collectors.toList()));

    final double medianEnergy = (Double) energyStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[0];

    final double firstQuartileEnergy = (Double) energyStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[1];

    final double thirdQuartileEnergy = (Double) energyStats.orElseGet(() -> new Double[] {-1.0, -1.0, -1.0})[2];

    double covEnergy = -1.0;
    if (meanEnergy != 0 && meanEnergy != -1.0) {
      covEnergy = stdEnergy / meanEnergy;
    }

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
        .meanNetworkUsage(meanNetworkUsage)
        .covNetworkUsage(covNetworkUsage)
        .firstQuartileNetworkUsage(firstQuartileNetworkUsage)
        .maxNetworkUsage(maxNetworkUsage)
        .medianNetworkUsage(medianNetworkUsage)
        .minNetworkUsage(minNetworkUsage)
        .stdNetworkUsage(stdNetworkUsage)
        .thirdQuartileNetworkUsage(thirdQuartileNetworkUsage)
        .maxEnergy(maxEnergy)
        .covEnergy(covEnergy)
        .firstQuartileEnergy(firstQuartileEnergy)
        .meanEnergy(meanEnergy)
        .medianEnergy(medianEnergy)
        .minEnergy(minEnergy)
        .stdEnergy(stdEnergy)
        .thirdQuartileEnergy(thirdQuartileEnergy)
        .covResourceTask(covResourceTask)
        .meanResourceTask(meanResourceTask)
        .stdResourceTask(stdResourceTask)
        .maxResourceTask(maxResourceTask)
        .minResourceTask(minResourceTask)
        .medianResourceTask(medianResourceTask)
        .firstQuartileResourceTask(firstQuartileResourceTask)
        .thirdQuartileResourceTask(thirdQuartileResourceTask)
        .covMemory(covMemory)
        .meanMemory(meanMemory)
        .stdMemory(stdMemory)
        .maxMemory(maxMemory)
        .minMemory(minMemory)
        .medianMemory(medianMemory)
        .firstQuartileMemory(firstQuartileMemory)
        .thirdQuartileMemory(thirdQuartileMemory)
        .covDiskSpaceUsage(covDiskSpaceUsage)
        .stdDiskSpaceUsage(stdDiskSpaceUsage)
        .meanDiskSpaceUsage(meanDiskSpaceUsage)
        .firstQuartileDiskSpaceUsage(firstQuartileDiskSpaceUsage)
        .thirdQuartileDiskSpaceUsage(thirdQuartileDiskSpaceUsage)
        .medianDiskSpaceUsage(medianDiskSpaceUsage)
        .maxDiskSpaceUsage(maxDiskSpaceUsage)
        .minDiskSpaceUsage(minDiskSpaceUsage)
        .build());
  }

  private double standardDeviation(java.util.stream.Stream<Double> data, Double mean, long size) {
    double temp = data.map(x -> x * x).reduce(Double::sum).orElseGet(() -> -1.0);
    if (temp == -1.0) {
      return -1.0;
    } else {
      return Math.pow(temp / size - mean * mean, 0.5);
    }
  }

  private <T extends Comparable> Optional<Object[]> medianAndQuatiles(List<T> data) {
    if (data == null || data.size() == 0) {
      return Optional.empty();
    }
    Collections.sort(data);
    Object[] triple = new Object[3];
    triple[0] = data.get(data.size() / 2);
    triple[1] = data.get(data.size() / 4);
    triple[2] = data.get(data.size() * 3 / 4);
    return Optional.of(triple);
  }
}
