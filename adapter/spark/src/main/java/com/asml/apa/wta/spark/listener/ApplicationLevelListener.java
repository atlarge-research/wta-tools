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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

  private void setTasks(TaskStageBaseListener taskListener, StageLevelListener stageListener) {
    TaskLevelListener listener = (TaskLevelListener) taskListener;
    final List<Task> tasks = taskListener.getProcessedObjects();
    for (Task task : tasks) {
      // parent children fields
      final int stageId = listener.getTaskToStage().get(task.getId());
      final Integer[] parentStages =
          stageListener.getStageToParents().get().get(stageId);
      if (parentStages != null) {
        final Long[] parents = Arrays.stream(parentStages)
            .flatMap(x ->
                Arrays.stream(listener.getStageToTasks().getOrDefault(x, new ArrayList<>()).stream()
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
      }
    }
  }

  private void setStages(StageLevelListener stageListener) {
    final List<Task> stages = stageListener.getProcessedObjects();
    Map<Integer, List<Integer>> parentToChildren = stageListener.getParentToChildren();
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
          .orElseGet(() -> -1.0));
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
  @SuppressWarnings({"CyclomaticComplexity", "MethodLength"})
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
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

    final long resourceTaskSize = size(tasks.stream().map(Task::getResourceAmountRequested));

    final double meanResourceTask = mean(tasks.stream().map(Task::getResourceAmountRequested), resourceTaskSize);

    final double stdResourceTask = standardDeviation(
        tasks.stream().map(Task::getResourceAmountRequested).filter(x -> x >= 0.0),
        meanResourceTask,
        resourceTaskSize);

    final Object[] resourceStats = medianAndQuatiles(tasks.stream()
            .map(Task::getResourceAmountRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()))
        .orElseGet(() -> new Double[] {-1.0, -1.0, -1.0});

    final double medianResourceTask = (Double) resourceStats[0];

    final double firstQuartileResourceTask = (Double) resourceStats[1];

    final double thirdQuartileResourceTask = (Double) resourceStats[2];

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

    long memorySize = size(tasks.stream().map(Task::getMemoryRequested));

    final double meanMemory = mean(tasks.stream().map(Task::getMemoryRequested), memorySize);

    final double stdMemory = standardDeviation(
        tasks.stream().map(Task::getMemoryRequested).filter(x -> x >= 0.0), meanMemory, memorySize);

    final Object[] memoryStats = medianAndQuatiles(tasks.stream()
            .map(Task::getMemoryRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()))
        .orElseGet(() -> new Double[] {-1.0, -1.0, -1.0});

    final double medianMemory = (Double) memoryStats[0];

    final double firstQuartileMemory = (Double) memoryStats[1];

    final double thirdQuartileMemory = (Double) memoryStats[2];

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

    final long networkUsageSize =
        size(tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed());
    final double meanNetworkUsage =
        mean(tasks.stream().mapToDouble(Task::getNetworkIoTime).boxed(), networkUsageSize);

    final double stdNetworkUsage = standardDeviation(
        tasks.stream().map(Task::getNetworkIoTime).filter(x -> x >= 0.0).map(Long::doubleValue),
        meanNetworkUsage,
        networkUsageSize);

    final Object[] networkStats = medianAndQuatiles(tasks.stream()
            .map(Task::getNetworkIoTime)
            .filter(x -> x >= 0)
            .collect(Collectors.toList()))
        .orElseGet(() -> new Long[] {-1L, -1L, -1L});

    final long medianNetworkUsage = (Long) networkStats[0];

    final long firstQuartileNetworkUsage = (Long) networkStats[1];

    final long thirdQuartileNetworkUsage = (Long) networkStats[2];

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

    final long diskSpaceSize = size(tasks.stream().map(Task::getDiskSpaceRequested));
    final double meanDiskSpaceUsage = mean(tasks.stream().map(Task::getDiskSpaceRequested), diskSpaceSize);

    final double stdDiskSpaceUsage = standardDeviation(
        tasks.stream().map(Task::getDiskSpaceRequested).filter(x -> x >= 0.0),
        meanDiskSpaceUsage,
        diskSpaceSize);

    final Object[] diskSpaceStats = medianAndQuatiles(tasks.stream()
            .map(Task::getDiskSpaceRequested)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()))
        .orElseGet(() -> new Double[] {-1.0, -1.0, -1.0});

    final double medianDiskSpaceUsage = (Double) diskSpaceStats[0];

    final double firstQuartileDiskSpaceUsage = (Double) diskSpaceStats[1];

    final double thirdQuartileDiskSpaceUsage = (Double) diskSpaceStats[2];

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
    final long energySize = size(tasks.stream().map(Task::getEnergyConsumption));
    final double meanEnergy = mean(tasks.stream().map(Task::getEnergyConsumption), energySize);

    final double stdEnergy = standardDeviation(
        tasks.stream().map(Task::getEnergyConsumption).filter(x -> x >= 0.0), meanEnergy, energySize);

    final Object[] energyStats = medianAndQuatiles(tasks.stream()
            .map(Task::getEnergyConsumption)
            .filter(x -> x >= 0.0)
            .collect(Collectors.toList()))
        .orElseGet(() -> new Double[] {-1.0, -1.0, -1.0});

    final double medianEnergy = (Double) energyStats[0];

    final double firstQuartileEnergy = (Double) energyStats[1];

    final double thirdQuartileEnergy = (Double) energyStats[2];

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

  private long size(Stream<Double> data) {
    return data.filter(x -> x >= 0.0).count();
  }

  private double mean(Stream<Double> data, long size) {
    double mean = -1.0;
    if (size != 0) {
      mean = (double) data.filter(x -> x >= 0.0).reduce(Double::sum).get() / size;
    }
    return mean;
  }

  private double standardDeviation(Stream<Double> data, Double mean, long size) {
    if (size == 0) {
      return -1.0;
    } else {
      double temp = data.map(x -> x * x).reduce(Double::sum).get();
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
