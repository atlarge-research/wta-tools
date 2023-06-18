package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.enums.Domain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import scala.collection.JavaConverters;

/**
 * This class is a job-level listener for the Spark data source.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
public class JobLevelListener extends AbstractListener<Workflow> {

  private final TaskStageBaseListener taskListener;

  private final StageLevelListener stageLevelListener;

  private final Map<Integer, Long> jobSubmitTimes = new ConcurrentHashMap<>();

  private final Map<Long, Map<Long, List<Long>>> jobToStages = new ConcurrentHashMap<>();

  /**
   * Constructor for the job-level listener.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @param taskListener       The task-level listener to be used by this listener
   * @param stageLevelListener The stage-level listener
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public JobLevelListener(
      SparkContext sparkContext,
      RuntimeConfig config,
      TaskStageBaseListener taskListener,
      StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.taskListener = taskListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Constructor for the job-level listener.
   * This constructor is for stage-level plugin.
   *
   * @param sparkContext       The current spark context
   * @param config             Additional config specified by the user for the plugin
   * @param stageLevelListener The stage-level listener
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public JobLevelListener(SparkContext sparkContext, RuntimeConfig config, StageLevelListener stageLevelListener) {
    super(sparkContext, config);
    this.taskListener = stageLevelListener;
    this.stageLevelListener = stageLevelListener;
  }

  /**
   * Callback for job start event, tracks the submit time of the job.
   *
   * @param jobStart The jobstart event object containing information upon job start.
   * @author Henry Page
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    jobSubmitTimes.put(jobStart.jobId() + 1, jobStart.time());
    Map<Long, List<Long>> parents = new ConcurrentHashMap<>();
    JavaConverters.seqAsJavaList(jobStart.stageInfos()).stream().forEach(stageInfo -> {
      parents.put(
          (long) stageInfo.stageId() + 1,
          JavaConverters.seqAsJavaList(stageInfo.parentIds()).stream()
              .mapToInt(x -> (int) x + 1)
              .mapToLong(x -> (long) x)
              .boxed()
              .collect(Collectors.toList()));
    });
    jobToStages.put((long) jobStart.jobId() + 1, parents);
  }

  /**
   * Processes the workflow and puts it into an object.
   *
   * @param jobEnd The job end event object containing information upon job end
   * @author Henry Page
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final int jobId = jobEnd.jobId() + 1;
    final long submitTime = jobSubmitTimes.get(jobId);
    final Task[] tasks = taskListener
        .getWithCondition(task -> task.getWorkflowId() == jobId)
        .toArray(Task[]::new);
    final int numTasks = tasks.length;
    // we can also get the mode from the config, if that's what the user wants?
    final String scheduler = sparkContext.getConf().get("spark.scheduler.mode", "FIFO");
    final Domain domain = config.getDomain();
    final String appName = sparkContext.appName();

    final double totalResources = -1.0;
    final double totalMemoryUsage =
        calculatePositiveDoubleSum(Arrays.stream(tasks).map(Task::getMemoryRequested));
    final long totalNetworkUsage =
        calculatePositiveLongSum(Arrays.stream(tasks).map(Task::getNetworkIoTime));
    final double totalDiskSpaceUsage =
        calculatePositiveDoubleSum(Arrays.stream(tasks).map(Task::getDiskSpaceRequested));
    final double totalEnergyConsumption =
        calculatePositiveDoubleSum(Arrays.stream(tasks).map(Task::getEnergyConsumption));
    // Critical Path
    final int criticalPathTaskCount = -1;
    final long criticalPathLength = jobEnd.time() - jobSubmitTimes.get(jobId);

    // unknown

    final int maxNumberOfConcurrentTasks = -1;
    final String nfrs = "";
    final String applicationField = "ETL";

    this.getProcessedObjects()
        .add(Workflow.builder()
            .id(jobId)
            .tsSubmit(submitTime)
            .tasks(tasks)
            .taskCount(numTasks)
            .criticalPathLength(criticalPathLength)
            .criticalPathTaskCount(criticalPathTaskCount)
            .maxConcurrentTasks(maxNumberOfConcurrentTasks)
            .nfrs(nfrs)
            .scheduler(scheduler)
            .domain(domain)
            .applicationName(appName)
            .applicationField(applicationField)
            .totalResources(totalResources)
            .totalMemoryUsage(totalMemoryUsage)
            .totalNetworkUsage(totalNetworkUsage)
            .totalDiskSpaceUsage(totalDiskSpaceUsage)
            .totalEnergyConsumption(totalEnergyConsumption)
            .build());

    jobSubmitTimes.remove(jobId);
  }

  /**
   * Summation for Double stream for positive terms.
   *
   * @param data data stream
   * @return summation
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private double calculatePositiveDoubleSum(Stream<Double> data) {
    return data.filter(task -> task >= 0.0).reduce(Double::sum).orElse(-1.0);
  }

  /**
   * Summation for Long stream for positive terms.
   *
   * @param data data stream
   * @return summation
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private long calculatePositiveLongSum(Stream<Long> data) {
    return data.filter(task -> task >= 0).reduce(Long::sum).orElse(-1L);
  }

  /**
   * This is a method called on application end. it sets up the resources used in the spark workflow.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  public void setWorkflows() {
    final List<Workflow> workflows = getProcessedObjects();
    for (Workflow workflow : workflows) {
      workflow.setTotalResources(Arrays.stream(workflow.getTasks())
          .map(Task::getResourceAmountRequested)
          .filter(resourceAmount -> resourceAmount >= 0.0)
          .reduce(Double::sum)
          .orElse(-1.0));

      if (!config.isStageLevel()) {
        Set<Long> stageIds = jobToStages.get(workflow.getId()).keySet();
        List<Task> jobStages = stageLevelListener.getProcessedObjects().stream()
            .filter(stage -> stageIds.contains(stage.getId()))
            .collect(Collectors.toList());
        jobToStages.get(workflow.getId()).forEach((id, parents) -> {
          if (!jobStages.stream()
              .map(stage -> stage.getId())
              .collect(Collectors.toList())
              .contains(id)) {
            jobStages.add(Task.builder()
                .id(id)
                .type("cached stage")
                .parents(parents.stream()
                    .mapToLong(parentId -> parentId)
                    .toArray())
                .build());
          }
        });

        final List<Task> criticalPath = solveCriticalPath(jobStages);
        workflow.setCriticalPathTaskCount(criticalPath.size());
        final long driverTime = workflow.getCriticalPathLength()
            - criticalPath.stream()
                .map(Task::getRuntime)
                .reduce(Long::sum)
                .orElse(0L);
        TaskLevelListener listener = (TaskLevelListener) taskListener;
        final Map<Integer, List<Task>> stageToTasks = listener.getStageToTasks();
        workflow.setCriticalPathLength(driverTime
            + criticalPath.stream()
                .map(stage -> stageToTasks.getOrDefault((int) stage.getId(), new ArrayList<>()).stream()
                    .map(Task::getRuntime)
                    .reduce(Long::max)
                    .orElse(0L))
                .reduce(Long::sum)
                .orElse(-1L));
      } else {
        workflow.setCriticalPathLength(-1L);
      }
    }
  }

  public List<Task> solveCriticalPath(List<Task> stages) {
    DAG dag = new DAG(stages);
    return dag.longestPath().stream()
        .filter(stage -> !stage.getType().equals("cached stage"))
        .collect(Collectors.toList());
  }

  class Node {

    long id;

    long dist;

    public Node(Task stage) {
      id = stage.getId() + 1;
      dist = Integer.MIN_VALUE;
    }

    public Node(long id) {
      this.id = id;
      dist = Integer.MIN_VALUE;
    }
  }

  class DAG {

    Map<Long, Node> nodes;

    Map<Long, Map<Long, Long>> adjMap;

    Map<Long, Map<Long, Long>> adjMapReversed;

    Deque<Long> stack;

    List<Task> stages;

    public DAG(List<Task> stages) {
      nodes = new ConcurrentHashMap<>();
      adjMap = new ConcurrentHashMap<>();
      stack = new ConcurrentLinkedDeque<>();
      adjMapReversed = new ConcurrentHashMap<>();
      this.stages = stages;

      addNode(0L);
      for (Task stage : stages) {
        addNode(stage);
      }
      addNode(1L);
    }

    public void addNode(Task stage) {
      Node node = new Node(stage);
      nodes.put(stage.getId() + 1, node);

      TaskLevelListener listener = (TaskLevelListener) taskListener;
      long runtime = 0L;
      List<Task> tasks = listener.getStageToTasks().get((int) stage.getId() - 1);
      if (tasks != null) {
        runtime = tasks.stream().map(Task::getRuntime).reduce(Long::max).orElse(0L);
      }

      if (stage.getParents().length > 0) {
        for (long id : stage.getParents()) {
          addEdge(id + 1, stage.getId() + 1, runtime);
        }
      } else {
        addEdge(0, stage.getId() + 1, runtime);
      }
    }

    public void addNode(Long id) {
      Node node = new Node(id);
      nodes.put(id, node);
      // if the node is the ending node
      if (id == 1) {
        setFinalEdges();
        adjMap.put(1L, new ConcurrentHashMap<>());
      } else if (id == 0) { // if the node is the starting node
        node.dist = 0;
        adjMapReversed.put(0L, new ConcurrentHashMap<>());
      }
    }

    private void addEdge(long v, long u, long weight) {
      if (adjMap.get(v) == null) {
        adjMap.put(v, new ConcurrentHashMap<>());
      }
      if (adjMapReversed.get(u) == null) {
        adjMapReversed.put(u, new ConcurrentHashMap<>());
      }
      adjMap.get(v).put(u, weight);
      adjMapReversed.get(u).put(v, weight);
    }

    private void setFinalEdges() {
      for (Long node : nodes.keySet()) {
        if (adjMap.get(node) == null && node != 1) {
          addEdge(node, 1, 0);
        }
      }
    }

    private void topologicalSort() {
      Map<Long, Boolean> visited = new ConcurrentHashMap<>();
      topoUtil(visited, 0L);
    }

    private void topoUtil(Map<Long, Boolean> visited, Long node) {
      visited.put(node, true);
      for (Long children : adjMap.get(node).keySet()) {
        if (visited.get(children) == null) {
          topoUtil(visited, children);
        }
      }
      stack.push(node);
    }

    public List<Task> longestPath() {
      topologicalSort();
      while (!stack.isEmpty()) {
        Node node = nodes.get(stack.pop());
        for (Long childId : adjMap.get(node.id).keySet()) {
          Node child = nodes.get(childId);
          if (child.dist < node.dist + adjMap.get(node.id).get(childId)) {
            child.dist = node.dist + adjMap.get(node.id).get(childId);
          }
        }
      }
      return backTracing();
    }

    private List<Task> backTracing() {
      long pointer = 1L;
      List<Long> criticalPath = new ArrayList<>();
      while (pointer != 0) {
        criticalPath.add(pointer);
        AtomicLong nextPointer = new AtomicLong();
        long edgeWeight = -1;
        adjMapReversed.get(pointer).forEach((adjNode, weight) -> {
          if (weight > edgeWeight) {
            nextPointer.set(adjNode);
          }
        });
        pointer = nextPointer.get();
      }
      List<Long> finalCriticalPath =
          criticalPath.stream().map(x -> x - 1).filter(x -> x >= 0).collect(Collectors.toList());
      ;
      return stages.stream()
          .filter(x -> finalCriticalPath.contains(x.getId()))
          .collect(Collectors.toList());
    }
  }
}
