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
   * Also tracks the stages and their parents in the job.
   *
   * @param jobStart The jobstart event object containing information upon job start.
   * @author Henry Page
   * @author Tianchen Qu
   * @since 1.0.0
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
   * @author Tianchen Qu
   * @since 1.0.0
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
   * It also calculated the critical path for the job.
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

  /**
   * This method takes the stages inside this job and return the critical path.
   * it will filter out all dummy caches nodes.
   *
   * @param stages all stages in the job(including the cached ones)
   * @return critical path
   * @author Tianchen Qu
   * @since 1.0.0
   */
  List<Task> solveCriticalPath(List<Task> stages) {
    DagSolver dag = new DagSolver(stages);
    return dag.longestPath().stream()
        .filter(stage -> !stage.getType().equals("cached stage"))
        .collect(Collectors.toList());
  }

  /**
   * This is the utility class for all the nodes inside the DAG graph for stages.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  class Node {

    private long id;

    private long dist;

    /**
     * The id get added one as we need to leave node 0,1 as the extra origin/ending node.
     *
     * @param stage stage
     * @author Tianchen Qu
     * @since 1.0.0
     */
    Node(Task stage) {
      id = stage.getId() + 1;
      dist = Integer.MIN_VALUE;
    }

    Node(long id) {
      this.id = id;
      dist = Integer.MIN_VALUE;
    }
  }

  /**
   * This class will take in all stages within the job, generate the dependency DAG
   * and find the longest path(critical path). It will add two additional node to the dependency graph
   * one as the origin connecting all stages that don't have parents, the other as the sink connecting all stages
   * without a children. The critical path shall be the longest path from the origin to the sink.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  class DagSolver {

    private Map<Long, Node> nodes;

    private Map<Long, Map<Long, Long>> adjMap;

    private Map<Long, Map<Long, Long>> adjMapReversed;

    private Deque<Long> stack;

    private List<Task> stages;

    DagSolver(List<Task> stages) {
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

    /**
     * This will add the node and the adjacent edges of the node.
     * If there is no parents, it will be linked to the origin node.
     *
     * @param stage stage
     * @author Tianchen Qu
     * @since 1.0.0
     */
    void addNode(Task stage) {
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

    /**
     * This method is used to create the origin and sink node.
     *
     * @param id id of the origin(id = 0) and sink(id = 1) node
     * @author Tianchen Qu
     * @since 1.0.0
     */
    void addNode(Long id) {
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

    /**
     * This method add a directed edge from vertex1 to vertex2 with the specified weight.
     *
     * @param vertex1 vertex1
     * @param vertex2 vertex2
     * @param weight weight
     * @author Tianchen Qu
     * @since 1.0.0
     */
    private void addEdge(long vertex1, long vertex2, long weight) {
      if (adjMap.get(vertex1) == null) {
        adjMap.put(vertex1, new ConcurrentHashMap<>());
      }
      if (adjMapReversed.get(vertex2) == null) {
        adjMapReversed.put(vertex2, new ConcurrentHashMap<>());
      }
      adjMap.get(vertex1).put(vertex2, weight);
      adjMapReversed.get(vertex2).put(vertex1, weight);
    }

    /**
     * This method links all nodes without a children to the sink node.
     * @author Tianchen Qu
     * @since 1.0.0
     */
    private void setFinalEdges() {
      for (Long node : nodes.keySet()) {
        if (adjMap.get(node) == null && node != 1) {
          addEdge(node, 1, 0);
        }
      }
    }

    /**
     * This method does topological sorting on the dag using a deque.
     *
     * @author Tianchen Qu
     * @since 1.0.0
     */
    private void topologicalSort() {
      Map<Long, Boolean> visited = new ConcurrentHashMap<>();
      topoUtil(visited, 0L);
    }

    /**
     * This is the recursive utility method for topological sorting
     *
     * @param visited a map of all visited nodes
     * @param node the current node
     * @author Tianchen Qu
     * @since 1.0.0
     */
    private void topoUtil(Map<Long, Boolean> visited, Long node) {
      visited.put(node, true);
      for (Long children : adjMap.get(node).keySet()) {
        if (visited.get(children) == null) {
          topoUtil(visited, children);
        }
      }
      stack.push(node);
    }

    /**
     * This method returns the longest path on the DAG.
     *
     * @return longest path
     * @author Tianchen Qu
     * @since 1.0.0
     */
    List<Task> longestPath() {
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

    /**
     * This method backtraces the longest path on the DAG based on each node's maximum value.
     *
     * @return the longest path
     * @author Tianchen Qu
     * @since 1.0.0
     */
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
      return stages.stream()
          .filter(x -> finalCriticalPath.contains(x.getId()))
          .collect(Collectors.toList());
    }
  }
}
