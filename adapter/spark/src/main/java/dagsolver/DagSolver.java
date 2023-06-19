package dagsolver;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This class will take in all stages within the job, generate the dependency DAG
 * and find the longest path(critical path). It will add two additional node to the dependency graph
 * one as the origin connecting all stages that don't have parents, the other as the sink connecting all stages
 * without a children. The critical path shall be the longest path from the origin to the sink.
 *
 * @author Tianchen Qu
 * @since 1.0.0
 */
public class DagSolver {

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

  private Map<Long, Node> nodes;

  private Map<Long, Map<Long, Long>> adjMap;

  private Map<Long, Map<Long, Long>> adjMapReversed;

  private Deque<Long> stack;

  private List<Task> stages;

  public DagSolver(List<Task> stages, TaskLevelListener listener) {
    nodes = new ConcurrentHashMap<>();
    adjMap = new ConcurrentHashMap<>();
    stack = new ConcurrentLinkedDeque<>();
    adjMapReversed = new ConcurrentHashMap<>();
    this.stages = stages;

    addNode(0L);
    for (Task stage : stages) {
      addNode(stage, listener);
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
  private void addNode(Task stage, TaskLevelListener listener) {
    Node node = new Node(stage);
    nodes.put(stage.getId() + 1, node);

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
  private void addNode(Long id) {
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
   * This is the recursive utility method for topological sorting.
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
   * Computes the longest path on the DAG.
   *
   * @return longest path
   * @author Tianchen Qu
   * @since 1.0.0
   */
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
