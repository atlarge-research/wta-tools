package com.asml.apa.wta.spark.util;

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
 * This class takes in all Spark Stages within a Spark Job, generate the DAG and find the critical path.
 * It will add two additional nodes to the dependency graph: one as the source (node with id = 0) connecting
 * all stages that do not have parents, the other as the sink (node with id = -1) connecting all stages
 * without children. The critical path shall be the longest path from the source to the sink. The ids of
 * additional node(source and sink) are chosen such that there will be no collisions with the stage ids.
 *
 * @author Tianchen Qu
 * @since 1.0.0
 */
public class DagSolver {

  /**
   * This is a class representing node inside the DAG graph.
   */
  private static class Node {

    private final long id;

    private long distance;

    Node(Task stage) {
      id = stage.getId();
      distance = Integer.MIN_VALUE;
    }

    /**
     * This is used for instantiating node 0, -1 as the extra source/sink node.
     * The ids of the source and sink are chosen such that there will be no collisions with the stage ids.
     *
     * @param nodeId        id 0 or -1
     */
    Node(long nodeId) {
      id = nodeId;
      distance = Integer.MIN_VALUE;
    }
  }

  private static final long sourceId = 0;

  private static final long sinkId = -1;

  private final Map<Long, Node> nodes = new ConcurrentHashMap<>();

  private final Map<Long, Map<Long, Long>> adjacencyMap = new ConcurrentHashMap<>();

  private final Map<Long, Map<Long, Long>> adjacencyMapReversed = new ConcurrentHashMap<>();

  private final List<Task> stages;

  public DagSolver(List<Task> stages, TaskLevelListener listener) {
    this.stages = stages;

    addNode(sourceId);
    for (Task stage : stages) {
      addNode(stage, listener);
    }
    addNode(sinkId);
  }

  /**
   * This will add the node and the adjacent edges of the node. If there is no parents, it will be linked to the
   * source node.
   *
   * @param stage                 Spark Stage
   * @param taskLevelListener     TaskLevelListener
   */
  private void addNode(Task stage, TaskLevelListener taskLevelListener) {
    Node node = new Node(stage);
    nodes.put(stage.getId(), node);

    List<Task> tasks = taskLevelListener.getStageToTasks().getOrDefault(stage.getId(), new ArrayList<>());
    long runtime = tasks.stream().map(Task::getRuntime).reduce(Long::max).orElse(0L);

    if (stage.getParents().length > 0) {
      for (long id : stage.getParents()) {
        addEdge(id, stage.getId(), runtime);
      }
    } else {
      addEdge(sourceId, stage.getId(), runtime);
    }
  }

  /**
   * This method is used to create the additional source and sink node.
   * The ids of the source and sink are chosen such that there will be no collisions with the stage ids.
   *
   * @param id            id of the source (id = 0) and sink (id = -1) node
   */
  private void addNode(Long id) {
    Node node = new Node(id);
    nodes.put(id, node);
    // if the node is the ending node
    if (id == sinkId) {
      setFinalEdges();
      adjacencyMap.put(sinkId, new ConcurrentHashMap<>());
    } else if (id == sourceId) { // if the node is the starting node
      node.distance = 0;
      adjacencyMapReversed.put(sourceId, new ConcurrentHashMap<>());
    }
  }

  /**
   * This method add a directed edge from vertex1 to vertex2 with the specified weight.
   *
   * @param vertex1       vertex1
   * @param vertex2       vertex2
   * @param weight        weight of edge from vertex1 to vertex2
   */
  private void addEdge(long vertex1, long vertex2, long weight) {
    if (adjacencyMap.get(vertex1) == null) {
      adjacencyMap.put(vertex1, new ConcurrentHashMap<>());
    }
    if (adjacencyMapReversed.get(vertex2) == null) {
      adjacencyMapReversed.put(vertex2, new ConcurrentHashMap<>());
    }
    adjacencyMap.get(vertex1).put(vertex2, weight);
    adjacencyMapReversed.get(vertex2).put(vertex1, weight);
  }

  /**
   * This method links all nodes without a children to the sink node.
   */
  private void setFinalEdges() {
    for (Long node : nodes.keySet()) {
      if (adjacencyMap.get(node) == null && node != sinkId) {
        addEdge(node, sinkId, 0);
      }
    }
  }

  /**
   * This method does topological sorting on the DAG using a {@link Deque}.
   */
  private Deque<Long> topologicalSort() {
    Deque<Long> stack = new ConcurrentLinkedDeque<>();
    Map<Long, Boolean> visited = new ConcurrentHashMap<>();
    topoUtil(visited, sourceId, stack);
    return stack;
  }

  /**
   * This is the recursive utility method for topological sorting.
   *
   * @param visited       map of all visited nodes
   * @param node          current node
   * @param stack         stack used for topological sorting
   */
  private void topoUtil(Map<Long, Boolean> visited, Long node, Deque<Long> stack) {
    visited.put(node, true);
    for (Long children : adjacencyMap.get(node).keySet()) {
      if (visited.get(children) == null) {
        topoUtil(visited, children, stack);
      }
    }
    stack.push(node);
  }

  /**
   * Computes the longest path on the DAG.
   *
   * @return              longest path
   */
  public List<Task> longestPath() {
    Deque<Long> stack = topologicalSort();
    while (!stack.isEmpty()) {
      Node node = nodes.get(stack.pop());
      for (Long childId : adjacencyMap.get(node.id).keySet()) {
        Node child = nodes.get(childId);
        if (child.distance < node.distance + adjacencyMap.get(node.id).get(childId)) {
          child.distance = node.distance + adjacencyMap.get(node.id).get(childId);
        }
      }
    }
    return backTracing();
  }

  /**
   * Backtraces the longest path on the DAG based on each node's maximum value.
   *
   * @return              longest path
   */
  private List<Task> backTracing() {
    AtomicLong pointer = new AtomicLong(sinkId);
    List<Long> criticalPath = new ArrayList<>();
    while (pointer.get() != sourceId) {
      criticalPath.add(pointer.get());
      adjacencyMapReversed.get(pointer.get()).forEach((adjNode, weight) -> {
        if (weight + nodes.get(adjNode).distance == nodes.get(pointer.get()).distance) {
          pointer.set(adjNode);
        }
      });
    }
    List<Long> finalCriticalPath =
        criticalPath.stream().filter(nodeId -> nodeId >= 1).collect(Collectors.toList());
    return stages.stream()
        .filter(stage -> finalCriticalPath.contains(stage.getId()))
        .collect(Collectors.toList());
  }
}
