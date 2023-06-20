package com.asml.apa.wta.spark.dagsolver;

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
 * This class will take in all stages within the job, generate the DAG
 * and find the critical path. It will add two additional nodes to the dependency graph
 * one as the source connecting all stages that do not have parents, the other as the sink connecting all stages
 * without children. The critical path shall be the longest path from the source to the sink.
 *
 * @author Tianchen Qu
 * @since 1.0.0
 */
public class DagSolver {

  /**
   * This is a class representing node inside the DAG graph.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  class Node {

    private long id;

    private long distance;

    Node(Task stage) {
      id = stage.getId();
      distance = Integer.MIN_VALUE;
    }

    /**
     * This is used for instantiating node 0,-1 as the extra source/sink node.
     *
     * @param id id(0/-1)
     * @author Tianchen Qu
     * @since 1.0.0
     */
    Node(long id) {
      this.id = id;
      distance = Integer.MIN_VALUE;
    }
  }

  private Map<Long, Node> nodes = new ConcurrentHashMap<>();

  private Map<Long, Map<Long, Long>> adjMap = new ConcurrentHashMap<>();

  private Map<Long, Map<Long, Long>> adjMapReversed = new ConcurrentHashMap<>();

  private List<Task> stages;

  public DagSolver(List<Task> stages, TaskLevelListener listener) {
    this.stages = stages;

    addNode(0L);
    for (Task stage : stages) {
      addNode(stage, listener);
    }
    addNode(-1L);
  }

  /**
   * This will add the node and the adjacent edges of the node.
   * If there is no parents, it will be linked to the source node.
   *
   * @param stage stage
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void addNode(Task stage, TaskLevelListener listener) {
    Node node = new Node(stage);
    nodes.put(stage.getId(), node);

    long runtime = 0L;
    List<Task> tasks = listener.getStageToTasks().get(stage.getId());
    if (tasks != null) {
      runtime = tasks.stream().map(Task::getRuntime).reduce(Long::max).orElse(0L);
    }

    if (stage.getParents().length > 0) {
      for (long id : stage.getParents()) {
        addEdge(id, stage.getId(), runtime);
      }
    } else {
      addEdge(0, stage.getId(), runtime);
    }
  }

  /**
   * This method is used to create the source and sink node.
   *
   * @param id id of the source(id = 0) and sink(id = -1) node
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void addNode(Long id) {
    Node node = new Node(id);
    nodes.put(id, node);
    // if the node is the ending node
    if (id == -1L) {
      setFinalEdges();
      adjMap.put(-1L, new ConcurrentHashMap<>());
    } else if (id == 0) { // if the node is the starting node
      node.distance = 0;
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
      if (adjMap.get(node) == null && node != -1) {
        addEdge(node, -1L, 0);
      }
    }
  }

  /**
   * This method does topological sorting on the dag using a deque.
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private Deque<Long> topologicalSort() {
    Deque<Long> stack = new ConcurrentLinkedDeque<>();
    Map<Long, Boolean> visited = new ConcurrentHashMap<>();
    topoUtil(visited, 0L, stack);
    return stack;
  }

  /**
   * This is the recursive utility method for topological sorting.
   *
   * @param visited a map of all visited nodes
   * @param node the current node
   * @param stack stack used for topological sorting
   * @author Tianchen Qu
   * @since 1.0.0
   */
  private void topoUtil(Map<Long, Boolean> visited, Long node, Deque<Long> stack) {
    visited.put(node, true);
    for (Long children : adjMap.get(node).keySet()) {
      if (visited.get(children) == null) {
        topoUtil(visited, children, stack);
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
    Deque<Long> stack = topologicalSort();
    while (!stack.isEmpty()) {
      Node node = nodes.get(stack.pop());
      for (Long childId : adjMap.get(node.id).keySet()) {
        Node child = nodes.get(childId);
        if (child.distance < node.distance + adjMap.get(node.id).get(childId)) {
          child.distance = node.distance + adjMap.get(node.id).get(childId);
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
    AtomicLong pointer = new AtomicLong(-1L);
    List<Long> criticalPath = new ArrayList<>();
    while (pointer.get() != 0) {
      criticalPath.add(pointer.get());
      adjMapReversed.get(pointer.get()).forEach((adjNode, weight) -> {
        if (weight + nodes.get(adjNode).distance == nodes.get(pointer.get()).distance) {
          pointer.set(adjNode);
        }
      });
    }
    List<Long> finalCriticalPath = criticalPath.stream().filter(x -> x >= 1).collect(Collectors.toList());
    return stages.stream()
        .filter(x -> finalCriticalPath.contains(x.getId()))
        .collect(Collectors.toList());
  }
}
