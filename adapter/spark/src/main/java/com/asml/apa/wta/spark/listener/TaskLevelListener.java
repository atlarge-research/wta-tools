package com.asml.apa.wta.spark.listener;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import com.asml.apa.wta.core.model.Task;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.resource.ResourceProfile;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @author Henry Page
 * @since 1.0.0
 */
@RequiredArgsConstructor
public class TaskLevelListener extends SparkListener {

  private final SparkContext sparkContext;

  private Map<Integer, Set<Integer>> jobIdstoTasks = new ConcurrentHashMap<>();

  private final List<Task> processedTasks = new LinkedList<>();

  /**
   * This method is called every time a task ends, where task-level metrics are added to the list.
   *
   * @param taskEnd   SparkListenerTaskEnd
   */
  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    TaskContext context = TaskContext.get();
    final var curTaskInfo = taskEnd.taskInfo();
    final var curTaskMetrics = taskEnd.taskMetrics();

    final var taskId = curTaskInfo.taskId();
    final var type = taskEnd.taskType();
    final var submitTime = curTaskInfo.launchTime();
    final Long runTime = curTaskMetrics.executorRunTime();
    final String resourceType = "CPU";
    final Double resourceAmountRequested = 1.0;
    final int userId = sparkContext.sparkUser().hashCode();

    TaskContext.get().


    var resources = context.resources();

    // TODO: CALL EXTERNAL DEPENDENCIES

  }

  /**
   * This method is called every time a job starts.
   * In the context of the WTA, this is a workflow.
   *
   * @param jobStart The object corresponding to information on job start.
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    if (!jobIdstoTasks.containsKey(jobStart.jobId())) {
      jobIdstoTasks.put(jobStart.jobId(), ConcurrentHashMap.newKeySet());
    }
  }

  /**
   * This method gets a list of task metrics.
   *
   * @return  List of task metrics
   */
  public List<Task> getProcessedTasks() {
    return processedTasks;
  }
}
