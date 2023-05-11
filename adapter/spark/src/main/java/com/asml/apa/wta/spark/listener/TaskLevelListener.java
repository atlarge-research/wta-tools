package com.asml.apa.wta.spark.listener;

import java.util.LinkedList;
import java.util.List;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class TaskLevelListener extends SparkListener {

  private final List<TaskMetrics> taskMetricsList = new LinkedList<>();

  /**
   * This method is called every time a task ends, where task-level metrics are added to the list.
   *
   * @param taskEnd   SparkListenerTaskEnd
   */
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    taskMetricsList.add(taskEnd.taskMetrics());
  }

  /**
   * This method gets a list of task metrics.
   *
   * @return  List of task metrics
   */
  public List<TaskMetrics> getTaskMetricsList() {
    return taskMetricsList;
  }
}
