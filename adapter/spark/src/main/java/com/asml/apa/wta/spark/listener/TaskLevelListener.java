package com.asml.apa.wta.spark.listener;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;

import java.util.LinkedList;
import java.util.List;

/**
 * This class is a task-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class TaskLevelListener extends SparkListener {

    public List<TaskMetrics> taskMetricsList = new LinkedList<>();

    /**
     * This method is called every time a task ends, where task-level metrics
     * are added to the list
     *
     * @param taskEnd   SparkListenerTaskEnd
     */
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        taskMetricsList.add(taskEnd.taskMetrics());
    }
}
