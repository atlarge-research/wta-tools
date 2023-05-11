package com.asml.apa.wta.spark.listener;

import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;

import java.util.LinkedList;
import java.util.List;


/**
 * This class is a placeholder for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class TaskLevelListener extends SparkListener {

    public List<TaskMetrics> taskMetricsList = new LinkedList<>();

    public void onTaskStart(SparkListenerTaskStart taskStart) {
        TaskInfo taskInfo = taskStart.taskInfo();
        taskInfo.taskId();
    }

    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        TaskMetrics taskMetrics = taskEnd.taskMetrics();
        taskMetricsList.add(taskMetrics);
    }
}
