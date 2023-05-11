package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.spark.listener.ApplicationLevelListener;
import com.asml.apa.wta.spark.listener.StageLevelListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.status.api.v1.StageData;

import javax.ws.rs.core.Application;
import java.util.List;

public class SparkMetrics {

    private SparkSession sparkSession;
    private TaskLevelListener taskLevelListener;
    private StageLevelListener stageLevelListener;

    public SparkMetrics(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        taskLevelListener = new TaskLevelListener();
        stageLevelListener = new StageLevelListener();
    }

    public void registerStageListener() {
        sparkSession.sparkContext().addSparkListener(stageLevelListener);
    }

    public void removeStageListener() {
        sparkSession.sparkContext().removeSparkListener(stageLevelListener);
    }

    public void registerTaskListener() {
        sparkSession.sparkContext().addSparkListener(taskLevelListener);
    }

    public void removeTaskListener() {
        sparkSession.sparkContext().removeSparkListener(taskLevelListener);
    }

    public List<TaskMetrics> getTaskMetrics() {
        return taskLevelListener.taskMetricsList;
    }

    public List<StageInfo> getStageMetrics() {
        return stageLevelListener.stageInfoList;
    }
}
