package com.asml.apa.wta.spark.listener;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;

import java.util.LinkedList;
import java.util.List;

public class StageLevelListener extends SparkListener {

    public List<StageInfo> stageInfoList = new LinkedList<>();

    public void onStageCompleted(SparkListenerStageCompleted stageComplete) {

    }

    public void onStageSubmitted(SparkListenerStageCompleted stageSubmit) {
        StageInfo stageInfo = stageSubmit.stageInfo();
        stageInfoList.add(stageInfo);
    }
}
