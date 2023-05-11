package com.asml.apa.wta.spark.listener;

import org.apache.spark.scheduler.*;

import java.util.LinkedList;
import java.util.List;

/**
 * This class is a stage-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class StageLevelListener extends SparkListener {

    public List<StageInfo> stageInfoList = new LinkedList<>();

    public void onStageCompleted(SparkListenerStageCompleted stageComplete) {
        stageInfoList.add(stageComplete.stageInfo());
    }
}
