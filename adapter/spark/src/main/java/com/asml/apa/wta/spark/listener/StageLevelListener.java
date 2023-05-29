package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import scala.collection.immutable.List;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class StageLevelListener extends AbstractListener<Task>{  //not sure about the type

    @Getter
    private final Map<Integer, Integer[]> stageToParents = new ConcurrentHashMap<>();

    public StageLevelListener(SparkContext sparkContext, RuntimeConfig config) {
        super(sparkContext, config);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        super.onStageCompleted(stageCompleted);
        final StageInfo stageInfo = stageCompleted.stageInfo();
        final int stageId = stageInfo.stageId();
        final List<Integer> scalaTemp = stageInfo.parentIds().toStream().map(x -> (Integer) x).toList();
        final Integer[] parentIds = new Integer[scalaTemp.length()];
        stageToParents.put(stageId, parentIds);
    }
}
