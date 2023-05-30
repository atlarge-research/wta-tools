package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Task;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;


public class StageLevelListener extends AbstractListener<Task>{  //not sure about the type

    @Getter
    private final Map<Integer, Integer[]> stageToParents = new ConcurrentHashMap<>();

    @Getter
    private final Map<Integer, ListBuffer<Integer>> parentToChildren = new ConcurrentHashMap<>();

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
        scalaTemp.copyToArray(parentIds);
        stageToParents.put(stageId, parentIds);

        for(Integer id : parentIds){
            ListBuffer<Integer> children = parentToChildren.get(id);
            if(children == null){
                children = new ListBuffer<>();
                children.addOne(stageId);
            }else {
                children.addOne(stageId);
            }
            parentToChildren.put(id, children);
        }
    }
}
