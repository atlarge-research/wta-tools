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

/**
 * This class is a stage-level listener for the Spark data source.
 *
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Getter
public class StageLevelListener extends AbstractListener<Task> { // not sure about the type

  private final Map<Integer, Integer[]> stageToParents = new ConcurrentHashMap<>();

  private final Map<Integer, ListBuffer<Integer>> parentToChildren = new ConcurrentHashMap<>();

  public StageLevelListener(SparkContext sparkContext, RuntimeConfig config) {
    super(sparkContext, config);
  }

  /**
   * This method will store the stage hierarchy information from the callback.
   *
   * @param stageCompleted callback from stage completion
   *
   * @author Tianchen Qu
   * @since 1.0.0
   */
  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    super.onStageCompleted(stageCompleted);
    final StageInfo stageInfo = stageCompleted.stageInfo();
    final int stageId = stageInfo.stageId();
    final List<Integer> scalaTemp =
        stageInfo.parentIds().toStream().map(x -> (Integer) x).toList();
    final Integer[] parentIds = new Integer[scalaTemp.length()];
    scalaTemp.copyToArray(parentIds);
    stageToParents.put(stageId, parentIds);

    for (Integer id : parentIds) {
      ListBuffer<Integer> children = parentToChildren.get(id);
      if (children == null) {
        children = new ListBuffer<>();
        children.addOne(stageId);
        parentToChildren.put(id, children);
      } else {
        children.addOne(stageId);
      }
    }
  }
}
