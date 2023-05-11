package com.asml.apa.wta.spark.listener;

import java.util.LinkedList;
import java.util.List;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;

/**
 * This class is a stage-level listener for the Spark data source.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class StageLevelListener extends SparkListener {

  private final List<StageInfo> stageInfoList = new LinkedList<>();

  /**
   * This method is called every time on stage completion, where stage-level information is added to the list.
   *
   * @param stageComplete  instance of completed stage with the stage-level info
   */
  public void onStageCompleted(SparkListenerStageCompleted stageComplete) {
    stageInfoList.add(stageComplete.stageInfo());
  }

  /**
   * This method gets a list of stage information.
   *
   * @return  List of stage information
   */
  public List<StageInfo> getStageInfoList() {
    return stageInfoList;
  }
}
