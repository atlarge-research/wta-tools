package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

class StageLevelListenerTest extends BaseLevelListenerTest {

  SparkListenerStageCompleted stageCompleted;

  StageInfo testStageInfo;

  @BeforeEach
  void setup() {
    ListBuffer<Integer> parents = new ListBuffer<>();
    parents.addOne(1);
    parents.addOne(2);

    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);

    testStageInfo = new StageInfo(
        3, 0, "test", 50, null, parents.toList().map(x -> x), "None", mockedMetrics, null, null, 100);

    stageCompleted = new SparkListenerStageCompleted(testStageInfo);
  }

  @Test
  void onStageCompletedTest() {
    fakeStageListener.onStageCompleted(stageCompleted);
    assertThat(fakeStageListener.getStageToParents()).containsEntry(3, new Integer[] {1, 2});
    assertThat(fakeStageListener.getParentToChildren()).containsEntry(1, new ListBuffer<Integer>().addOne(3));
    assertThat(fakeStageListener.getParentToChildren()).containsEntry(2, new ListBuffer<Integer>().addOne(3));
  }

  public boolean equalsArray(Map<Integer, Integer[]> first, Map<Integer, Integer[]> second) {
    if (first.size() != second.size()) {
      return false;
    }

    return first.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), second.get(e.getKey())));
  }
}
