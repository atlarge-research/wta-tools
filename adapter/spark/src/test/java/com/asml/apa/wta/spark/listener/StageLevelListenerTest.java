package com.asml.apa.wta.spark.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ListBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.asml.apa.wta.core.model.Task;
import java.util.Properties;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskLocation;
import org.apache.spark.storage.RDDInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;

class StageLevelListenerTest extends BaseLevelListenerTest {

  SparkListenerStageCompleted stageCompleted;

  StageInfo testStageInfo;

  @BeforeEach
  void setup() {
    TaskMetrics mockedMetrics = mock(TaskMetrics.class);
    when(mockedMetrics.executorRunTime()).thenReturn(100L);

    ListBuffer<Integer> parents = new ListBuffer<>();
    parents.addOne(1);
    parents.addOne(2);


    testStageInfo = new StageInfo(
        3, 0, "test", 50, null, parents.toList().map(x -> x), "None", mockedMetrics, null, null, 100);

    spyStageInfo = spy(testStageInfo);
    Option<Object> submissionTimeOption = Option.apply(10L);
    when(spyStageInfo.submissionTime()).thenReturn(submissionTimeOption);
    stageEndEvent = new SparkListenerStageCompleted(spyStageInfo);
  }
  @Test
  void testStageEndMetricExtraction() {
    ListBuffer<StageInfo> stageBuffer = new ListBuffer<>();
    stageBuffer.$plus$eq(spyStageInfo);

    fakeStageListener.onJobStart(new SparkListenerJobStart(1, 2L, stageBuffer.toList(), new Properties()));
    fakeStageListener.onStageCompleted(stageEndEvent);
    assertEquals(1, fakeStageListener.getProcessedObjects().size());
    Task curStage = fakeStageListener.getProcessedObjects().get(0);
    assertEquals(1, curStage.getId());
    assertEquals("", curStage.getType());
    assertEquals(10L, curStage.getSubmitTime());
    assertEquals(100L, curStage.getRuntime());
    assertEquals(2L, curStage.getWorkflowId());
    assertEquals("testUser".hashCode(), curStage.getUserId());
    assertEquals(-1, curStage.getSubmissionSite());
    assertEquals("N/A", curStage.getResourceType());
    assertEquals(-1.0, curStage.getResourceAmountRequested());
    assertEquals(-1.0, curStage.getMemoryRequested());
    assertEquals(-1.0, curStage.getDiskSpaceRequested());
    assertEquals(-1L, curStage.getEnergyConsumption());
    assertEquals(-1L, curStage.getNetworkIoTime());
    assertEquals(-1L, curStage.getDiskIoTime());
    assertEquals(-1, curStage.getGroupId());
    assertEquals("", curStage.getNfrs());
    assertEquals("", curStage.getParams());
    assertEquals(0, curStage.getParents().length);
    assertEquals(0, curStage.getChildren().length);
  }

  @Test
  void onStageCompletedTest() {
    fakeStageListener.onStageCompleted(stageCompleted);
    assertThat(fakeStageListener.getStageToParents()).containsEntry(3, new Integer[] {1, 2});
    assertThat(fakeStageListener.getStageToParents().size()).isEqualTo(1);
    assertThat(fakeStageListener.getParentToChildren()).containsEntry(1, new ListBuffer<Integer>().addOne(3));
    assertThat(fakeStageListener.getParentToChildren()).containsEntry(2, new ListBuffer<Integer>().addOne(3));
    assertThat(fakeStageListener.getParentToChildren().size()).isEqualTo(2);
  }
}